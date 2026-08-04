package wecom

// replier.go — the WeCom OutboundReplier. Handles the engine's needs_binding
// / agent_offline / agent_archived / issue_created outcomes by sending a
// text message back over the same aibot WebSocket the inbound loop owns
// (aibot has no REST outbound; every write is on the socket). It goes
// through sendersRegistry.send, the one outbound entry point, so a notice
// raised mid-reconnect waits for the socket instead of vanishing.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
)

// bindingMinter is the binding-token surface the replier needs.
// *BindingTokenService satisfies it.
type bindingMinter interface {
	Mint(ctx context.Context, workspaceID, installationID pgtype.UUID, wecomUserID string) (BindingToken, error)
}

// OutboundReplier implements engine.OutboundReplier for WeCom.
type OutboundReplier struct {
	binding     bindingMinter
	senders     *sendersRegistry
	languages   languageLookup
	appURL      string
	bindingPath string
	logger      *slog.Logger
}

// OutboundReplierConfig configures the replier. Binding + AppURL are
// required for the needs_binding prompt to work; without them the prompt
// is skipped (the offline/archived/issue notices still fire).
type OutboundReplierConfig struct {
	Binding bindingMinter

	// Senders is the same sendersRegistry the wecom ChannelDeps was built
	// with. The replier looks up the live wsSender by installation id.
	Senders *sendersRegistry

	// Languages resolves the sender to their Multica profile language, which
	// is what the notices are written in (language.go). Nil — and every
	// unbound sender, which notably includes everyone the binding prompt is
	// FOR — gets DefaultLocale.
	Languages languageLookup

	// AppURL is the Multica web app host the user clicks into to redeem
	// the binding token (e.g. https://multica.example). It comes from
	// MULTICA_APP_URL (falling back to FRONTEND_ORIGIN) and is
	// intentionally separate from MULTICA_PUBLIC_URL, which is the
	// backend/API URL — the bind page (/wecom/bind) is served by the web
	// app, so the link must point at the app host.
	AppURL      string
	BindingPath string // default "/wecom/bind"
	Logger      *slog.Logger
}

var _ engine.OutboundReplier = (*OutboundReplier)(nil)

// NewOutboundReplier builds the replier.
func NewOutboundReplier(cfg OutboundReplierConfig) *OutboundReplier {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	bindingPath := cfg.BindingPath
	if bindingPath == "" {
		bindingPath = "/wecom/bind"
	}
	if !strings.HasPrefix(bindingPath, "/") {
		bindingPath = "/" + bindingPath
	}
	return &OutboundReplier{
		binding:     cfg.Binding,
		senders:     cfg.Senders,
		languages:   cfg.Languages,
		appURL:      strings.TrimRight(cfg.AppURL, "/"),
		bindingPath: bindingPath,
		logger:      logger,
	}
}

// Reply routes each outcome to its user-visible message. Errors are
// logged, not propagated: the replier runs detached from the inbound ACK
// path (the engine.Router owns that goroutine).
func (r *OutboundReplier) Reply(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, res engine.Result) {
	c := copyFor(localeForSender(ctx, r.languages, inst.ID, msg.Source.SenderID))
	switch res.Outcome {
	case engine.OutcomeNeedsBinding:
		if err := r.sendBindingPrompt(ctx, inst, msg, res, c); err != nil {
			r.logger.WarnContext(ctx, "wecom replier: binding prompt failed",
				"installation_id", util.UUIDToString(inst.ID), "error", err)
		}
	case engine.OutcomeAgentOffline:
		if err := r.post(ctx, inst, msg, c.AgentOffline); err != nil {
			r.logger.WarnContext(ctx, "wecom replier: offline notice failed",
				"installation_id", util.UUIDToString(inst.ID), "error", err)
		}
	case engine.OutcomeAgentArchived:
		if err := r.post(ctx, inst, msg, c.AgentArchived); err != nil {
			r.logger.WarnContext(ctx, "wecom replier: archived notice failed",
				"installation_id", util.UUIDToString(inst.ID), "error", err)
		}
	case engine.OutcomeIngested:
		// Only a /issue-created message warrants a confirmation; a plain
		// chat message stays silent (the agent's own reply lands via
		// EventChatDone / Channel.Send).
		if res.IssueID.Valid {
			if err := r.post(ctx, inst, msg, issueCreatedText(res, c)); err != nil {
				r.logger.WarnContext(ctx, "wecom replier: issue-created confirmation failed",
					"installation_id", util.UUIDToString(inst.ID), "error", err)
			}
		}
	}
}

func (r *OutboundReplier) sendBindingPrompt(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, res engine.Result, c copyPack) error {
	sender := res.Sender
	if sender == "" {
		sender = msg.Source.SenderID
	}
	if sender == "" {
		return errors.New("wecom: missing sender id")
	}
	if r.binding == nil {
		return errors.New("wecom: binding service not configured")
	}
	if r.appURL == "" {
		return errors.New("wecom: app url not configured")
	}
	token, err := r.binding.Mint(ctx, inst.WorkspaceID, inst.ID, sender)
	if err != nil {
		return fmt.Errorf("wecom: mint binding token: %w", err)
	}
	// The throttle suppressed the mint: a live link is already in this chat.
	// Only its hash was ever stored, so there is no URL to rebuild — point
	// the user at the message they already have.
	if token.Reused {
		return r.post(ctx, inst, msg, c.BindingPending)
	}
	bindURL := r.appURL + r.bindingPath + "?token=" + url.QueryEscape(token.Raw)
	return r.post(ctx, inst, msg, c.BindingPromptPrefix+bindURL+c.BindingPromptSuffix)
}

// post hands the text to the registry's outbound queue, which writes it to
// the live connection when there is one and holds it for the next one when
// there is not (see outbound_queue.go). These notices matter most exactly
// when the socket is flapping: a binding prompt dropped mid-reconnect
// leaves the user unable to bind at all, and the offline / archived /
// issue-created notices answer a message the user already sent.
//
// An error here means the wire will never accept this body — a missing
// chat id, a bad chat type — not that delivery is late.
func (r *OutboundReplier) post(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, text string) error {
	_ = ctx
	if r.senders == nil {
		return errors.New("wecom: sender registry not configured")
	}
	if !inst.ID.Valid {
		return errors.New("wecom: installation id is zero")
	}
	return r.senders.send(inst.ID, pendingSend{
		ChatID:   msg.Source.ChatID,
		ChatType: aibotChatTypeFromChannel(msg.Source.ChatType),
		Content:  text,
	})
}

func issueCreatedText(res engine.Result, c copyPack) string {
	id := res.IssueIdentifier
	if id == "" {
		id = fmt.Sprintf("#%d", res.IssueNumber)
	}
	return c.issueCreated(id, res.IssueTitle)
}
