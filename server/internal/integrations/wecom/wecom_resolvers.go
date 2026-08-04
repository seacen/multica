package wecom

// wecom_resolvers.go — the ResolverSet the engine.Router routes through when
// the inbound channel_type is "wecom". Each interface method translates
// between the engine's normalized channel.InboundMessage and the wecom store
// / services. Platform-specific fields the normalized envelope does not carry
// (BotID, sender userid) come out of the wecom InboundMessage stashed in
// channel.InboundMessage.Raw.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// originWecomChat is the issue.origin_type label written for issues created
// via the wecom /issue command. Matches the "lark_chat" pattern (platform +
// "_chat") so analytics keeps the origin family consistent.
const originWecomChat = "wecom_chat"

// wecomMsgFromRaw decodes the wecom-side InboundMessage from
// channel.InboundMessage.Raw. Every resolver ends up doing this at least
// once; we centralize the JSON tag so a Raw shape change is a single-file
// edit.
func wecomMsgFromRaw(msg channel.InboundMessage) (InboundMessage, error) {
	if len(msg.Raw) == 0 {
		return InboundMessage{}, errors.New("wecom: inbound message Raw is empty")
	}
	var wm InboundMessage
	if err := json.Unmarshal(msg.Raw, &wm); err != nil {
		return InboundMessage{}, fmt.Errorf("wecom: decode inbound raw: %w", err)
	}
	return wm, nil
}

// The four query surfaces below are the slices of *Store each resolver
// actually uses. They are interfaces, not the concrete *Store, for one
// reason: *Store embeds *db.Queries, so anything holding it can only be
// driven by a live database. Narrowing to the handful of methods each
// resolver calls lets the routing, membership and dedup decisions be
// exercised with fakes — the same seam slack uses for installQueries.
// *Store satisfies all four.
type (
	installationLookup interface {
		GetInstallationByBotID(ctx context.Context, botID string) (Installation, error)
	}

	identityLookup interface {
		GetChannelUserBindingByUserID(ctx context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error)
		IsWorkspaceMember(ctx context.Context, workspaceID, userID pgtype.UUID) (bool, error)
	}

	dedupQueries interface {
		ClaimChannelInboundDedup(ctx context.Context, arg db.ClaimChannelInboundDedupParams) (db.ChannelInboundMessageDedup, error)
		MarkChannelInboundDedupProcessed(ctx context.Context, arg db.MarkChannelInboundDedupProcessedParams) (int64, error)
		ReleaseChannelInboundDedup(ctx context.Context, arg db.ReleaseChannelInboundDedupParams) (int64, error)
	}

	auditQueries interface {
		RecordChannelInboundDrop(ctx context.Context, arg db.RecordChannelInboundDropParams) error
	}
)

// NewResolverSet assembles the wecom ResolverSet from the store, the shared
// chat-session service, an outbound replier, the typing indicator and the
// media resolver.
//
// The last three are optional: pass nil to disable outbound binding prompts,
// the streaming bubble, or inbound media. Replier and typing are taken as
// concrete types rather than interfaces so a nil argument leaves the field nil
// instead of a typed-nil interface the Router would happily call; media is
// already an interface value the caller either built or did not.
func NewResolverSet(
	store *Store,
	session engineSessionBinder,
	replier engine.OutboundReplier,
	typing *TypingIndicatorManager,
	media engine.MediaResolver,
) engine.ResolverSet {
	set := engine.ResolverSet{
		Installation: &installationResolver{store: store},
		Identity:     &identityResolver{store: store},
		Dedup:        &deduper{q: store},
		Session:      &sessionBinder{session: session},
		Audit:        &auditor{q: store},
		OriginType:   originWecomChat,
	}
	if replier != nil {
		set.Replier = replier
	}
	if typing != nil {
		set.Typing = typing
	}
	if media != nil {
		set.Media = media
	}
	return set
}

// engineSessionBinder is the slice of engine.ChatSession the wecom binder
// drives. Declared as an interface so the platform-specific param mapping
// can be exercised with a fake in unit tests; *engine.ChatSession is the
// production value.
type engineSessionBinder interface {
	EnsureSession(ctx context.Context, in engine.EnsureSessionInput) (pgtype.UUID, error)
	AppendUserMessage(ctx context.Context, in engine.AppendInput) (engine.AppendResult, error)
	BindMediaRefs(ctx context.Context, in engine.BindMediaInput) error
}

// ---- installation routing ----

type installationResolver struct{ store installationLookup }

// ResolveInstallation looks up the wecom installation by the BotID carried
// on the inbound event. Every aibot_msg_callback frame identifies the bot
// via the WebSocket connection it arrived on (one bot per connection); the
// connector stamps BotID into InboundMessage.Raw so this resolver stays a
// pure DB lookup rather than needing socket-side plumbing.
func (r *installationResolver) ResolveInstallation(ctx context.Context, msg channel.InboundMessage) (engine.ResolvedInstallation, error) {
	wm, err := wecomMsgFromRaw(msg)
	if err != nil {
		return engine.ResolvedInstallation{}, err
	}
	if wm.BotID == "" {
		return engine.ResolvedInstallation{}, engine.ErrInstallationNotFound
	}
	inst, err := r.store.GetInstallationByBotID(ctx, wm.BotID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ResolvedInstallation{}, engine.ErrInstallationNotFound
		}
		return engine.ResolvedInstallation{}, err
	}
	return engine.ResolvedInstallation{
		ID:              inst.ID,
		WorkspaceID:     inst.WorkspaceID,
		AgentID:         inst.AgentID,
		InstallerUserID: inst.InstallerUserID,
		Active:          inst.Status == InstallationActive,
		Platform:        inst,
	}, nil
}

// ---- identity ----

type identityResolver struct{ store identityLookup }

// ResolveSender maps the WeCom smart-bot userid (the anonymized "T"-prefixed
// id the aibot API assigns per bot, from Source.SenderID) to a Multica user
// via the channel_user_binding table. First-time senders have no row and
// return engine.ErrSenderUnbound, which the Router pairs with the outbound
// binding prompt (see OutboundReplier.sendBindingPrompt).
//
// Why explicit binding rather than an implicit heuristic: aibot's userids
// have no relationship to real enterprise userids or emails — they are
// per-(bot, user) anonymized stable ids — so email-prefix matching (the
// pattern the internal customer-service adapter used) cannot work. See
// binding.go for the rationale.
//
// Membership re-check: the binding row's existence does NOT prove current
// workspace membership — a removed member's binding survives until an admin
// vacuums it. ErrSenderNotMember lets the Router drop silently rather than
// re-prompt (the correct product outcome, avoids leaking that a user was
// once a member).
func (r *identityResolver) ResolveSender(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage) (engine.ResolvedIdentity, error) {
	senderID := strings.TrimSpace(msg.Source.SenderID)
	if senderID == "" {
		return engine.ResolvedIdentity{}, engine.ErrSenderUnbound
	}
	binding, err := r.store.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: inst.ID,
		ChannelUserID:  senderID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.ResolvedIdentity{}, engine.ErrSenderUnbound
		}
		return engine.ResolvedIdentity{}, err
	}
	isMember, err := r.store.IsWorkspaceMember(ctx, inst.WorkspaceID, binding.MulticaUserID)
	if err != nil {
		return engine.ResolvedIdentity{}, err
	}
	if !isMember {
		return engine.ResolvedIdentity{}, engine.ErrSenderNotMember
	}
	return engine.ResolvedIdentity{UserID: binding.MulticaUserID}, nil
}

// ---- dedup ----

// deduper is the wecom Deduper. It uses the shared channel_inbound_message_dedup
// sqlc queries — the same table Feishu / Slack use — so the two-phase
// idempotency invariant is enforced uniformly across channels.
type deduper struct{ q dedupQueries }

// NewInboundDeduper hands boot the same two-phase deduper the ResolverSet
// runs on. The Channel needs it directly, not only through the Router: the
// "text only, please" receipt for a voice note is written from the read loop
// and never enters the Router, so its at-most-once guarantee has to come from
// the same table.
func NewInboundDeduper(store *Store) engine.Deduper { return &deduper{q: store} }

func (d *deduper) Claim(ctx context.Context, installationID pgtype.UUID, messageID string) (pgtype.UUID, error) {
	row, err := d.q.ClaimChannelInboundDedup(ctx, db.ClaimChannelInboundDedupParams{
		InstallationID: installationID,
		MessageID:      messageID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return pgtype.UUID{}, engine.ErrDuplicate
		}
		return pgtype.UUID{}, err
	}
	return row.ClaimToken, nil
}

func (d *deduper) Mark(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := d.q.MarkChannelInboundDedupProcessed(ctx, db.MarkChannelInboundDedupProcessedParams{
		InstallationID: installationID,
		MessageID:      messageID,
		ClaimToken:     claimToken,
	})
	return err
}

func (d *deduper) Release(ctx context.Context, installationID pgtype.UUID, messageID string, claimToken pgtype.UUID) error {
	_, err := d.q.ReleaseChannelInboundDedup(ctx, db.ReleaseChannelInboundDedupParams{
		InstallationID: installationID,
		MessageID:      messageID,
		ClaimToken:     claimToken,
	})
	return err
}

// ---- session binding ----

type sessionBinder struct{ session engineSessionBinder }

// EnsureSession picks the wecom session-isolation key. For single (p2p)
// chats the wecom ChatID already IS the userid, one session per user;
// for group chats we key on the chatid so all group traffic lands in one
// session — the aibot API does not have a first-class thread concept.
func (r *sessionBinder) EnsureSession(ctx context.Context, p engine.EnsureSessionParams) (pgtype.UUID, error) {
	return r.session.EnsureSession(ctx, engine.EnsureSessionInput{
		WorkspaceID:    p.Installation.WorkspaceID,
		AgentID:        p.Installation.AgentID,
		InstallationID: p.Installation.ID,
		Sender:         p.Sender,
		BindingKey:     p.Message.Source.ChatID,
		ChatType:       p.Message.Source.ChatType,
	})
}

func (r *sessionBinder) AppendMessage(ctx context.Context, p engine.AppendParams) (engine.AppendResult, error) {
	// CommandText is the user's OWN line. A message that quotes another one
	// is stored with the quote in front, so parsing /issue off the stored
	// body would read somebody else's text — and miss the command that
	// follows it. Raw carries the un-quoted line for exactly this.
	command := p.Message.Text
	if wm, err := wecomMsgFromRaw(p.Message); err == nil {
		command = wm.CommandBody
	}
	return r.session.AppendUserMessage(ctx, engine.AppendInput{
		MessageKind:    p.MessageKind(),
		SessionID:      p.SessionID,
		Sender:         p.Sender,
		InstallationID: p.InstallationID,
		Body:           p.Message.Text,
		CommandText:    command,
		MessageID:      p.Message.MessageID,
		ClaimToken:     p.ClaimToken,
		// The budget the agent run waits out before giving up on the
		// attachments. Passing it is what makes the run see the photo rather
		// than only the "[图片]" standing in for it: the task row is deferred
		// to this deadline and promoted early the moment binding finishes.
		// It is zero for a message with no media, so a plain sentence is
		// never held back.
		MediaPendingSeconds: p.MediaPendingSeconds,
	})
}

// BindMedia attaches the objects the MediaResolver stored to this message.
// The Router calls it off the connector ACK path, after the download and
// upload have finished, with whatever survived — an empty ref list still
// arrives, and still has to clear the message's pending marker so the
// deferred run stops waiting.
func (r *sessionBinder) BindMedia(ctx context.Context, p engine.BindMediaParams) error {
	return r.session.BindMediaRefs(ctx, engine.BindMediaInput{
		MessageID:   p.MessageID,
		SessionID:   p.SessionID,
		WorkspaceID: p.WorkspaceID,
		Sender:      p.Sender,
		MediaRefs:   p.MediaRefs,
	})
}

// ---- audit ----

type auditor struct{ q auditQueries }

func (a *auditor) RecordDrop(ctx context.Context, instID pgtype.UUID, msg channel.InboundMessage, reason engine.DropReason) error {
	var eventType string
	if wm, err := wecomMsgFromRaw(msg); err == nil {
		eventType = wm.MsgType
	}
	var instIDArg pgtype.UUID
	if instID.Valid {
		instIDArg = instID
	}
	return a.q.RecordChannelInboundDrop(ctx, db.RecordChannelInboundDropParams{
		InstallationID:   instIDArg,
		ChannelType:      channelTypeWecom,
		ChannelChatID:    textOrNull(msg.Source.ChatID),
		EventType:        eventType,
		ChannelEventID:   textOrNull(msg.EventID),
		ChannelMessageID: textOrNull(msg.MessageID),
		DropReason:       string(reason),
	})
}

// textOrNull maps an empty string to a NULL pgtype.Text — the shared
// RecordChannelInboundDrop query uses sqlc.narg on the id columns so we
// need to pass NULL rather than "".
func textOrNull(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}
