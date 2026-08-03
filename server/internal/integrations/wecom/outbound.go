package wecom

// outbound.go — the WeCom EventChatDone subscriber. After an agent finishes
// producing a chat reply on the bus, this subscriber delivers it to the chat
// that asked. Mirrors slack.Outbound; sessions with no wecom binding are
// ignored so it coexists with Slack / Lark subscribers on the shared bus.
//
// There are two ways out, and which one is used decides what the user sees.
// If the typing indicator opened a streaming bubble for this turn
// (stream_store.go), the answer replaces that bubble in place and the
// conversation reads as one question and one answer. If it did not — no
// indicator, a restart since, a run past the protocol's stream window — the
// answer goes out as a fresh aibot_send_msg through the holding queue, exactly
// as it did before. The fallback is not an error path: it is the same delivery
// the adapter has always made, and it is what the streaming path degrades to.
//
// Kept lean otherwise: aibot has no threading, no per-bot outbound REST, and no
// mrkdwn conversion — the reply text goes through sendMsgTextBody the
// same way OutboundReplier's messages do (markdown msgtype, which
// renders plaintext without escaping).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// outboundQueries is the slice of generated queries the WeCom outbound
// subscriber needs. *db.Queries satisfies it.
type outboundQueries interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
	FindChannelBindingForMember(ctx context.Context, arg db.FindChannelBindingForMemberParams) (db.ChannelUserBinding, error)
	GetWorkspace(ctx context.Context, id pgtype.UUID) (db.Workspace, error)
}

// Outbound delivers an agent's chat reply back to WeCom over the same
// aibot WebSocket the inbound loop owns. Registered against the shared
// event bus; sessions with no wecom binding are silently ignored.
type Outbound struct {
	q       outboundQueries
	senders *sendersRegistry
	streams *streamStore
	logger  *slog.Logger
}

// NewOutbound builds the WeCom outbound subscriber. senders is the same
// process-wide registry the wecom.ChannelDeps and OutboundReplier were
// built with — reply delivery goes through the live wsSender for the
// binding's installation, so a session whose Supervisor lost the lease
// mid-flight silently drops rather than opening a second connection.
//
// streams is the same store the typing indicator writes to; nil disables the
// in-place reply and leaves every answer going out as a new message.
func NewOutbound(q outboundQueries, senders *sendersRegistry, streams *streamStore, logger *slog.Logger) *Outbound {
	if logger == nil {
		logger = slog.Default()
	}
	return &Outbound{q: q, senders: senders, streams: streams, logger: logger}
}

// Register subscribes to the chat-done event on the bus.
func (o *Outbound) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventChatDone, o.handleEvent)
	// Inbox notifications delivered through the smart bot: when the
	// recipient member has a WeCom binding with a live connection, their
	// inbox:new items are pushed to the aibot as a markdown card.
	bus.Subscribe(protocol.EventInboxNew, o.handleInboxNew)
}

func (o *Outbound) handleEvent(e events.Event) {
	// Bus delivery is synchronous — a stuck WS write must not wedge the
	// publish call site. Fresh ctx with a tight timeout, same as Slack.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := o.processEvent(ctx, e); err != nil {
		o.logger.WarnContext(ctx, "wecom outbound: reply delivery failed",
			"error", err, "chat_session_id", e.ChatSessionID)
	}
}

func (o *Outbound) processEvent(ctx context.Context, e events.Event) error {
	sessionID, err := util.ParseUUID(e.ChatSessionID)
	if err != nil || !sessionID.Valid {
		// Issue / autopilot tasks carry no chat_session.
		return nil
	}

	content := chatDoneContent(e.Payload)

	// The bubble the question opened, if there is one. Taken up front and
	// unconditionally: from here on this turn owns it, and a handle left in
	// the store after the turn ends is a handle pointing at a sealed message.
	if handle, streaming := o.takeStream(sessionID); streaming {
		// A bubble on screen has to end in words. An empty completion is a
		// legitimate outcome — the agent had nothing to add — but an endless
		// spinner is not, so the copy stands in for the silence.
		text := content
		if !hasVisibleChar(text) {
			text = copyFor(handle.Locale).StreamNoReply
		}
		// A bubble the server disowned mid-run is not tried again: the typing
		// indicator has already been told this stream takes no frame, and has
		// already told the user the answer would arrive as a new message. This
		// is that message.
		if !handle.Unusable {
			if err := o.finishStream(ctx, handle, text); err == nil {
				return nil
			}
		}
		// The frame was refused. Say it as a new message instead — and never
		// queue the stream frame itself, whose req_id will have expired long
		// before a reconnect could replay it.
		content = text
	}
	if content == "" {
		return nil // nothing to say (empty completion, no bubble to close)
	}

	binding, err := o.q.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: sessionID,
		ChannelType:   channelTypeWecom,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // not a wecom session (Slack / Lark / web-only)
		}
		return fmt.Errorf("wecom: lookup chat binding: %w", err)
	}
	inst, err := o.q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecom,
	})
	if err != nil {
		return fmt.Errorf("wecom: load installation: %w", err)
	}
	if inst.Status != string(InstallationActive) {
		return nil // revoked between trigger and reply
	}
	if o.senders == nil {
		return errors.New("wecom: sender registry not configured")
	}
	// No live connection (lease flip, Supervisor backoff, a revoke a second
	// ago) means the reply waits for the next one rather than disappearing —
	// see outbound_queue.go. The user asked a question; an answer a minute
	// late still answers it.
	chatType := aibotChatTypeFromChannel(channel.ChatType(binding.ChatType))
	if err := o.senders.send(inst.ID, pendingSend{
		ChatID:   binding.ChannelChatID,
		ChatType: chatType,
		Content:  content,
	}); err != nil {
		return err
	}
	// An answer is the round's ending whether it went into the bubble or
	// underneath it, and this is the branch where no handle was consumed to
	// record that. Without the note, a task:failed arriving behind a delivered
	// answer — an auto-retry's first attempt, a sweeper that ran late — reaches
	// the typing indicator with nothing to tell it the round is over, and
	// contradicts the answer the user is already reading.
	if o.streams != nil {
		o.streams.remember(sessionID, roundAddress{
			InstallationID: inst.ID,
			ChatID:         binding.ChannelChatID,
			ChatType:       chatType,
			Locale:         localeOfRow(inst),
		}, roundOver)
	}
	return nil
}

// takeStream hands over the bubble open for this session, if the typing
// indicator managed to open one and it is still inside the protocol's window.
func (o *Outbound) takeStream(sessionID pgtype.UUID) (streamHandle, bool) {
	if o.streams == nil || o.senders == nil {
		return streamHandle{}, false
	}
	return o.streams.take(sessionID, roundOver)
}

// finishStream writes the answer into the bubble and seals it. A failure here
// is not fatal to the reply — it means the caller falls back to a new message —
// so it is logged with the one detail that explains it: whether the stream is
// beyond saving (past its window, bad req_id) or the socket simply blinked.
func (o *Outbound) finishStream(ctx context.Context, h streamHandle, text string) error {
	err := o.senders.stream(ctx, h, text, true)
	if err == nil {
		return nil
	}
	o.logger.WarnContext(ctx, "wecom outbound: in-place reply failed, sending a new message instead",
		"installation_id", uuidStringPub(h.InstallationID),
		"stream_unusable", streamUnusable(err), "error", err)
	return err
}

// chatDoneContent extracts the reply text from an EventChatDone payload
// (the typed payload, or its map form after a serialization round trip).
func chatDoneContent(payload any) string {
	switch p := payload.(type) {
	case protocol.ChatDonePayload:
		return p.Content
	case map[string]any:
		if s, ok := p["content"].(string); ok {
			return s
		}
	}
	return ""
}

// handleInboxNew is the inbox:new subscriber that delivers a member
// notification via the smart bot. When the recipient member has a WeCom
// binding with a live connection, the notification is pushed to the aibot.
// On any miss — non-member recipient, no wecom binding, a revoked
// installation, an unsendable body — the handler is a no-op and the member
// simply receives the notification through the in-app inbox as usual.
func (o *Outbound) handleInboxNew(e events.Event) {
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}
	item, ok := payload["item"].(map[string]any)
	if !ok {
		return
	}
	// Only member recipients — agents receive nothing via chat channels.
	if rt, _ := item["recipient_type"].(string); rt != "member" {
		return
	}
	recipientIDStr, _ := item["recipient_id"].(string)
	workspaceIDStr, _ := item["workspace_id"].(string)
	if recipientIDStr == "" || workspaceIDStr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	o.tryDeliverInbox(ctx, item, recipientIDStr, workspaceIDStr)
}

// tryDeliverInbox is the delivery core. Returns true iff the bot pushed
// the notification.
func (o *Outbound) tryDeliverInbox(ctx context.Context, item map[string]any, recipientIDStr, workspaceIDStr string) bool {
	recipientID, err := util.ParseUUID(recipientIDStr)
	if err != nil || !recipientID.Valid {
		return false
	}
	workspaceID, err := util.ParseUUID(workspaceIDStr)
	if err != nil || !workspaceID.Valid {
		return false
	}
	binding, err := o.q.FindChannelBindingForMember(ctx, db.FindChannelBindingForMemberParams{
		WorkspaceID:   workspaceID,
		MulticaUserID: recipientID,
		ChannelType:   channelTypeWecom,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			o.logger.WarnContext(ctx, "wecom outbound: lookup member binding failed",
				"error", err, "workspace_id", workspaceIDStr, "recipient_id", recipientIDStr)
		}
		return false // no binding → nothing to deliver via bot
	}
	if o.senders == nil {
		return false
	}

	// One row answers two questions: whether the bot is still installed, and
	// which language its copy speaks. A revoked installation stops here — the
	// binding row outlives the revoke, so the member still looks reachable.
	// A lookup failure is not fatal for the locale — the default copy still
	// says something useful, and dropping a notification over a language
	// choice would be worse than sending it in the wrong one.
	cp := copyFor(DefaultLocale)
	if inst, err := o.q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecom,
	}); err == nil {
		if inst.Status != string(InstallationActive) {
			o.logger.DebugContext(ctx, "wecom outbound: inbox push skipped, installation not active",
				"installation_id", uuidStringPub(binding.InstallationID),
				"status", inst.Status, "recipient_id", recipientIDStr)
			return false
		}
		cp = copyFor(localeOfRow(inst))
	} else {
		o.logger.WarnContext(ctx, "wecom outbound: installation lookup for inbox locale failed",
			"error", err, "installation_id", uuidStringPub(binding.InstallationID))
	}

	// Resolve slug for the link. Best-effort — a missing slug just falls
	// back to the workspace UUID in the URL.
	slug := ""
	if ws, err := o.q.GetWorkspace(ctx, workspaceID); err == nil {
		slug = ws.Slug
	}
	content := buildInboxMarkdown(item, workspaceIDStr, slug, cp)
	if content == "" {
		return false
	}
	// Smart-bot inbox notifications are 1:1 pushes to the bound user. The
	// binding row's channel_user_id is the bot-scoped T-* userid — WeCom
	// treats that as the chatid for a single (chat_type=1) send. With no live
	// connection the card is held for the reconnect (outbound_queue.go)
	// rather than dropped.
	if err := o.senders.send(binding.InstallationID, pendingSend{
		ChatID:   binding.ChannelUserID,
		ChatType: chatTypeSingleInt,
		Content:  content,
	}); err != nil {
		o.logger.WarnContext(ctx, "wecom outbound: inbox push failed",
			"error", err, "installation_id", uuidStringPub(binding.InstallationID),
			"recipient_id", recipientIDStr)
		return false // send failed → no bot delivery
	}
	o.logger.DebugContext(ctx, "wecom outbound: inbox delivered via bot",
		"installation_id", uuidStringPub(binding.InstallationID),
		"recipient_id", recipientIDStr,
		"inbox_type", item["type"])
	return true
}

// uuidStringPub renders a pgtype.UUID for a log line without depending on
// engine.uuidString (a different package).
func uuidStringPub(u pgtype.UUID) string {
	return util.UUIDToString(u)
}
