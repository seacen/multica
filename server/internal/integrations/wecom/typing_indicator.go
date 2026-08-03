package wecom

// typing_indicator.go — WeCom's answer to "the bot heard you".
//
// Slack stamps 👀 on the user's message, Feishu a typing badge. WeCom has
// neither: the server-side API publishes no reaction, no read receipt and no
// typing signal at all. What it does have is the streaming message, so the
// same engine.TypingNotifier interface means something different here — the
// indicator IS the reply, opened early and filled in later. Two consequences
// follow from that, and both shape this file:
//
//   - The bubble MUST be closed. A reaction that never clears is untidy; a
//     stream that never finishes is a spinner sitting in the user's chat for
//     good. So every way a turn can end — answered, failed, never started,
//     outlived its window — writes a closing frame carrying visible text.
//   - OnSettled is not the normal ending. As on the other platforms the Router
//     only calls it when the flush produced no task run; the answer closes the
//     bubble from the chat-done subscriber in outbound.go, which is the only
//     place that has the answer to close it with.

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// streamCloseTimeout bounds a closing frame written from a timer or a bus
// subscriber, neither of which has a caller's context to inherit.
const streamCloseTimeout = 10 * time.Second

// TypingIndicatorManager opens the streaming bubble when a message is ingested
// and owns it until something closes it.
type TypingIndicatorManager struct {
	senders *sendersRegistry
	streams *streamStore
	log     *slog.Logger

	// guardAfter is when the manager closes a bubble nobody else has. Zero
	// disables the guard (tests that drive the clock themselves).
	guardAfter time.Duration
}

// TypingIndicatorConfig wires the manager. Senders and Streams are the same
// two instances the outbound subscriber holds — the bubble is opened on one
// side of the process and closed on the other.
type TypingIndicatorConfig struct {
	Senders *sendersRegistry
	Streams *streamStore
	Logger  *slog.Logger

	// GuardAfter overrides streamGuardAfter. Test-only.
	GuardAfter time.Duration
}

var _ engine.TypingNotifier = (*TypingIndicatorManager)(nil)

// NewTypingIndicator builds the manager.
func NewTypingIndicator(cfg TypingIndicatorConfig) *TypingIndicatorManager {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	guard := cfg.GuardAfter
	if guard == 0 {
		guard = streamGuardAfter
	}
	return &TypingIndicatorManager{
		senders:    cfg.Senders,
		streams:    cfg.Streams,
		log:        logger,
		guardAfter: guard,
	}
}

// OnIngested paints the "working on it" bubble and records what it takes to
// come back and fill it in. The Router calls this on a detached goroutine with
// its own deadline, so nothing here needs to be quick for the ACK's sake — but
// everything here is best-effort: a bubble that fails to open costs the user a
// few seconds of uncertainty, and the answer still arrives as a plain message.
func (m *TypingIndicatorManager) OnIngested(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, sessionID pgtype.UUID) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return
	}
	// A standalone /issue is answered by the replier and deliberately never
	// triggers an agent run, so no chat-done event would ever arrive to close
	// a bubble opened for it.
	if msg.SkipAgentRun {
		return
	}
	wm, err := wecomMsgFromRaw(msg)
	if err != nil {
		m.log.WarnContext(ctx, "wecom typing: cannot read the inbound envelope",
			"chat_session_id", util.UUIDToString(sessionID), "error", err)
		return
	}
	// Without the callback's req_id there is no stream to open: the server
	// refuses a frame carrying anything else, and an event callback's req_id
	// is refused outright (846605).
	if wm.ReqID == "" {
		return
	}
	chatID := msg.Source.ChatID
	if chatID == "" {
		chatID = wm.ChatID
	}
	if chatID == "" {
		return
	}

	h := streamHandle{
		ReqID:          wm.ReqID,
		StreamID:       newStreamID(),
		InstallationID: inst.ID,
		ChatID:         chatID,
		ChatType:       aibotChatTypeFromChannel(msg.Source.ChatType),
		Locale:         localeOf(inst),
	}
	if !m.streams.claim(sessionID, h) {
		// A bubble is already open for this session. Two messages inside one
		// debounce window are one run and belong in one bubble.
		return
	}

	if err := m.senders.stream(ctx, h, streamThinkingPlaceholder, false); err != nil {
		if !errors.Is(err, errStreamAckTimeout) {
			m.streams.drop(sessionID)
			m.log.WarnContext(ctx, "wecom typing: opening frame refused",
				"chat_session_id", util.UUIDToString(sessionID), "error", err)
			return
		}
		// The frame went out and the verdict did not come back. Keep the
		// handle: re-sending the same stream id later creates the message if
		// the opening frame was lost, so the worst case is a user who waits
		// without a spinner rather than one who never gets an answer.
		m.log.DebugContext(ctx, "wecom typing: opening frame unacknowledged, keeping the handle",
			"chat_session_id", util.UUIDToString(sessionID))
	}
	m.armGuard(sessionID, h)
}

// OnSettled closes a bubble whose message never became a run — agent offline
// or archived, or an enqueue that failed. This is the only chance to stop the
// spinner: with no task there is no task lifecycle event, so neither the
// chat-done subscriber nor the failure subscriber will ever fire. The copy is
// deliberately thin because the replier's own notice follows as a separate
// message with the reason.
func (m *TypingIndicatorManager) OnSettled(ctx context.Context, sessionID pgtype.UUID) {
	m.closeBubble(ctx, sessionID, func(c copyPack) string { return c.StreamNotStarted }, "settled")
}

// Register subscribes the manager to the run failure event. EventChatDone is
// deliberately NOT subscribed here: the answer belongs in the bubble, and only
// the outbound subscriber holds the answer. Registering for it here would
// close the bubble first and leave the reply to arrive underneath it.
func (m *TypingIndicatorManager) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventTaskFailed, m.handleTaskFailed)
}

func (m *TypingIndicatorManager) handleTaskFailed(e events.Event) {
	sessionID, ok := sessionIDFromEvent(e)
	if !ok {
		return // an issue / autopilot run, with no chat session and no bubble
	}
	ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
	defer cancel()
	m.closeBubble(ctx, sessionID, func(c copyPack) string { return c.StreamFailed }, "task failed")
}

// armGuard schedules the close that happens when nothing else does. WeCom
// stops accepting frames for a stream past streamMaxAge, so a run that outlives
// the window would otherwise leave a spinner we can no longer touch.
func (m *TypingIndicatorManager) armGuard(sessionID pgtype.UUID, h streamHandle) {
	if m.guardAfter <= 0 {
		return
	}
	t := time.AfterFunc(m.guardAfter, func() {
		ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
		defer cancel()
		m.closeBubble(ctx, sessionID, func(c copyPack) string { return c.StreamStillWorking }, "window expiring")
	})
	m.streams.arm(sessionID, h.StreamID, t)
}

// closeBubble seals a session's bubble with the copy pick chooses, if there is
// still a bubble to seal. Taking the handle first makes this idempotent: two
// closers racing produce one closing frame.
func (m *TypingIndicatorManager) closeBubble(ctx context.Context, sessionID pgtype.UUID, pick func(copyPack) string, why string) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return
	}
	h, ok := m.streams.take(sessionID)
	if !ok {
		return
	}
	if err := m.senders.stream(ctx, h, pick(copyFor(h.Locale)), true); err != nil {
		m.log.WarnContext(ctx, "wecom typing: closing frame failed",
			"chat_session_id", util.UUIDToString(sessionID),
			"reason", why, "unusable", streamUnusable(err), "error", err)
	}
}

// sessionIDFromEvent recovers the chat session from a task lifecycle event.
// EventChatDone puts it on the envelope; EventTaskFailed carries it only in the
// broadcast payload, and only for chat runs — so both places are checked.
func sessionIDFromEvent(e events.Event) (pgtype.UUID, bool) {
	if e.ChatSessionID != "" {
		if id, err := util.ParseUUID(e.ChatSessionID); err == nil && id.Valid {
			return id, true
		}
	}
	if p, ok := e.Payload.(map[string]any); ok {
		if s, _ := p["chat_session_id"].(string); s != "" {
			if id, err := util.ParseUUID(s); err == nil && id.Valid {
				return id, true
			}
		}
	}
	return pgtype.UUID{}, false
}
