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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// streamCloseTimeout bounds a closing frame written from a timer or a bus
// subscriber, neither of which has a caller's context to inherit.
const streamCloseTimeout = 10 * time.Second

// progressWriteTimeout and taskLookupTimeout are the whole budget this
// subscriber may spend on somebody else's goroutine, and they add up on
// purpose.
//
// The bus is synchronous: a task:message subscriber runs on the goroutine that
// published the event, which is the daemon's own POST
// /api/daemon/tasks/{id}/messages handler — and the daemon gives that request
// five seconds before it gives up. It does not retry: a batch that misses the
// deadline is gone, and with it the transcript every other surface reads. So
// the ceiling here is not "how long is a refresh worth waiting for" but "how
// much of the daemon's five seconds may a spinner in one chat borrow". Both
// numbers together are 1.8s, which leaves the request its own work with room
// to spare, and a refresh that has not been acked inside a second is one the
// user would not have seen anyway — the next tool call is 500ms behind it.
//
// progressWriteTimeout is a ceiling and not a hope: respondStream bounds the
// wait for the writer and the socket write by it as well as the wait for the
// ack, so the whole refresh returns inside the second whatever the socket is
// doing (ws_sender.go).
const (
	progressWriteTimeout = 1 * time.Second
	taskLookupTimeout    = 800 * time.Millisecond
)

// taskLookup is the one query the progress refresh needs. task:progress
// carries a task id and no chat session, so the session the bubble is keyed on
// has to be read back off the task row. *db.Queries satisfies it.
type taskLookup interface {
	GetAgentTask(ctx context.Context, id pgtype.UUID) (db.AgentTaskQueue, error)
}

// TypingIndicatorManager opens the streaming bubble when a message is ingested
// and owns it until something closes it.
type TypingIndicatorManager struct {
	senders *sendersRegistry
	streams *streamStore
	tasks   taskLookup
	log     *slog.Logger

	// taskSessions remembers which chat a task belongs to — and which tasks
	// belong to no chat at all — so a run's dozens of tool messages cost one
	// database read between them.
	taskSessions *taskSessionCache

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

	// Tasks resolves a task id to its chat session for the progress refresh.
	// Nil leaves the bus-driven refresh off; UpdateProgress still works for
	// any caller that already knows the session.
	Tasks taskLookup

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
		senders:      cfg.Senders,
		streams:      cfg.Streams,
		tasks:        cfg.Tasks,
		taskSessions: newTaskSessionCache(),
		log:          logger,
		guardAfter:   guard,
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

// Register subscribes the manager to the run failure event, and — when a task
// lookup was configured — to the two events that say where a run has got to.
//
// task:progress fires twice per run and both lines are the daemon's own
// ("Launching claude", "Finishing task"), so on its own it leaves the whole
// middle of a run blank. task:message is the transcript: one event per tool
// call, batched about twice a second, and the only signal fine-grained enough
// to answer what the agent is doing right now. Both feed the same list.
//
// EventChatDone is deliberately NOT subscribed here: the answer belongs in the
// bubble, and only the outbound subscriber holds the answer. Registering for
// it here would close the bubble first and leave the reply to arrive
// underneath it.
func (m *TypingIndicatorManager) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventTaskFailed, m.handleTaskFailed)
	if m.tasks != nil {
		bus.Subscribe(protocol.EventTaskProgress, m.handleTaskProgress)
		bus.Subscribe(protocol.EventTaskMessage, m.handleTaskMessage)
	}
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

// UpdateProgress adds one line to an open bubble's list of steps and refreshes
// it, leaving it open. It is the whole in-flight-refresh surface: anything that
// learns something worth saying mid-run — the bus subscribers below, or a
// future caller closer to the agent — needs only a session and a sentence.
//
// The line is taken as written, so callers on this path must have vetted it
// already; the subscribers below go through progress_render.go, which is where
// the rule about what may reach a WeCom chat is enforced.
func (m *TypingIndicatorManager) UpdateProgress(ctx context.Context, sessionID pgtype.UUID, text string) {
	text = safeFragment(text)
	if text == "" {
		return
	}
	m.recordStep(ctx, sessionID, progressStep{kind: progressRaw, arg: text})
}

// recordStep folds one step into a session's bubble and writes the result.
//
// Three properties are deliberate. Content is a full replacement, not a delta,
// so a frame carries the whole list and no caller has to know what the bubble
// says now. A refresh yields when the previous frame has not been acked (the
// backpressure the official SDK calls replyStreamNonBlocking) because progress
// is worth less than the answer behind it, and it yields again to the feed's
// own minimum interval, which folds a burst of tool calls into one frame. And a
// refresh that merely fails leaves the spinner exactly as it was, for the
// answer or the guard to finish properly.
//
// The one thing a refresh does end is a bubble the server has disowned. 846608
// and 846605 mean this stream will never take another frame, so keeping the
// handle would buy a refusal every 1.5s for the rest of the run and then spend
// the answer's own ack timeout learning the same thing. Letting the handle go
// costs nothing that was still available: the answer arrives as a plain
// message, which is exactly where it was heading anyway.
func (m *TypingIndicatorManager) recordStep(ctx context.Context, sessionID pgtype.UUID, step progressStep) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return
	}
	h, feed, ok := m.streams.feedFor(sessionID)
	if !ok {
		return // no bubble open — a non-wecom session, or one already answered
	}
	content := feed.record(step, copyFor(h.Locale), h.CreatedAt)
	if content == "" {
		// Inside the refresh interval. The step is in the list and the next
		// frame carries it; sending one per tool call would spend the bot's
		// socket on frames nobody can read that fast.
		return
	}
	err := m.senders.stream(ctx, h, content, false)
	switch {
	case err == nil:
	case errors.Is(err, errStreamBusy), errors.Is(err, errStreamSuperseded):
		// The previous frame is still in flight, or the answer got there
		// first. Skipping is the design in both cases.
	case streamUnusable(err):
		m.streams.drop(sessionID)
		m.log.DebugContext(ctx, "wecom typing: bubble disowned by the server, answering as a new message",
			"chat_session_id", util.UUIDToString(sessionID), "error", err)
	default:
		m.log.DebugContext(ctx, "wecom typing: progress refresh failed",
			"chat_session_id", util.UUIDToString(sessionID), "error", err)
	}
}

// handleTaskProgress plays the daemon's own two milestones into the bubble.
func (m *TypingIndicatorManager) handleTaskProgress(e events.Event) {
	if m.streams == nil || m.streams.depth() == 0 {
		return
	}
	summary := safeFragment(progressSummary(e.Payload))
	if summary == "" {
		return
	}
	m.refreshFromTask(e, progressStep{kind: progressRaw, arg: summary})
}

// handleTaskMessage plays the run's transcript into the bubble — the tool
// calls that are the whole middle of a run.
//
// The order of the two cheap rejections matters. The store is checked first so
// that a deployment with no WeCom turn in flight pays nothing at all for this
// subscription; the message is classified second, which drops tool results and
// agent prose — most of the event's volume — before anything reaches the
// database.
func (m *TypingIndicatorManager) handleTaskMessage(e events.Event) {
	if m.streams == nil || m.streams.depth() == 0 {
		return
	}
	step, ok := stepFromTaskMessage(e.Payload)
	if !ok {
		return
	}
	m.refreshFromTask(e, step)
}

// refreshFromTask resolves the chat session behind a task event and records
// the step against it. Neither event carries the session, so it comes off the
// task row — via the cache, which is what keeps a run's dozens of tool
// messages down to one read, and which also remembers the tasks that have no
// chat session at all so an issue run never asks twice.
//
// Everything here runs on the goroutine that published the event: the daemon's
// own HTTP handler. Hence the short deadlines.
func (m *TypingIndicatorManager) refreshFromTask(e events.Event, step progressStep) {
	sessionID, ok := sessionIDFromEvent(e)
	if !ok {
		if m.tasks == nil {
			return
		}
		taskID := taskIDFromEvent(e)
		if taskID == "" {
			return
		}
		cached, hit := m.taskSessions.get(taskID)
		if hit {
			sessionID = cached
		} else {
			id, err := util.ParseUUID(taskID)
			if err != nil || !id.Valid {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), taskLookupTimeout)
			task, err := m.tasks.GetAgentTask(ctx, id)
			cancel()
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				// A row that is not there has no chat session and never will —
				// a run cancelled and deleted while its transcript was still
				// flushing. Remembering that is what stops its remaining
				// messages from putting a read behind every one.
				m.taskSessions.put(taskID, pgtype.UUID{})
				return
			case err != nil:
				return // a read that failed; not cached, the next one may work
			}
			sessionID = task.ChatSessionID
			m.taskSessions.put(taskID, sessionID)
		}
		if !sessionID.Valid {
			return // an issue / autopilot run, now known to have no bubble
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), progressWriteTimeout)
	defer cancel()
	m.recordStep(ctx, sessionID, step)
}

// progressSummary reads the human-readable line off a task:progress payload,
// typed or in its map form after a serialization round trip.
func progressSummary(payload any) string {
	switch p := payload.(type) {
	case protocol.TaskProgressPayload:
		return p.Summary
	case map[string]any:
		if s, _ := p["summary"].(string); s != "" {
			return s
		}
	}
	return ""
}

// taskIDFromEvent prefers the envelope's routing hint and falls back to the
// payload.
func taskIDFromEvent(e events.Event) string {
	if e.TaskID != "" {
		return e.TaskID
	}
	switch p := e.Payload.(type) {
	case protocol.TaskProgressPayload:
		return p.TaskID
	case protocol.TaskMessagePayload:
		return p.TaskID
	case map[string]any:
		s, _ := p["task_id"].(string)
		return s
	}
	return ""
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
