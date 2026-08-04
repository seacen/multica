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
	"strings"
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

// chatBindingLookup finds which WeCom chat a session belongs to. It is the
// address of last resort for a failure notice: the handle is gone and this
// process has no note of the round, which is what a restart mid-run looks like.
// The two queries are the ones outbound.go already makes on the same path for
// the answer, so *db.Queries satisfies both interfaces without a new query.
type chatBindingLookup interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
}

// TypingIndicatorManager opens a streaming bubble per round when messages are
// ingested and owns each one until something closes it.
type TypingIndicatorManager struct {
	senders    *sendersRegistry
	streams    *streamStore
	tasks      taskLookup
	identities identityLookup
	languages  languageLookup
	bindings   chatBindingLookup
	log        *slog.Logger

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

	// Tasks resolves a task id to its chat session — for the progress refresh,
	// and for the task:failed the sweepers publish without one. Nil leaves the
	// bus-driven refresh off and limits failure handling to the events that
	// carry a session; UpdateProgress still works for any caller that already
	// knows the session.
	Tasks taskLookup

	// Identities resolves the WeCom sender id on an inbound message to the
	// Multica user behind it, which is the only way to tell the principal's
	// own chat from a colleague's. Nil leaves every bubble on the closed tier:
	// a deployment that cannot prove who is asking must not assume.
	//
	// The Router resolves the same binding a moment earlier and its
	// TypingNotifier interface does not carry the answer across, so this reads
	// it again. That costs one indexed row per turn, on the detached goroutine
	// the Router already gives OnIngested, so it is nowhere near the ACK.
	Identities identityLookup

	// Languages resolves the asker to their Multica profile language, which is
	// what the bubble's closers speak (language.go). Nil means DefaultLocale.
	Languages languageLookup

	// Bindings finds the chat behind a session when nothing else can — a run
	// that failed after this process was restarted, or after a bubble that was
	// never painted. Nil keeps the failure notice to the rounds this process
	// has a handle or a note for.
	Bindings chatBindingLookup

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
		identities:   cfg.Identities,
		languages:    cfg.Languages,
		bindings:     cfg.Bindings,
		taskSessions: newTaskSessionCache(),
		log:          logger,
		guardAfter:   guard,
	}
}

// OnIngested paints a "working on it" bubble for the round this message
// starts and records what it takes to come back and fill it in. A message
// inside the debounce window joins the round already on screen; a message
// past it — one that will wait behind the run in flight — opens a bubble of
// its own immediately, because a wait with nothing on screen reads as a
// message that was lost. The bubble carries no words while it waits: the
// client's own loading affordance is the receipt, and words would need a
// language before there is anything to say.
//
// The Router calls this on a detached goroutine with its own deadline, so
// nothing here needs to be quick for the ACK's sake — but everything here is
// best-effort: a bubble that fails to open costs the user a few seconds of
// uncertainty, and the answer still arrives as a plain message.
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
	// Stamp the arrival now, before the lookups below. Whether two messages
	// belong to one round is a question about when they were SENT, and the
	// engine's debouncer answers it from arrival. Reading the clock after
	// three database round trips answers a different question — one that a
	// loaded pool can move by seconds — so the two disagreed: a message the
	// debouncer gave its own run could be folded into the previous round
	// here, leaving it with no bubble and no receipt, or the reverse, leaving
	// one run with two spinners.
	arrivedAt := m.streams.clock()
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
		Locale:         localeFor(ctx, m.languages, inst.ID, aibotChatTypeFromChannel(msg.Source.ChatType), msg.Source.SenderID),
		Level:          m.levelFor(ctx, inst, msg),
		CreatedAt:      arrivedAt,
	}
	if m.streams.open(sessionID, h) == roundJoined {
		// Inside the debounce window: the batcher is about to fold this
		// message into the round whose bubble is already on screen, and that
		// bubble is this message's receipt too.
		return
	}

	// Three ways the opening frame does not land, and only one of them is a
	// reason to give the bubble up.
	//
	// An ack that did not come back says nothing about whether the frame did:
	// re-sending the same stream id later creates the message if the opening
	// frame was lost, so the worst case is a user who waits without a spinner
	// rather than one who never gets an answer.
	//
	// Busy and superseded mean something stronger. Both say another frame on
	// this req_id got to the socket first — a straggling refresh from the
	// previous turn is enough — and that frame carried a stream id of its own,
	// which is exactly how a bubble is created. So the spinner is on the user's
	// screen. Dropping the handle there arms no guard and leaves nothing that
	// could ever close it.
	//
	// A verdict from the server is the one that ends it: 846605 and 846608 mean
	// this stream will never take a frame, so no bubble was painted and keeping
	// the handle would swallow the answer rather than deliver it.
	if err := m.senders.stream(ctx, h, streamThinkingPlaceholder, false); err != nil {
		switch {
		case errors.Is(err, errStreamAckTimeout),
			errors.Is(err, errStreamBusy),
			errors.Is(err, errStreamSuperseded):
			m.log.DebugContext(ctx, "wecom typing: opening frame did not land, keeping the handle",
				"chat_session_id", util.UUIDToString(sessionID), "error", err)
		default:
			m.streams.drop(sessionID, h.StreamID)
			m.log.WarnContext(ctx, "wecom typing: opening frame refused",
				"chat_session_id", util.UUIDToString(sessionID), "error", err)
			return
		}
	}
	m.armGuard(sessionID, h)
}

// levelFor decides how much of the run this bubble may show, once, while the
// facts it needs are still in hand: who asked, and where.
//
// One condition, stated two ways. The chat has to be a one-to-one, and the
// person on the other end of it has to be the principal. A group fails the
// first: the WeCom session key for a room is the room, so every member reads
// the same bubble, and the person who asked is one of several. A colleague's
// own chat fails the second: private, but not to the principal.
//
// Everything unknown resolves to the closed tier. No lookup configured, no
// sender id, a binding that is not there, a database that did not answer —
// none of those prove the reader is the principal, and this is the side to be
// wrong on. The cost of being wrong the other way is a file path in a chat
// that cannot unsend it.
func (m *TypingIndicatorManager) levelFor(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage) progressLevel {
	if aibotChatTypeFromChannel(msg.Source.ChatType) != chatTypeSingleInt {
		return progressLevelNone
	}
	principal := principalOf(inst)
	if !principal.Valid || m.identities == nil {
		return progressLevelNone
	}
	senderID := strings.TrimSpace(msg.Source.SenderID)
	if senderID == "" {
		return progressLevelNone
	}
	binding, err := m.identities.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: inst.ID,
		ChannelUserID:  senderID,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			m.log.WarnContext(ctx, "wecom typing: cannot tell who is asking, showing no steps",
				"installation_id", util.UUIDToString(inst.ID), "error", err)
		}
		return progressLevelNone
	}
	if binding.MulticaUserID != principal {
		return progressLevelNone
	}
	return progressLevelDetail
}

// principalOf names whose bot this is.
//
// The installer is the default and is right for the case this adapter is built
// for: one person sets the bot up on their own workspace and talks to it. It
// stops being right the moment an admin installs on someone else's behalf, so
// an installation may name the principal itself — same shape as the locale,
// an optional key in the config JSONB, absent on every row that predates it. A
// value that will not parse falls back rather than failing: a typo should cost
// the wrong audience, not the whole feature.
func principalOf(inst engine.ResolvedInstallation) pgtype.UUID {
	if p, ok := inst.Platform.(Installation); ok && strings.TrimSpace(p.PrincipalUserID) != "" {
		if id, err := util.ParseUUID(strings.TrimSpace(p.PrincipalUserID)); err == nil && id.Valid {
			return id
		}
	}
	return inst.InstallerUserID
}

// OnSettled closes the bubble of a round that never became a run — agent
// offline or archived, or an enqueue that failed. This is the only chance to
// stop that spinner: with no task there is no task lifecycle event, so neither
// the chat-done subscriber nor the failure subscriber will ever fire. The copy
// is deliberately thin because the replier's own notice follows as a separate
// message with the reason.
//
// The TAIL is the right bubble: OnSettled answers the flush that just
// settled, which is the newest round — the ones ahead of it belong to runs
// that are already real and end through their own events.
//
// With no bubble to close this says nothing, and deliberately: there is no
// spinner to stop, and the replier's notice is already on its way with the part
// the user actually needs. That is the one difference from handleTaskFailed,
// whose copy is the only account of a failure there is.
func (m *TypingIndicatorManager) OnSettled(ctx context.Context, sessionID pgtype.UUID) {
	m.closeBubble(ctx, sessionID,
		func(s *streamStore) (streamHandle, bool) { return s.takeTail(sessionID, roundOver) },
		func(c copyPack) string { return c.StreamNotStarted }, "settled")
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

// handleTaskFailed says a run died, in the bubble if there still is one and as
// a plain message if there is not.
//
// task:failed has two publishers and they do not agree on the payload.
// broadcastTaskEvent, which FailTask uses, carries chat_session_id.
// HandleFailedTasks — every sweeper, recover-orphans, and the daemon heartbeat
// timeout — carries task_id, agent_id, issue_id, status and failure_reason, and
// no session anywhere. That second publisher is the whole crashed-daemon path,
// so the bubble used to spin on until the guard replaced it with "still
// working, I'll reply separately" — a promise about a run that had been dead
// for five minutes. The task-id fallback the progress refresh already uses
// covers it, and it stays on this side of the boundary: the adapter carries its
// own routing rather than widening a payload three other consumers read.
//
// The bubble is not the whole of it, which is what this used to assume. A
// handle is consumed by whichever ending gets there first, and the guard is
// allowed to be that one at the five-minute mark while the run carries on — so
// every run longer than five minutes that then failed found no handle here and
// returned in silence. StreamFailed is the only "that run did not go through"
// WeCom ever produces: the replier speaks for needs_binding, offline, archived
// and issue_created and for nothing else. So the handle is one address among
// three, and sayTheRunFailed works down the rest.
func (m *TypingIndicatorManager) handleTaskFailed(e events.Event) {
	if m.streams == nil {
		return
	}
	if m.bindings == nil && m.streams.depth() == 0 && m.streams.remembered() == 0 {
		// Nothing open, nothing owed, and no way to find a chat: no reason to
		// read a row for someone else's run.
		return
	}
	sessionID, ok := m.sessionFor(e)
	if !ok {
		return // an issue / autopilot run, with no chat session and no bubble
	}
	ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
	defer cancel()
	taskID := taskIDFromEvent(e)
	if m.closeBubble(ctx, sessionID,
		func(s *streamStore) (streamHandle, bool) { return s.takeTask(sessionID, taskID, roundOver) },
		func(c copyPack) string { return c.StreamFailed }, "task failed") {
		return
	}
	m.sayTheRunFailed(ctx, sessionID, taskID)
}

// sayTheRunFailed delivers the failure to a round whose bubble is already gone.
//
// Two addresses, in the order of how much they are trusted. The note the handle
// left behind is the chat the question came from, which is what the guard's
// promise was made in and what the binding row may no longer point at. Failing
// that — a restart mid-run, a turn whose opening frame the server refused —
// the binding row is the only address there is.
//
// A round the store says is accounted for gets nothing. That is the whole of
// the not-twice rule: the answer took the handle the same way the guard does,
// and a task:failed arriving behind a delivered answer — an auto-retry's first
// attempt, a sweeper that ran late — must not contradict it.
func (m *TypingIndicatorManager) sayTheRunFailed(ctx context.Context, sessionID pgtype.UUID, taskID string) {
	if m.senders == nil {
		return
	}
	addr, verdict := m.streams.claimEnding(sessionID)
	switch verdict {
	case roundToldAlready:
		return
	case roundForgotten:
		found, ok := m.addressFromBinding(ctx, sessionID)
		if !ok {
			return
		}
		addr = found
		// No handle was consumed and no note was claimed on this path, so
		// filing one is the only thing that stops the next publisher of the
		// same news from repeating it.
		m.streams.remember(sessionID, addr, roundOver, taskID)
	}
	if err := m.senders.send(addr.InstallationID, pendingSend{
		ChatID:   addr.ChatID,
		ChatType: addr.ChatType,
		Content:  copyFor(addr.Locale).StreamFailed,
	}); err != nil {
		m.log.WarnContext(ctx, "wecom typing: could not say the run failed",
			"chat_session_id", util.UUIDToString(sessionID),
			"installation_id", util.UUIDToString(addr.InstallationID), "error", err)
	}
}

// addressFromBinding reads the chat a session belongs to off the binding row —
// the same two queries outbound.go makes when an answer has no bubble to land
// in. A session with no wecom binding is not ours to speak in: this subscriber
// sees every failed run on a shared bus, including Slack's and the web UI's.
func (m *TypingIndicatorManager) addressFromBinding(ctx context.Context, sessionID pgtype.UUID) (roundAddress, bool) {
	if m.bindings == nil {
		return roundAddress{}, false
	}
	binding, err := m.bindings.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: sessionID,
		ChannelType:   channelTypeWecom,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			m.log.WarnContext(ctx, "wecom typing: cannot find the chat a failed run belongs to",
				"chat_session_id", util.UUIDToString(sessionID), "error", err)
		}
		return roundAddress{}, false
	}
	// The installation row answers whether the bot is still installed. The
	// binding row outlives a revoke, so a session keeps looking reachable
	// after the bot has been removed.
	inst, err := m.bindings.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecom,
	})
	if err != nil {
		m.log.WarnContext(ctx, "wecom typing: cannot load the installation a failed run belongs to",
			"installation_id", util.UUIDToString(binding.InstallationID), "error", err)
		return roundAddress{}, false
	}
	if inst.Status != string(InstallationActive) {
		return roundAddress{}, false
	}
	chatType := aibotChatTypeFromChannel(channel.ChatType(binding.ChatType))
	return roundAddress{
		InstallationID: binding.InstallationID,
		ChatID:         binding.ChannelChatID,
		ChatType:       chatType,
		Locale:         localeFor(ctx, m.languages, binding.InstallationID, chatType, binding.ChannelChatID),
	}, true
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
	m.recordStep(ctx, sessionID, "", progressStep{kind: progressRaw, arg: text})
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
// and 846605 mean this stream will never take another frame, so the bubble is
// marked and no refresh attempts one again — otherwise it would buy a refusal
// every 1.5s for the rest of the run and then spend the answer's own ack
// timeout learning the same thing. The handle itself stays: the round is still
// running, and the handle is the only address its ending has.
func (m *TypingIndicatorManager) recordStep(ctx context.Context, sessionID pgtype.UUID, taskID string, step progressStep) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return
	}
	h, feed, ok := m.streams.feedFor(sessionID, taskID)
	if !ok {
		// No bubble open — a non-wecom session, one already answered, or a
		// bubble that belongs to a different run than the one speaking.
		return
	}
	if h.Level == progressLevelNone {
		// A group, or a colleague's own chat. The bubble stays as the opening
		// frame painted it and the answer seals it; nothing in between is
		// theirs to read. Stopping here rather than one layer down is what
		// keeps a bubble that shows nothing from spending a socket write per
		// tool call for the length of the run.
		return
	}
	content := feed.record(step, copyFor(h.Locale), h.CreatedAt, h.Level)
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
		if !m.streams.markUnusable(sessionID, h.StreamID) {
			// A refresh that raced into the same refusal got here first and has
			// already said it.
			return
		}
		m.log.WarnContext(ctx, "wecom typing: bubble disowned by the server, answering as a new message",
			"chat_session_id", util.UUIDToString(sessionID),
			"installation_id", util.UUIDToString(h.InstallationID), "error", err)
		// Nothing can close this bubble now — the stream takes no further
		// frame — so the spinner turns for good and the user is owed a word
		// about where the rest of the round is going to appear instead. Said
		// once: the mark is what makes this the only refresh that gets here.
		// Bounded by the caller's ctx, not by the socket's own ten seconds:
		// this runs on the daemon's transcript-post path, whose budget is five,
		// and a notice is not worth the post it would cost. A write cut short
		// is held for the reconnect, so saying it stays owed either way.
		if sendErr := m.senders.sendCtx(ctx, h.InstallationID, pendingSend{
			ChatID:   h.ChatID,
			ChatType: h.ChatType,
			Content:  copyFor(h.Locale).StreamStuck,
		}); sendErr != nil {
			m.log.WarnContext(ctx, "wecom typing: could not say the bubble is stuck",
				"chat_session_id", util.UUIDToString(sessionID), "error", sendErr)
		}
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
// calls and the reasoning between them, which are the whole middle of a run.
//
// The order of the two cheap rejections matters. The store is checked first so
// that a deployment with no WeCom turn in flight pays nothing at all for this
// subscription; the message is classified second, which drops tool results and
// the answer being written — most of the event's volume — before anything
// reaches the database.
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
// the step against it, under the id of the run that produced it — the bubble
// only takes steps from the run it adopted.
func (m *TypingIndicatorManager) refreshFromTask(e events.Event, step progressStep) {
	sessionID, ok := m.sessionFor(e)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), progressWriteTimeout)
	defer cancel()
	m.recordStep(ctx, sessionID, taskIDFromEvent(e), step)
}

// sessionFor finds the chat session behind a task lifecycle event. Most of
// these events do not carry one — task:progress and task:message never do, and
// the sweeper's task:failed does not either — so it comes off the task row, via
// the cache. The cache is what keeps a run's dozens of tool messages down to one
// read, and it remembers the tasks that have no chat session at all so an issue
// run never asks twice.
//
// Everything here runs on the goroutine that published the event: the daemon's
// own HTTP handler, or a sweeper tick. Hence the short deadline.
func (m *TypingIndicatorManager) sessionFor(e events.Event) (pgtype.UUID, bool) {
	if sessionID, ok := sessionIDFromEvent(e); ok {
		return sessionID, true
	}
	if m.tasks == nil {
		return pgtype.UUID{}, false
	}
	taskID := taskIDFromEvent(e)
	if taskID == "" {
		return pgtype.UUID{}, false
	}
	if cached, hit := m.taskSessions.get(taskID); hit {
		return cached, cached.Valid
	}
	id, err := util.ParseUUID(taskID)
	if err != nil || !id.Valid {
		return pgtype.UUID{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskLookupTimeout)
	task, err := m.tasks.GetAgentTask(ctx, id)
	cancel()
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// A row that is not there has no chat session and never will — a run
		// cancelled and deleted while its transcript was still flushing.
		// Remembering that is what stops its remaining messages from putting a
		// read behind every one.
		m.taskSessions.put(taskID, pgtype.UUID{})
		return pgtype.UUID{}, false
	case err != nil:
		// A failure is not an answer, but re-asking is worse than waiting: the
		// rest of this batch would each put another round trip on the daemon's
		// request, on a database that has just shown it is in no state to serve
		// them. Remembered for seconds, not minutes.
		m.taskSessions.putFailure(taskID)
		return pgtype.UUID{}, false
	}
	m.taskSessions.put(taskID, task.ChatSessionID)
	return task.ChatSessionID, task.ChatSessionID.Valid
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
// payload. ChatDonePayload matters most: broadcastChatDone sets no TaskID on
// the envelope, and on the in-process bus the payload stays typed — miss it
// and every answer takes the HEAD round unconditionally, which is the wrong
// bubble whenever a guard has already closed the running round's.
func taskIDFromEvent(e events.Event) string {
	if e.TaskID != "" {
		return e.TaskID
	}
	switch p := e.Payload.(type) {
	case protocol.ChatDonePayload:
		return p.TaskID
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
// stops accepting frames for a stream past streamMaxAge, so a bubble that
// outlives the window — a long run, or a round stuck in the queue behind one —
// would otherwise become a spinner we can no longer touch. The guard closes
// exactly the bubble it was armed for, by stream id: with several bubbles open
// in one session, a timer that took the head could seal a newer round's bubble
// with an older round's promise.
//
// This is the one closer that does not end the round. Its copy says the reply
// is coming separately, and the run is still going — so the handle it consumes
// leaves a note behind, and whatever the run does next is said against that.
func (m *TypingIndicatorManager) armGuard(sessionID pgtype.UUID, h streamHandle) {
	if m.guardAfter <= 0 {
		return
	}
	t := time.AfterFunc(m.guardAfter, func() {
		ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
		defer cancel()
		m.closeBubble(ctx, sessionID,
			func(s *streamStore) (streamHandle, bool) { return s.takeStream(sessionID, h.StreamID, roundContinues) },
			func(c copyPack) string { return c.StreamStillWorking }, "window expiring")
	})
	m.streams.arm(sessionID, h.StreamID, t)
}

// closeBubble seals one bubble with the copy pick chooses, if take finds one
// to seal, and reports whether it did. take names WHICH bubble — the failed
// task's, the settled flush's (tail), the guard's own (by stream id) — and
// consuming the handle inside the store makes this idempotent: two closers
// racing produce one closing frame. The ending each take carries is what the
// caller's words amount to for the round; every closer ends it except the
// guard, whose copy promises a separate reply while the run goes on.
//
// A closing frame that cannot go out falls back to a plain message, the same
// way the answer does in outbound.go. The words matter more here than there:
// StreamFailed is the only "that run did not go through" WeCom ever produces —
// the replier speaks for needs_binding, offline, archived and issue_created,
// and for nothing else — so a frame lost to a reconnect window used to leave
// the user with a spinner and no explanation that would ever arrive. The
// addressing comes off the handle, captured at ingest, because by now the
// binding row may point at a different chat.
//
// A handle the server has already disowned skips straight to that fallback.
// There is no bubble left to seal, and the frame would cost a round trip to be
// told so a second time.
func (m *TypingIndicatorManager) closeBubble(ctx context.Context, sessionID pgtype.UUID, take func(*streamStore) (streamHandle, bool), pick func(copyPack) string, why string) bool {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return false
	}
	h, ok := take(m.streams)
	if !ok {
		return false
	}
	text := pick(copyFor(h.Locale))
	if h.Unusable {
		m.log.DebugContext(ctx, "wecom typing: bubble already disowned, saying it as a new message",
			"chat_session_id", util.UUIDToString(sessionID), "reason", why)
	} else {
		err := m.senders.stream(ctx, h, text, true)
		if err == nil {
			return true
		}
		m.log.WarnContext(ctx, "wecom typing: closing frame failed, saying it as a new message",
			"chat_session_id", util.UUIDToString(sessionID),
			"reason", why, "unusable", streamUnusable(err), "error", err)
	}
	if sendErr := m.senders.send(h.InstallationID, pendingSend{
		ChatID:   h.ChatID,
		ChatType: h.ChatType,
		Content:  text,
	}); sendErr != nil {
		m.log.WarnContext(ctx, "wecom typing: the fallback message was unsendable too",
			"chat_session_id", util.UUIDToString(sessionID), "reason", why, "error", sendErr)
	}
	return true
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
