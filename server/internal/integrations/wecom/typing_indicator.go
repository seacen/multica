package wecom

// typing_indicator.go — WeCom's answer to "the bot heard you".
//
// Slack stamps 👀 on the user's message, Feishu a typing badge. WeCom has
// neither: the smart-bot protocol publishes no reaction, no read receipt and
// no typing signal at all. What it does have is the streaming message, so the
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
//
// Between those two the bubble is not left blank. The run's own transcript —
// task:message, one event per tool call — is played into it as a scrolling list
// of steps, refreshed in place at most every 1.5s. What may be shown, and to
// whom, is progress_render.go's subject; this file is the wiring and the two
// bus subscriptions that carry it.

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

// The ways a streaming reply ends in something other than an answer live in
// the copy pack as copyPack.Stream* (strings.go), and each closer names the
// one it wants rather than a string: the bubble's language is a property of
// the reader, and by the time a closer runs the only thing that still knows
// who that is, is the handle the round was opened with.

// streamCloseTimeout bounds a closing frame written from a timer or a bus
// subscriber, neither of which has a caller's context to inherit.
const streamCloseTimeout = 10 * time.Second

// taskLookup resolves a task id to the chat session it belongs to.
// task:failed's sweeper publisher carries no session, so it has to come off
// the task row. *db.Queries satisfies it.
type taskLookup interface {
	GetAgentTask(ctx context.Context, id pgtype.UUID) (db.AgentTaskQueue, error)
}

// identityLookup resolves the WeCom sender on an inbound message to the
// Multica user behind them. It answers the only question the step list's
// audience gate asks — is this person the bot's principal — and *db.Queries
// satisfies it with the row the identity resolver already reads.
type identityLookup interface {
	GetChannelUserBindingByUserID(ctx context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error)
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
	senders  *sendersRegistry
	streams  *streamStore
	tasks    taskLookup
	bindings chatBindingLookup
	// languages resolves a destination to the language its bubble is closed
	// in (language.go). nil closes every bubble in the deployment's.
	languages languageLookup
	// identities resolves the asker to a Multica user, which is what decides
	// whether their bubble may show the run's steps at all. nil shows none.
	identities identityLookup
	log        *slog.Logger

	// taskSessions remembers which chat and which round a task belongs to, so
	// one run's dozens of transcript messages cost one database read rather
	// than one each (task_sessions.go).
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

	// Tasks resolves a task id to its chat session, for the task:failed the
	// sweepers publish without one. Nil limits failure handling to the events
	// that carry a session.
	Tasks taskLookup

	// Bindings finds the chat behind a session when nothing else can — a run
	// that failed after this process was restarted, or after a bubble that was
	// never painted. Nil keeps the failure notice to the rounds this process
	// has a handle or a note for.
	Bindings chatBindingLookup

	// Languages resolves the chat a bubble was opened in to the language it is
	// closed in: a 1:1 reads the asker's own Multica profile, a room the
	// deployment's. Nil puts every bubble on the deployment's.
	Languages languageLookup

	// Identities resolves the WeCom sender to a Multica user, which is how the
	// bubble decides whether the person reading it is the one who owns the
	// run. Nil shows no steps to anyone — the bubble still opens and the
	// answer still closes it, there is just nothing in between.
	Identities identityLookup

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
		bindings:     cfg.Bindings,
		languages:    cfg.Languages,
		identities:   cfg.Identities,
		log:          logger,
		taskSessions: newTaskSessionCache(),
		guardAfter:   guard,
	}
}

// TypingIndicatorWiring reports which of the five dependencies a manager holds.
//
// Every one of them is optional and every one of them narrows the manager
// silently when it is missing: nothing panics, nothing logs, Register still
// subscribes, and the events still arrive — the closing frame just never gets
// written, which the user sees as a bubble that spins until the guard replaces
// it with a promise nobody keeps. That makes "is it wired" unfalsifiable from
// the outside, and a boot path that drops one looks exactly like a healthy
// one. This is the inspection point that makes it falsifiable.
//
// Languages is deliberately not among them: a manager without it still closes
// every bubble, in the deployment's language rather than the reader's.
type TypingIndicatorWiring struct {
	// Senders is the live WebSocket registry. Without it no closing frame and
	// no plain-message fallback can be written at all.
	Senders bool
	// Streams is the round store shared with the outbound subscriber. Without
	// it every handler returns on its first line, so no bubble is ever closed
	// by any ending.
	Streams bool
	// Tasks resolves a task id to its chat session. Without it the sweepers'
	// task:failed — which names a task and no session — resolves to nothing
	// and the bubble behind a swept run is never closed.
	Tasks bool
	// Bindings finds the chat behind a session when no round is on file.
	// Without it a run that fails after its bubble is gone (guard closed it at
	// five minutes, or the process restarted mid-run) tells the user nothing,
	// and the guard's "I'll reply separately" is never answered.
	Bindings bool
	// Identities says whether the asker can be recognised as the bot's
	// principal. Without it every bubble falls to the closed tier, which is the
	// safe side to be wrong on and also the side where the whole in-flight step
	// list silently does not exist: the bubble opens, spins and closes.
	Identities bool
}

// Wiring reports the dependencies this manager was built with. For boot-wiring
// guards; it copies five booleans and hands out no references.
func (m *TypingIndicatorManager) Wiring() TypingIndicatorWiring {
	return TypingIndicatorWiring{
		Senders:    m.senders != nil,
		Streams:    m.streams != nil,
		Tasks:      m.tasks != nil,
		Bindings:   m.bindings != nil,
		Identities: m.identities != nil,
	}
}

// OnIngested paints a "working on it" bubble for the run this message belongs
// to and records what it takes to come back and fill it in. Which run that is
// comes from batch — the engine debouncer's own verdict, decided under the
// lock that arms the window — so the first message of a run paints a bubble
// and the rest join it, and a message the debouncer gave a run of its own gets
// a bubble of its own immediately, because a wait with nothing on screen reads
// as a message that was lost. The bubble carries no words while it waits: the
// think tag renders as the client's own animated dots, which is the receipt,
// and words would need a language before there is anything to say.
//
// The Router calls this on a detached goroutine with its own deadline, so
// nothing here needs to be quick for the ACK's sake — but everything here is
// best-effort: a bubble that fails to open costs the user a few seconds of
// uncertainty, and the answer still arrives as a plain message.
func (m *TypingIndicatorManager) OnIngested(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage, sessionID pgtype.UUID, batch engine.RunBatchID) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid || batch == 0 {
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

	chatType := aibotChatTypeFromChannel(msg.Source.ChatType)
	h := streamHandle{
		ReqID:          wm.ReqID,
		StreamID:       newStreamID(),
		InstallationID: inst.ID,
		ChatID:         chatID,
		ChatType:       chatType,
		// Resolved here, while the asker is still in hand. Every closer runs
		// later, from an event that names a task and nobody else — and one of
		// them runs on a timer, minutes after this goroutine is gone.
		Locale: localeFor(ctx, m.languages, inst.ID, chatType, msg.Source.SenderID),
		// Same reason, and settled for THIS round only. Every later refresh
		// reads it back off the handle rather than asking again, and the next
		// round asks from scratch (levelFor).
		Level: m.levelFor(ctx, inst, msg),
	}
	if m.streams.open(sessionID, batch, h) != roundOpened {
		// roundJoined — the batcher folded this message into a run whose
		// bubble is already on screen, and that bubble is this message's
		// receipt too. roundFinished — this goroutine outlived the run it was
		// painting for, and a bubble now would be one nothing ever closes.
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
	// this req_id got to the socket first, and that frame carried a stream id
	// of its own, which is exactly how a bubble is created. So the spinner is
	// on the user's screen. Dropping the handle there arms no guard and leaves
	// nothing that could ever close it.
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
			m.streams.drop(sessionID, batch)
			m.log.WarnContext(ctx, "wecom typing: opening frame refused",
				"chat_session_id", util.UUIDToString(sessionID), "error", err)
			return
		}
	}
	m.armGuard(sessionID, batch)
}

// levelFor decides how much of the run a new bubble may show, while the facts
// it needs are still in hand: who asked, and where.
//
// One condition, stated two ways. The chat has to be a one-to-one, and the
// person on the other end of it has to be the principal — on this branch, the
// person who installed the bot. A group fails the first: the WeCom session key
// for a room is the room, so every member reads the same bubble, and the person
// who asked is one of several. A colleague's own chat fails the second:
// private, but not to the principal.
//
// Everything unknown resolves to the closed tier. No lookup configured, no
// sender id, a binding that is not there, a database that did not answer —
// none of those prove the reader is the principal, and this is the side to be
// wrong on. The cost of being wrong the other way is a file path in a chat
// that cannot unsend it.
//
// ASKED AGAIN FOR EVERY ROUND, never once per session. Every input can change
// between two questions in the same chat: a binding is revoked, or re-pointed
// at a different person, and re-installing the bot moves InstallerUserID. An
// answer cached for the session would keep showing one person's file paths and
// search terms to whoever inherited the chat, for as long as the session
// lasted.
//
// The cost is one indexed read per ingested message, beside the locale's,
// which OnIngested already pays on the same row for the same reason. A message
// the batcher folds into a run whose bubble is already open pays it for
// nothing — this runs before open() says which of the two happened — and that
// is the price of deciding while the asker is still in hand, against a feature
// that writes a frame every 1.5s once it starts.
func (m *TypingIndicatorManager) levelFor(ctx context.Context, inst engine.ResolvedInstallation, msg channel.InboundMessage) progressLevel {
	// Positively a one-to-one, not merely "not a group". aibotChatTypeFromChannel
	// answers single for anything it does not recognise, which is the right
	// default for ADDRESSING a frame — a wire field has to be one or the other
	// — and the wrong one for deciding who may read a run's step detail. An
	// unrecognised chat type (a kind WeCom adds later, a field that did not
	// survive a decode) would be treated as private, and the cost of being
	// wrong here is a file path or a search term in a room.
	if msg.Source.ChatType != channel.ChatTypeP2P {
		return progressLevelNone
	}
	if !inst.InstallerUserID.Valid || m.identities == nil {
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
	if binding.MulticaUserID != inst.InstallerUserID {
		return progressLevelNone
	}
	return progressLevelDetail
}

// OnRunStarted files the task the debounced flush created for this run. It is
// the binding every later ending is matched on: the answer, the failure and
// the cancellation all name a task, and this is what turns that name into "the
// bubble this question opened" without reading anything off arrival order.
//
// It can arrive before OnIngested has painted the bubble — the Router detaches
// the ingest goroutine and the flush runs on the batcher's timer — so the
// store files the run either way and the bubble attaches to it when it lands.
func (m *TypingIndicatorManager) OnRunStarted(_ context.Context, sessionID pgtype.UUID, batch engine.RunBatchID, taskID pgtype.UUID) {
	if m.streams == nil || !sessionID.Valid || !taskID.Valid {
		return
	}
	m.streams.bind(sessionID, batch, util.UUIDToString(taskID))
}

// OnSettled closes the bubble of a round that never became a run — agent
// offline or archived, or an enqueue that failed. This is the only chance to
// stop that spinner: with no task there is no task lifecycle event, so neither
// the chat-done subscriber nor the failure subscriber will ever fire. The copy
// is deliberately thin because the replier's own notice follows as a separate
// message with the reason.
//
// batch names which bubble: the flush that settled reports the run it was
// answering, so a session with several rounds open closes the right one
// instead of whichever happens to be newest.
func (m *TypingIndicatorManager) OnSettled(ctx context.Context, sessionID pgtype.UUID, batch engine.RunBatchID) {
	m.closeBubble(ctx, sessionID,
		func(s *streamStore) (streamHandle, bool) { return s.takeBatch(sessionID, batch, roundOver) },
		func(c copyPack) string { return c.StreamNotStarted }, "settled")
}

// Register subscribes the manager to the two ways a run ends without an
// answer, and — when a task lookup was configured — to the two events that say
// where a run has got to.
//
// The endings have to be here or the bubble outlives its run: a failure and a
// cancellation each publish nothing the outbound subscriber reads, so nothing
// else would ever seal that stream.
//
// The other two are the middle of the run. task:progress fires exactly twice
// and both lines are the daemon's own ("Launching claude", "Finishing task"),
// so on its own it leaves the whole middle blank; task:message is the
// transcript — one event per tool call, batched about twice a second — and it
// is the only signal fine-grained enough to answer what the agent is doing
// right now. Both feed the same list. They are gated on m.tasks because
// neither event carries a chat session, so without the task row there is no
// way to tell which bubble — if any — a step belongs in.
//
// EventChatDone is deliberately NOT subscribed here: the answer belongs in the
// bubble, and only the outbound subscriber holds the answer. Registering for
// it here would close the bubble first and leave the reply to arrive
// underneath it.
func (m *TypingIndicatorManager) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventTaskFailed, m.handleTaskFailed)
	bus.Subscribe(protocol.EventTaskCancelled, m.handleTaskCancelled)
	if m.tasks != nil {
		bus.Subscribe(protocol.EventTaskProgress, m.handleTaskProgress)
		bus.Subscribe(protocol.EventTaskMessage, m.handleTaskMessage)
	}
}

// progressWriteTimeout bounds one refresh. It runs on the goroutine that
// published the transcript event — the daemon's own HTTP request — so this is
// a slice of somebody else's budget, not ours to spend freely. Well under the
// socket's own ack wait, because a step nobody could write in this long is a
// step the next one replaces anyway.
const progressWriteTimeout = 3 * time.Second

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
// that a deployment with no WeCom bubble open pays nothing at all for this
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

// refreshFromTask resolves the bubble behind a transcript event and records the
// step in it, under the id of the run that produced it.
func (m *TypingIndicatorManager) refreshFromTask(e events.Event, step progressStep) {
	sessionID, taskID, roundID, ok := m.progressTarget(e)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), progressWriteTimeout)
	defer cancel()
	m.recordStep(ctx, sessionID, taskID, roundID, step)
}

// progressTarget resolves a transcript event to the chat session behind the
// run and the round the run belongs to.
//
// Neither task:progress nor task:message carries a chat session — both are
// published against a task id and nothing else — so the row is the only
// answer, and one run posts dozens of them. Hence the cache, which also
// remembers the tasks that have NO chat session so an issue or autopilot run
// asks once and then costs nothing. The round comes off the same row: see
// task_sessions.go for why that column is what an auto-retry clone is resolved
// through.
func (m *TypingIndicatorManager) progressTarget(e events.Event) (pgtype.UUID, string, string, bool) {
	if m.tasks == nil {
		return pgtype.UUID{}, "", "", false
	}
	taskID := taskIDFromEvent(e)
	if taskID == "" {
		return pgtype.UUID{}, "", "", false
	}
	if cached, hit := m.taskSessions.get(taskID); hit {
		return cached.session, taskID, cached.round, cached.session.Valid
	}
	id, err := util.ParseUUID(taskID)
	if err != nil || !id.Valid {
		return pgtype.UUID{}, "", "", false
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
		m.taskSessions.put(taskID, taskRound{})
		return pgtype.UUID{}, "", "", false
	case err != nil:
		// A failure is not an answer, but re-asking is worse than waiting: the
		// rest of this batch would each put another round trip on the daemon's
		// request, on a database that has just shown it is in no state to serve
		// them. Remembered for seconds, not minutes.
		m.taskSessions.putFailure(taskID)
		return pgtype.UUID{}, "", "", false
	}
	round := taskRound{session: task.ChatSessionID}
	if task.ChatInputTaskID.Valid {
		round.round = util.UUIDToString(task.ChatInputTaskID)
	}
	m.taskSessions.put(taskID, round)
	return round.session, taskID, round.round, round.session.Valid
}

// recordStep folds one step into the bubble of the round it belongs to and
// writes the result.
//
// taskID is the run that published the step; roundID is the round that run
// belongs to, which differs only for an auto-retry clone — FailTask gives the
// clone a fresh id and the round is still filed under the id the flush bound,
// which the clone inherited as chat_input_task_id. Both are tried, in that
// order, for the same reason roundTaker tries both when an ending arrives.
//
// Three properties are deliberate. Content is a full replacement, not a delta,
// so a frame carries the whole list and no caller has to know what the bubble
// says now. A refresh yields when the previous frame has not been acked (the
// backpressure the official SDK calls replyStreamNonBlocking) because progress
// is worth less than the answer behind it, and it yields again to the feed's
// own minimum interval, which folds a burst of tool calls into one frame. And
// a refresh that merely fails leaves the spinner exactly as it was, for the
// answer or the guard to finish properly.
//
// The one thing a refresh does end is a bubble the server has disowned; see
// markUnusable for why that stop condition is not optional.
func (m *TypingIndicatorManager) recordStep(ctx context.Context, sessionID pgtype.UUID, taskID, roundID string, step progressStep) {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return
	}
	h, feed, ok := m.streams.feedFor(sessionID, taskID)
	if !ok && roundID != "" && roundID != taskID {
		h, feed, ok = m.streams.feedFor(sessionID, roundID)
	}
	if !ok {
		// No bubble to write into: a session that is not WeCom's, a round
		// whose bubble is already over, or a run whose bubble belongs to
		// somebody else. A step has no second address and asks for none —
		// feedFor says why.
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
			// A refresh that raced into the same refusal got here first and
			// has already said it.
			return
		}
		m.log.WarnContext(ctx, "wecom typing: bubble disowned by the server, answering as a new message",
			"chat_session_id", util.UUIDToString(sessionID),
			"installation_id", util.UUIDToString(h.InstallationID), "error", err)
		// Nothing can close this bubble now — the stream takes no further
		// frame — so the spinner turns for good and the user is owed a word
		// about where the rest of the round is going to appear instead. Said
		// once: the mark is what makes this the only refresh that gets here.
		if sendErr := m.senders.sendTextCtx(ctx, h.InstallationID, h.ChatID, h.ChatType,
			copyFor(h.Locale).StreamStuck); sendErr != nil {
			m.log.WarnContext(ctx, "wecom typing: could not say the bubble is stuck",
				"chat_session_id", util.UUIDToString(sessionID), "error", sendErr)
		}
	default:
		m.log.DebugContext(ctx, "wecom typing: progress refresh failed",
			"chat_session_id", util.UUIDToString(sessionID), "error", err)
	}
}

// progressSummary reads the human-readable line off a task:progress payload,
// typed in-process or in its map form after a serialization round trip.
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

// handleTaskFailed says a run died, in the bubble if there still is one and as
// a plain message if there is not.
//
// task:failed has two publishers and they do not agree on the payload.
// broadcastTaskEvent, which FailTask uses, carries chat_session_id.
// HandleFailedTasks — every sweeper, recover-orphans, and the daemon heartbeat
// timeout — carries task_id, agent_id, issue_id, status and failure_reason, and
// no session anywhere. That second publisher is the whole crashed-daemon path,
// so without the task-id fallback the bubble spins on until the guard replaces
// it with "still working, I'll reply separately" — a promise about a run that
// has been dead for five minutes. The fallback stays on this side of the
// boundary: the adapter carries its own routing rather than widening a payload
// three other consumers read.
//
// The bubble is not the whole of it. A handle is consumed by whichever ending
// gets there first, and the guard is allowed to be that one at the five-minute
// mark while the run carries on — so every run longer than five minutes that
// then failed finds no handle here. This notice is the only "that run did not
// go through" WeCom ever produces: the replier speaks for needs_binding,
// offline, archived and issue_created and for nothing else. So the handle is
// one address among three, and sayTheRunFailed works down the rest.
func (m *TypingIndicatorManager) handleTaskFailed(e events.Event) {
	if m.streams == nil {
		return
	}
	// An attempt the platform is already retrying is not an ending. FailTask
	// publishes task:failed for it anyway — the web card has to clear — and
	// flags it retry_pending so consumers stay quiet; taskFailedFields even
	// withholds the error text, and dingtalk's outbound already honours it.
	// Closing the bubble here would tell the user "这次没跑通" about an attempt
	// whose replacement is already queued, and the retry's answer would then
	// land underneath a bubble that had declared failure. The round stays open
	// for the attempt that reports the real outcome; the retry clone's own
	// events find it through the batch owner it inherited (roundTaker).
	if retryPending(e) {
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
		func(*streamStore) (streamHandle, bool) {
			return m.rounds().take(ctx, sessionID, taskID, roundOver)
		},
		func(c copyPack) string { return c.StreamFailed }, "task failed") {
		return
	}
	m.sayTheRunFailed(ctx, sessionID, taskID)
}

// handleTaskCancelled seals the bubble of a run the user stopped.
//
// Cancellation is a terminal state that publishes no chat:done and no
// task:failed, so without this the bubble spins for the full five minutes and
// the guard then promises a separate reply — about a run the user cancelled
// themselves, that will never come. Every cancel path lands here: CancelTask
// for a running or queued task, CancelQueuedChatTasks for the follow-ups
// behind it, and the agent-level and issue-level bulk cancels, which all
// broadcast task:cancelled per row. A session with several rounds open
// therefore gets one closing frame per cancelled run, each on its own bubble,
// because the round is matched by the task id the flush bound to it.
//
// Unlike a failure this does NOT go looking in the binding row when no round
// is on file. StreamFailed is the only "that run did not go through" WeCom
// ever produces, which is why a failure is worth chasing an address for; a
// cancellation was performed by the user, and chasing it would turn one
// "cancel all tasks" click into a message in every chat that agent serves —
// including sessions where WeCom never showed a bubble at all.
func (m *TypingIndicatorManager) handleTaskCancelled(e events.Event) {
	if m.streams == nil {
		return
	}
	if m.streams.depth() == 0 && m.streams.remembered() == 0 {
		return
	}
	sessionID, ok := m.sessionFor(e)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
	defer cancel()
	taskID := taskIDFromEvent(e)
	if m.closeBubble(ctx, sessionID,
		func(*streamStore) (streamHandle, bool) {
			return m.rounds().take(ctx, sessionID, taskID, roundOver)
		},
		func(c copyPack) string { return c.StreamCancelled }, "task cancelled") {
		return
	}
	// No bubble left. If the guard already closed one for this run it promised
	// a separate reply, and that promise is now void — say so, in the chat the
	// promise was made in. The task id goes with it: the promise to keep is
	// the cancelled round's own, and a session can hold several.
	m.settleOwedEnding(ctx, sessionID, taskID, func(c copyPack) string { return c.StreamCancelled })
}

// rounds builds the matcher that turns a task id on an event into the round it
// belongs to.
func (m *TypingIndicatorManager) rounds() roundTaker {
	return roundTaker{streams: m.streams, tasks: m.tasks, log: m.log}
}

// retryPending reports whether FailTask has already created a retry child for
// this attempt. taskFailedFields sets it on every task:failed payload.
func retryPending(e events.Event) bool {
	p, ok := e.Payload.(map[string]any)
	if !ok {
		return false
	}
	pending, _ := p["retry_pending"].(bool)
	return pending
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
// attempt, a sweeper that ran late — must not contradict it. Which is why the
// claim is made in this run's name: a promise belonging to another round is
// not this failure's to spend, and spending it would both misreport that round
// and leave this one's repeat with a promise still to take.
func (m *TypingIndicatorManager) sayTheRunFailed(ctx context.Context, sessionID pgtype.UUID, taskID string) {
	if m.senders == nil {
		return
	}
	addr, verdict := m.rounds().claim(ctx, sessionID, taskID)
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
	m.sayAsPlainMessage(ctx, sessionID, addr, m.copyForAddress(ctx, addr).StreamFailed)
}

// settleOwedEnding keeps the guard's promise to ONE round and nothing more. It
// speaks only for a round the guard closed early — the one case where words are
// owed and no bubble is left to put them in — and stays silent when that round
// has no outstanding promise, which is every round that already ended properly.
//
// taskID is which round. A session can hold a promise per guard-closed round,
// and the copy differs per outcome, so claiming without a name would announce
// this run's outcome against somebody else's promise and leave this run's own
// asker with the silence the guard promised to break.
func (m *TypingIndicatorManager) settleOwedEnding(ctx context.Context, sessionID pgtype.UUID, taskID string, pick func(copyPack) string) {
	if m.senders == nil {
		return
	}
	addr, verdict := m.rounds().claim(ctx, sessionID, taskID)
	if verdict != roundOwesAnEnding {
		return
	}
	m.sayAsPlainMessage(ctx, sessionID, addr, pick(m.copyForAddress(ctx, addr)))
}

// copyForAddress picks the pack for a round whose handle is gone: the words
// are going to a chat rather than to a reader anybody still holds, and in a
// 1:1 that chatid IS the reader's userid, which is what localeFor wants. A
// room ignores it and reads the deployment's language (language.go).
func (m *TypingIndicatorManager) copyForAddress(ctx context.Context, addr roundAddress) copyPack {
	return copyFor(localeFor(ctx, m.languages, addr.InstallationID, addr.ChatType, addr.ChatID))
}

func (m *TypingIndicatorManager) sayAsPlainMessage(ctx context.Context, sessionID pgtype.UUID, addr roundAddress, text string) {
	if err := m.senders.sendTextCtx(ctx, addr.InstallationID, addr.ChatID, addr.ChatType, text); err != nil {
		m.log.WarnContext(ctx, "wecom typing: could not deliver a run's ending",
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
	return roundAddress{
		InstallationID: binding.InstallationID,
		ChatID:         binding.ChannelChatID,
		ChatType:       aibotChatTypeFromChannel(channel.ChatType(binding.ChatType)),
	}, true
}

// sessionFor finds the chat session behind a task lifecycle event. The
// sweeper's task:failed does not carry one, so it comes off the task row.
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
	id, err := util.ParseUUID(taskID)
	if err != nil || !id.Valid {
		return pgtype.UUID{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskLookupTimeout)
	defer cancel()
	task, err := m.tasks.GetAgentTask(ctx, id)
	if err != nil {
		return pgtype.UUID{}, false
	}
	return task.ChatSessionID, task.ChatSessionID.Valid
}

// taskLookupTimeout is the whole budget this subscriber may spend on somebody
// else's goroutine. The bus is synchronous: a task:failed subscriber runs on
// the goroutine that published the event, and a sweeper tick is not ours to
// hold while a loaded pool answers.
const taskLookupTimeout = 800 * time.Millisecond

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
// exactly the round it was armed for, by batch: with several bubbles open in
// one session, a timer that took the head could seal a newer round's bubble
// with an older round's promise.
//
// This is the one closer that does not end the round. Its copy says the reply
// is coming separately, and the run is still going — so the handle it consumes
// leaves a note behind, and whatever the run does next is said against that.
func (m *TypingIndicatorManager) armGuard(sessionID pgtype.UUID, batch engine.RunBatchID) {
	if m.guardAfter <= 0 {
		return
	}
	t := time.AfterFunc(m.guardAfter, func() {
		ctx, cancel := context.WithTimeout(context.Background(), streamCloseTimeout)
		defer cancel()
		m.closeBubble(ctx, sessionID,
			func(s *streamStore) (streamHandle, bool) { return s.takeBatch(sessionID, batch, roundContinues) },
			func(c copyPack) string { return c.StreamStillWorking }, "window expiring")
	})
	m.streams.arm(sessionID, batch, t)
}

// closeBubble seals one bubble with text, if take finds one to seal, and
// reports whether it did. take names WHICH bubble — the ended run's, by the
// task id the flush bound; the settled or guarded run's, by batch — and consuming the
// handle inside the store makes this idempotent: two closers racing produce one
// closing frame. The ending each take carries is what the caller's words amount
// to for the round; every closer ends it except the guard, whose copy promises
// a separate reply while the run goes on.
//
// A closing frame that cannot go out falls back to a plain message, the same
// way the answer does in outbound.go. The words matter more here than there:
// StreamFailed is the only "that run did not go through" WeCom ever
// produces, so a frame lost to a reconnect window would otherwise leave the
// user with a spinner and no explanation that would ever arrive. The addressing
// comes off the handle, captured at ingest, because by now the binding row may
// point at a different chat.
func (m *TypingIndicatorManager) closeBubble(ctx context.Context, sessionID pgtype.UUID, take func(*streamStore) (streamHandle, bool), pick func(copyPack) string, why string) bool {
	if m.senders == nil || m.streams == nil || !sessionID.Valid {
		return false
	}
	h, ok := take(m.streams)
	if !ok {
		return false
	}
	// The words come from the handle's own language, not the deployment's:
	// the round knows who asked, and this is the last point at which anything
	// does.
	text := pick(copyFor(h.Locale))
	if h.Unusable {
		// A refresh already learned this stream takes no frame, and already
		// told the user the rest of the round would arrive as new messages.
		// This is one of those. Trying the frame first would buy one more
		// refusal against the bot's shared rate limit and change nothing.
		m.log.DebugContext(ctx, "wecom typing: bubble already disowned, saying it as a new message",
			"chat_session_id", util.UUIDToString(sessionID), "reason", why)
		m.sayAsPlainMessage(ctx, sessionID, h.address(), text)
		return true
	}
	err := m.senders.stream(ctx, h, text, true)
	if err == nil {
		return true
	}
	m.log.WarnContext(ctx, "wecom typing: closing frame failed, saying it as a new message",
		"chat_session_id", util.UUIDToString(sessionID),
		"reason", why, "unusable", streamUnusable(err), "error", err)
	if sendErr := m.senders.sendTextCtx(ctx, h.InstallationID, h.ChatID, h.ChatType, text); sendErr != nil {
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
