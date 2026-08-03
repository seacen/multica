package wecom

// stream_store.go — the handle that lets an answer land in the bubble the
// question opened.
//
// WeCom's aibot API has no typing indicator, no reaction, no read receipt, and
// no way to edit a message after the fact. The one affordance it does have is
// the streaming message: an aibot_respond_msg frame with finish=false paints a
// bubble the client renders as "working", and a later frame carrying the SAME
// stream.id replaces that bubble's body in place — finish=true seals it and
// nothing can touch it again
// (https://developer.work.weixin.qq.com/document/path/101463).
//
// The catch is req_id. Every frame of one stream has to echo the req_id of the
// aibot_msg_callback that started the turn, and that value is only ever seen
// by the WebSocket read loop. The answer shows up minutes later on an event
// bus subscriber holding nothing but a chat_session_id. This store is the seam
// between the two: session in, {req_id, stream id, addressing} out.
//
// In-memory is the right storage. One bot is one long connection, and the
// Supervisor's WS lease already guarantees at most one replica holds it, so a
// handle is only ever useful in the process that created it. A restart loses
// the handles, which is exactly right: the socket the req_ids belonged to is
// gone too, and the answers fall back to plain messages.
//
// Replay is not this file's problem. WeChat redelivers callbacks after a
// reconnect, but a redelivered frame loses the dedup claim in
// channel_inbound_message_dedup and never reaches OutcomeIngested, so it never
// reaches the typing indicator either. What this file does bound is the
// protocol's own window: a stream past streamMaxAge is refused by the server,
// and a handle past that age is worse than no handle at all — it would swallow
// the answer instead of delivering it.

import (
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
)

// streamMaxAge is how long a handle is worth keeping. The long-connection doc
// gives a stream 10 minutes before the server ends it; the one production
// implementation we can read — Tencent's own OpenClaw plugin — treats errcode
// 846608 as a 6-minute ceiling. The two do not agree, so we take the shorter
// number: being early costs a fallback message, being late costs the answer.
const streamMaxAge = 6 * time.Minute

// streamGuardAfter is when we close a bubble ourselves rather than let it run
// into streamMaxAge. A minute of headroom covers a slow frame and leaves the
// user with a sentence instead of a spinner the server will no longer let us
// replace.
const streamGuardAfter = 5 * time.Minute

// roundMemory is how long a session keeps the note the handle left behind. It
// has to outlast the bubble by a lot: the guard closes at five minutes and the
// run it made a promise about carries on for as long as the agent needs, so the
// window between the promise and the failure it accounts for is the length of a
// long run, not the length of a stream. An hour covers those and still bounds
// the map to the sessions answered in the last hour.
const roundMemory = time.Hour

// sameRoundWindow is how close two messages have to be to be one agent run.
//
// It is the engine's debounce window rather than a number of its own, because
// it is the same question asked twice. The engine's batcher re-arms a
// per-session timer on every inbound message and fires one run when the session
// has been quiet for that long (engine/batcher.go), so messages less than a
// window apart are collected into a single run and anything later is a run of
// its own. A local constant here would be a second copy of that rule, free to
// drift from the one that decides.
//
// The gap is measured from the PREVIOUS message, not from when the bubble
// opened, for the same reason: the timer re-arms, so a message every two
// seconds is one run no matter how long the burst goes on, and the bubble's age
// says nothing about which run the next message belongs to.
const sameRoundWindow = engine.DefaultChatRunBatchWindow

// roundEnding says whether the words a closer is writing are the last this
// round will need. Only the guard's are not: "still working, I'll reply
// separately" is a promise, and whatever the run does next is still owed.
type roundEnding bool

const (
	roundOver      roundEnding = true
	roundContinues roundEnding = false
)

// roundVerdict is the store's answer to "may I speak for this round, and where".
type roundVerdict int

const (
	// roundForgotten — nothing on file. A turn from before a restart, or one
	// that never opened a bubble at all. The caller has to find the chat some
	// other way.
	roundForgotten roundVerdict = iota
	// roundOwesAnEnding — the bubble was closed early and the run went on, so
	// its real ending has never been said. Addressing follows.
	roundOwesAnEnding
	// roundToldAlready — the answer landed, or another publisher's failure got
	// here first. Nothing to add.
	roundToldAlready
)

// streamHandle is everything needed to keep writing to one open bubble. The
// addressing is captured at ingest rather than looked up later: by the time
// the answer arrives the binding row may have been re-pointed, and the frame
// has to go back to the chat that asked.
type streamHandle struct {
	// ReqID is the aibot_msg_callback's req_id. WeCom refuses a stream frame
	// carrying any other value, including a req_id from an event callback
	// (errcode 846605).
	ReqID string

	// StreamID is ours to choose. Reusing it updates the message; a new one
	// opens another.
	StreamID string

	// InstallationID finds the live socket. ChatID and ChatType address the
	// conversation for the fallback plain message a closing frame degrades to
	// when the stream cannot take it (typing_indicator.go).
	InstallationID pgtype.UUID
	ChatID         string
	ChatType       int

	// Locale is the installation's copy language, captured here so the closing
	// frame does not need a second installation read to know what to say.
	Locale Locale

	// Level is how much of the run this bubble may show. It is settled at
	// ingest, when who asked and where they asked is still known, and read
	// again on every refresh — the events that drive those refreshes carry a
	// task id and nothing about a person.
	Level progressLevel

	// Unusable says the server has disowned this stream (846608 / 846605): the
	// bubble it painted is on the user's screen and no frame will ever touch it
	// again, so what is left of the handle is the addressing. A caller that
	// gets one writes its words as a plain message instead of a frame. Set by
	// the store on the way out of take; a caller of claim always leaves it
	// false.
	Unusable bool

	CreatedAt time.Time
}

// roundAddress is where a round's words go once its bubble is gone: the
// installation whose socket carries them, the chat that asked, and the language
// to say it in. The stream ids are deliberately not here — they name a bubble
// nobody can write to any more, and carrying them would invite another attempt.
type roundAddress struct {
	InstallationID pgtype.UUID
	ChatID         string
	ChatType       int
	Locale         Locale
}

func (h streamHandle) address() roundAddress {
	return roundAddress{
		InstallationID: h.InstallationID,
		ChatID:         h.ChatID,
		ChatType:       h.ChatType,
		Locale:         h.Locale,
	}
}

// endedRound is what a session keeps once its handle is gone: where the round
// was speaking, and whether anything is still owed to it.
//
// The second field is the whole of it. A handle is taken by whichever ending
// gets there first, and the five-minute guard is allowed to be that one — it
// writes "still working, I'll reply separately" while the run carries on. The
// failure that arrives afterwards finds no handle, and without this note it
// used to return without a word: a promise, and then nothing.
type endedRound struct {
	addr roundAddress
	owed bool
	at   time.Time
}

// streamEntry pairs a handle with the timer that closes it if nothing else
// does. The timer is stored next to the handle so whoever consumes the handle
// also disarms the guard, in one lock.
type streamEntry struct {
	handle streamHandle
	guard  *time.Timer

	// feed is the bubble's running list of steps, created on the first one.
	// It lives here rather than beside the subscriber so its lifetime is the
	// bubble's: whoever takes or drops the handle also disposes of the list,
	// and nothing has to be swept separately.
	feed *progressFeed

	// taskID is the run this bubble belongs to, adopted from the first step
	// that reaches it. A chat session outlives its turns and the daemon flushes
	// a transcript in arrears, so the previous turn's tool calls can still be
	// arriving after the user has asked the next question — they resolve to the
	// same session and would be painted into the new bubble as if they were
	// what is happening now.
	taskID string

	// unusable is the server's verdict that this stream is finished with, kept
	// rather than acted on by forgetting the entry. The bubble is over either
	// way; the round is not, and the handle is the only address the round's
	// ending has.
	unusable bool

	// lastIngest is when this session last took a message — the bubble's own
	// opening for the first one, and every follow-up after that. It is what
	// separates a burst from a queue; see sameRoundWindow.
	lastIngest time.Time

	// queuedTold says the user has been told that a later message is waiting
	// behind the run this bubble belongs to. Said once per bubble: the news is
	// the same for the second message and the fifth, and a receipt per message
	// is the bot talking over itself.
	queuedTold bool
}

// followUpVerdict is the store's answer to "another message just landed on a
// session that already has a bubble — does the user need to hear about it".
type followUpVerdict int

const (
	// followUpNoBubble — nothing open. The round ended between the claim that
	// failed and this call, so there is no bubble and nothing to say about one.
	followUpNoBubble followUpVerdict = iota
	// followUpSameRound — inside the debounce window. The run trigger has not
	// fired yet, so this message joins the run the bubble already stands for
	// and the bubble is its receipt too.
	followUpSameRound
	// followUpQueued — past the window, so the run behind the bubble is under
	// way and this message starts a round that waits for it. Nobody has said so
	// yet and this caller is the one that may.
	followUpQueued
	// followUpToldAlready — as above, except the user already knows.
	followUpToldAlready
)

// streamStore maps chat_session_id to the open bubble for that session, and —
// for a while after the bubble is gone — to what the round it belonged to still
// owes.
type streamStore struct {
	mu     sync.Mutex
	byKey  map[string]streamEntry
	ended  map[string]endedRound
	maxAge time.Duration
	now    func() time.Time
}

func newStreamStore() *streamStore {
	return &streamStore{
		byKey:  make(map[string]streamEntry),
		ended:  make(map[string]endedRound),
		maxAge: streamMaxAge,
		now:    time.Now,
	}
}

// NewStreamStore is the constructor boot uses to mint the one store shared by
// the typing indicator (writer) and the chat-done subscriber (reader).
func NewStreamStore() *streamStore { return newStreamStore() }

// claim registers a handle for a session and reports whether it took. A live
// handle already on file wins: two messages inside one debounce window share a
// single agent run, so a second bubble would be one nobody ever closes.
func (s *streamStore) claim(sessionID pgtype.UUID, h streamHandle) bool {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked()
	if _, taken := s.byKey[key]; taken {
		return false
	}
	if h.CreatedAt.IsZero() {
		h.CreatedAt = s.now()
	}
	s.byKey[key] = streamEntry{handle: h, lastIngest: h.CreatedAt}
	return true
}

// followUp records another message arriving in a session whose bubble is
// already open, and says whether the user is owed a word about it.
//
// The claim that just failed proves only that a bubble exists, which covers two
// situations the user experiences completely differently. Inside the debounce
// window the message is part of the run the bubble already stands for, and the
// spinner on screen is the receipt for it as well. Past the window the run
// trigger has already fired, the first run is under way — for as long as the
// agent needs, which is what makes this visible at all — and this message
// begins a round that sits in agent_task_queue until that one finishes.
//
// So the gap decides, and the timestamp it is measured from is the previous
// message rather than the bubble's opening: the debouncer re-arms on every
// message, so a slow burst is still one run and only the gaps within it say so
// (sameRoundWindow).
//
// The latch is the other half. A queued round is queued once; three messages
// piling onto it are three copies of the same news.
func (s *streamStore) followUp(sessionID pgtype.UUID) followUpVerdict {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[key]
	if !ok {
		return followUpNoBubble
	}
	now := s.now()
	gap := now.Sub(entry.lastIngest)
	entry.lastIngest = now

	verdict := followUpQueued
	switch {
	case gap < sameRoundWindow:
		verdict = followUpSameRound
	case entry.queuedTold:
		verdict = followUpToldAlready
	default:
		entry.queuedTold = true
	}
	s.byKey[key] = entry
	return verdict
}

// arm attaches the expiry guard to a claimed handle. A run that finished
// between the claim and this call has already taken the entry, so there is
// nothing left to guard and the timer is stopped instead of leaked.
func (s *streamStore) arm(sessionID pgtype.UUID, streamID string, t *time.Timer) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	entry, ok := s.byKey[key]
	if !ok || entry.handle.StreamID != streamID {
		s.mu.Unlock()
		t.Stop()
		return
	}
	entry.guard = t
	s.byKey[key] = entry
	s.mu.Unlock()
}

// take removes a session's handle and hands it over — the ending of a round,
// whichever ending it is. A handle past maxAge is dropped and reported as
// absent: the server would refuse the frame, and a caller that believed it had
// a bubble would leave the user with nothing.
//
// A disowned handle IS handed over, with Unusable set. Its bubble cannot be
// sealed, but the round still has to end in words, and the addressing captured
// at ingest is what puts them in the right chat.
//
// ending is the caller's account of what it is about to write. Everything but
// the guard ends the round; the guard's copy promises a separate reply, and the
// note left behind is what makes that promise keepable — the failure that
// arrives ten minutes later has no handle and needs both the address and the
// knowledge that nobody has spoken yet.
func (s *streamStore) take(sessionID pgtype.UUID, ending roundEnding) (streamHandle, bool) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[key]
	if !ok {
		return streamHandle{}, false
	}
	delete(s.byKey, key)
	if entry.guard != nil {
		entry.guard.Stop()
	}
	if s.expiredLocked(entry.handle) {
		return streamHandle{}, false
	}
	s.rememberLocked(key, entry.handle.address(), ending)
	entry.handle.Unusable = entry.unusable
	return entry.handle, true
}

// remember files where a round was speaking without taking a handle for it —
// the answer that went out as a plain message because the bubble was already
// gone, and the failure notice that had to find its chat in the binding row.
// Both are endings, and both have to be on file or the next publisher of the
// same news repeats it.
func (s *streamStore) remember(sessionID pgtype.UUID, addr roundAddress, ending roundEnding) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.rememberLocked(key, addr, ending)
}

func (s *streamStore) rememberLocked(key string, addr roundAddress, ending roundEnding) {
	s.ended[key] = endedRound{addr: addr, owed: ending == roundContinues, at: s.now()}
}

// claimEnding asks whether a round with no bubble left is still owed an ending,
// and claims the right to say it. The claim is the point: task:failed has two
// publishers and a sweeper tick can repeat one, so whoever gets here first
// speaks and everyone after reads roundToldAlready.
//
// roundForgotten is not a refusal. It means this process never saw the round —
// a restart mid-run, a turn whose opening frame the server refused — so there
// is no address on file and the caller has to find the chat itself.
func (s *streamStore) claimEnding(sessionID pgtype.UUID) (roundAddress, roundVerdict) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	round, ok := s.ended[key]
	if !ok {
		return roundAddress{}, roundForgotten
	}
	if s.now().Sub(round.at) > roundMemory {
		delete(s.ended, key)
		return roundAddress{}, roundForgotten
	}
	if !round.owed {
		return round.addr, roundToldAlready
	}
	round.owed = false
	s.ended[key] = round
	return round.addr, roundOwesAnEnding
}

// remembered reports how many rounds are on file past their bubble. Diagnostics
// and the cheap rejection at the head of the failure subscriber.
func (s *streamStore) remembered() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ended)
}

// peek reads a session's handle without consuming it — the progress-refresh
// path, which expects to write to the same bubble again. An expired handle is
// evicted here too, and a disowned one is reported as absent: the question peek
// answers is "is there a bubble I can write to", and there is not.
func (s *streamStore) peek(sessionID pgtype.UUID) (streamHandle, bool) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[key]
	if !ok {
		return streamHandle{}, false
	}
	if s.expiredLocked(entry.handle) {
		delete(s.byKey, key)
		if entry.guard != nil {
			entry.guard.Stop()
		}
		return streamHandle{}, false
	}
	if entry.unusable {
		return streamHandle{}, false
	}
	return entry.handle, true
}

// feedFor returns a session's open bubble and the list of steps shown inside
// it, creating the list on first use. Like peek it leaves the handle in place,
// evicts an expired one and refuses a disowned one: a run whose window has
// closed gets no more refreshes, and its list goes with it.
//
// taskID says which run is asking. The first run to speak adopts the bubble and
// every other one is refused, which is what keeps the previous turn's trailing
// transcript out of the bubble this turn opened. An empty taskID is a caller
// that already knows the session — UpdateProgress — and is trusted rather than
// matched.
func (s *streamStore) feedFor(sessionID pgtype.UUID, taskID string) (streamHandle, *progressFeed, bool) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[key]
	if !ok {
		return streamHandle{}, nil, false
	}
	if s.expiredLocked(entry.handle) {
		delete(s.byKey, key)
		if entry.guard != nil {
			entry.guard.Stop()
		}
		return streamHandle{}, nil, false
	}
	if entry.unusable {
		return streamHandle{}, nil, false
	}
	if taskID != "" && entry.taskID != "" && entry.taskID != taskID {
		return streamHandle{}, nil, false
	}
	dirty := false
	if taskID != "" && entry.taskID == "" {
		entry.taskID = taskID
		dirty = true
	}
	if entry.feed == nil {
		entry.feed = newProgressFeed(s.now)
		dirty = true
	}
	if dirty {
		s.byKey[key] = entry
	}
	return entry.handle, entry.feed, true
}

// markUnusable records the server's verdict that a session's stream will take
// no further frame, and reports whether this call is the one that recorded it.
// Only that caller says so to the user; a second refresh that raced into the
// same refusal has nothing to add.
//
// What it deliberately does not do is forget the entry. The bubble is over —
// peek and feedFor refuse it from here, so no later refresh buys another
// refusal — but the round is not, and the entry is what the round's ending is
// addressed with. Forgetting it, which is what this path used to do, also
// stopped the guard, and left the run's failure with nowhere to be said: a dead
// spinner, and a promise of a new message that never arrived.
func (s *streamStore) markUnusable(sessionID pgtype.UUID) bool {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.byKey[key]
	if !ok || entry.unusable {
		return false
	}
	entry.unusable = true
	s.byKey[key] = entry
	return true
}

// drop forgets a session's handle without sending anything — used when the
// opening frame was refused and the bubble the handle describes never existed.
// A bubble the user can see is never dropped: it is marked (markUnusable) so
// whatever ends the round still knows where to say so.
func (s *streamStore) drop(sessionID pgtype.UUID) {
	key := util.UUIDToString(sessionID)

	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.byKey[key]; ok {
		if entry.guard != nil {
			entry.guard.Stop()
		}
		delete(s.byKey, key)
	}
}

// depth reports how many bubbles are open. Diagnostics and tests only.
func (s *streamStore) depth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byKey)
}

func (s *streamStore) expiredLocked(h streamHandle) bool {
	return s.now().Sub(h.CreatedAt) > s.maxAge
}

// sweepLocked evicts handles the server would no longer accept, and the notes
// left by rounds too old to still be running. The guard timer normally retires
// an entry long before this fires; the sweep is what keeps a process whose
// timers were beaten by a clock jump from accumulating keys forever, and it is
// the only thing that bounds the notes, which no timer touches. Caller holds
// s.mu.
func (s *streamStore) sweepLocked() {
	for key, entry := range s.byKey {
		if s.expiredLocked(entry.handle) {
			if entry.guard != nil {
				entry.guard.Stop()
			}
			delete(s.byKey, key)
		}
	}
	now := s.now()
	for key, round := range s.ended {
		if now.Sub(round.at) > roundMemory {
			delete(s.ended, key)
		}
	}
}
