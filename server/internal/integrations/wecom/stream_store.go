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

	InstallationID pgtype.UUID
	ChatID         string
	ChatType       int

	// Locale is the installation's copy language, captured here so the closing
	// frame does not need a second installation read to know what to say.
	Locale Locale

	CreatedAt time.Time
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
}

// streamStore maps chat_session_id to the open bubble for that session.
type streamStore struct {
	mu     sync.Mutex
	byKey  map[string]streamEntry
	maxAge time.Duration
	now    func() time.Time
}

func newStreamStore() *streamStore {
	return &streamStore{
		byKey:  make(map[string]streamEntry),
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
	s.byKey[key] = streamEntry{handle: h}
	return true
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

// take removes a session's handle and hands it over. A handle past maxAge is
// dropped and reported as absent: the server would refuse the frame, and a
// caller that believed it had a bubble would leave the user with nothing.
func (s *streamStore) take(sessionID pgtype.UUID) (streamHandle, bool) {
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
	return entry.handle, true
}

// peek reads a session's handle without consuming it — the progress-refresh
// path, which expects to write to the same bubble again. An expired handle is
// evicted here too.
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
	return entry.handle, true
}

// feedFor returns a session's open bubble and the list of steps shown inside
// it, creating the list on first use. Like peek it leaves the handle in place
// and disowns an expired one: a run whose window has closed gets no more
// refreshes, and its list goes with it.
func (s *streamStore) feedFor(sessionID pgtype.UUID) (streamHandle, *progressFeed, bool) {
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
	if entry.feed == nil {
		entry.feed = newProgressFeed(s.now)
		s.byKey[key] = entry
	}
	return entry.handle, entry.feed, true
}

// drop forgets a session's handle without sending anything — used when the
// opening frame was refused and the bubble the handle describes never existed.
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

// sweepLocked evicts handles the server would no longer accept. The guard
// timer normally retires an entry long before this fires; the sweep is what
// keeps a process whose timers were beaten by a clock jump from accumulating
// keys forever. Caller holds s.mu.
func (s *streamStore) sweepLocked() {
	for key, entry := range s.byKey {
		if s.expiredLocked(entry.handle) {
			if entry.guard != nil {
				entry.guard.Stop()
			}
			delete(s.byKey, key)
		}
	}
}
