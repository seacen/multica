package wecom

// outbound_queue.go — a per-installation holding queue for messages the aibot
// socket could not take.
//
// Why it exists: the smart-bot long connection is the ONLY way out. WeCom
// publishes no HTTPS equivalent of aibot_send_msg — the long-connection docs
// (https://developer.work.weixin.qq.com/document/path/101463) describe it
// purely as a WebSocket cmd, and the one HTTPS reply endpoint that does exist,
// https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=…, belongs
// to 回调模式: it is single-use, tied to a specific triggering callback, and
// the long-connection frame carries no response_code to use it with. So there
// is no fallback transport to reach for; the only question is whether an
// undeliverable message is dropped or held.
//
// It used to be dropped. Every reconnect window — a lease flip, the
// Supervisor's backoff, the seconds after a revoke — silently ate the agent's
// answer and any inbox card due in that moment. The user saw a question go
// unanswered with nothing anywhere to say why.
//
// Held messages are bounded on both axes: maxPendingPerInstallation entries
// and pendingTTL of age. Past either bound we drop and log, because a reply
// nobody will read is not worth unbounded memory.

import (
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

// maxPendingPerInstallation caps one installation's holding queue. A bot down
// for hours in a busy workspace must not grow the heap without limit; past
// the cap the oldest entry makes room, since the newest reply is the one
// still worth reading.
const maxPendingPerInstallation = 1000

// pendingTTL is how long a message is worth resending. A day matches WeCom's
// own 24h window for a proactive push tied to a user message; past it the
// conversation has moved on.
const pendingTTL = 24 * time.Hour

// pendingSend is one outbound message waiting for a connection. Addressing is
// captured at enqueue time — the binding row it came from may be gone by the
// time we resend.
type pendingSend struct {
	ChatID   string
	ChatType int
	Content  string

	// enqueuedAt is stamped by the queue, not the caller, so age is measured
	// on one clock.
	enqueuedAt time.Time
}

// outboundQueue holds undeliverable messages per installation, oldest first.
type outboundQueue struct {
	mu   sync.Mutex
	byID map[string][]pendingSend

	// draining counts the messages that have left the slice but have not yet
	// reached the socket. The drain pops first and writes afterwards, and in
	// between the message is in neither place — so a fresh reply arriving in
	// that window saw an empty queue, went straight out, and landed AHEAD of
	// the answer that had been waiting for the socket since before it existed.
	// The conversation read backwards, which is the one thing the queue is
	// there to prevent. Counting the in-flight message keeps depth honest.
	draining map[string]int

	max int
	ttl time.Duration
	now func() time.Time
	log *slog.Logger
}

func newOutboundQueue(log *slog.Logger) *outboundQueue {
	if log == nil {
		log = slog.Default()
	}
	return &outboundQueue{
		byID:     make(map[string][]pendingSend),
		draining: make(map[string]int),
		max:      maxPendingPerInstallation,
		ttl:      pendingTTL,
		now:      time.Now,
		log:      log,
	}
}

// enqueue appends a message, dropping the oldest entries when the queue is
// already at its cap.
func (q *outboundQueue) enqueue(id pgtype.UUID, msg pendingSend) {
	key := util.UUIDToString(id)
	msg.enqueuedAt = q.now()

	q.mu.Lock()
	defer q.mu.Unlock()
	pending := append(q.byID[key], msg)
	if overflow := len(pending) - q.max; overflow > 0 {
		pending = pending[overflow:]
		q.log.Warn("wecom outbound: queue full, dropped the oldest pending messages",
			"installation_id", key, "dropped", overflow, "cap", q.max)
	}
	q.byID[key] = pending
}

// pop returns the oldest message still worth sending, discarding expired ones
// on the way. ok is false when nothing is left.
func (q *outboundQueue) pop(id pgtype.UUID) (pendingSend, bool) {
	key := util.UUIDToString(id)

	q.mu.Lock()
	defer q.mu.Unlock()
	pending := q.byID[key]
	expired := 0
	for len(pending) > 0 {
		msg := pending[0]
		pending = pending[1:]
		if q.now().Sub(msg.enqueuedAt) > q.ttl {
			expired++
			continue
		}
		q.store(key, pending)
		if expired > 0 {
			q.log.Warn("wecom outbound: dropped pending messages past their shelf life",
				"installation_id", key, "dropped", expired, "ttl", q.ttl)
		}
		return msg, true
	}
	q.store(key, nil)
	if expired > 0 {
		q.log.Warn("wecom outbound: dropped pending messages past their shelf life",
			"installation_id", key, "dropped", expired, "ttl", q.ttl)
	}
	return pendingSend{}, false
}

// pushFront returns an undelivered message to the head of the queue, keeping
// conversation order across a drain that failed partway.
func (q *outboundQueue) pushFront(id pgtype.UUID, msg pendingSend) {
	key := util.UUIDToString(id)

	q.mu.Lock()
	defer q.mu.Unlock()
	pending := append([]pendingSend{msg}, q.byID[key]...)
	if overflow := len(pending) - q.max; overflow > 0 {
		pending = pending[:q.max]
		q.log.Warn("wecom outbound: queue full while re-queueing, dropped the newest messages",
			"installation_id", key, "dropped", overflow, "cap", q.max)
	}
	q.byID[key] = pending
}

// depth reports how many messages are ahead of a new one: those still in the
// queue plus the one being written right now.
func (q *outboundQueue) depth(id pgtype.UUID) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := util.UUIDToString(id)
	return len(q.byID[key]) + q.draining[key]
}

// beginDrain claims a popped message as in-flight; endDrain releases it once
// it has reached the socket or been put back.
func (q *outboundQueue) beginDrain(id pgtype.UUID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.draining[util.UUIDToString(id)]++
}

func (q *outboundQueue) endDrain(id pgtype.UUID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := util.UUIDToString(id)
	if q.draining[key] > 1 {
		q.draining[key]--
		return
	}
	delete(q.draining, key)
}

// store writes back a slice, dropping the map entry when empty so an
// installation that comes and goes does not leak keys. Caller holds q.mu.
func (q *outboundQueue) store(key string, pending []pendingSend) {
	if len(pending) == 0 {
		delete(q.byID, key)
		return
	}
	q.byID[key] = pending
}
