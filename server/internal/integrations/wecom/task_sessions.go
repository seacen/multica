package wecom

// task_sessions.go — remembering which chat a task belongs to.
//
// task:message and task:progress both carry a task id and no chat session, so
// the session a bubble is keyed on has to be read back off the task row. One
// run posts dozens of tool messages and the answer never changes, so without
// this the bubble would cost a database read per tool call on a code path that
// runs inside the daemon's own HTTP request.
//
// The absence of a session is cached too, and that is the more important half.
// Issue runs and autopilot runs publish exactly the same events, have no chat
// session at all, and on most deployments outnumber chat runs — so "this task
// has no bubble" is the answer being looked up most often.

import (
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

const (
	// taskSessionTTL is memory hygiene rather than correctness: a task's chat
	// session never changes, so an entry is only ever stale in the sense that
	// nobody will ask for it again. Comfortably past the stream window, so no
	// live bubble ever re-reads the row it already read.
	taskSessionTTL = 10 * time.Minute

	// taskSessionMax bounds the map on a busy workspace. Well above the number
	// of tasks that can be in flight inside one TTL; hitting it means
	// something unusual, and dropping the oldest entries costs one query each.
	taskSessionMax = 512
)

type taskSessionEntry struct {
	session pgtype.UUID // invalid means "this task has no chat session"
	at      time.Time
}

type taskSessionCache struct {
	mu     sync.Mutex
	byTask map[string]taskSessionEntry
	max    int
	ttl    time.Duration
	now    func() time.Time
}

func newTaskSessionCache() *taskSessionCache {
	return &taskSessionCache{
		byTask: make(map[string]taskSessionEntry),
		max:    taskSessionMax,
		ttl:    taskSessionTTL,
		now:    time.Now,
	}
}

// get returns a task's chat session and whether the question has been answered
// before. A hit whose session is invalid is a task known to have none — the
// caller must stop there rather than fall through to the database.
func (c *taskSessionCache) get(taskID string) (pgtype.UUID, bool) {
	if taskID == "" {
		return pgtype.UUID{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.byTask[taskID]
	if !ok {
		return pgtype.UUID{}, false
	}
	if c.now().Sub(entry.at) > c.ttl {
		delete(c.byTask, taskID)
		return pgtype.UUID{}, false
	}
	return entry.session, true
}

// put records an answer, including the answer "none".
func (c *taskSessionCache) put(taskID string, session pgtype.UUID) {
	if taskID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if len(c.byTask) >= c.max {
		c.evictLocked(now)
	}
	c.byTask[taskID] = taskSessionEntry{session: session, at: now}
}

func (c *taskSessionCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byTask)
}

// evictLocked makes room. Expired entries go first; if that was not enough the
// map is emptied rather than half-sorted, because at this point the cache is
// not doing its job anyway and the cost of being wrong is one query per task.
// Caller holds c.mu.
func (c *taskSessionCache) evictLocked(now time.Time) {
	for k, e := range c.byTask {
		if now.Sub(e.at) > c.ttl {
			delete(c.byTask, k)
		}
	}
	if len(c.byTask) >= c.max {
		clear(c.byTask)
	}
}
