package wecom

// stream_budget_test.go — how much of somebody else's request a bubble may
// borrow.
//
// The progress subscriber runs on the goroutine that published the event: the
// daemon's own POST /api/daemon/tasks/{id}/messages handler, which has five
// seconds and does not retry. A batch that misses that deadline is gone, and
// with it the transcript every other surface reads. So the deadline a refresh
// is given has to reach the socket, not just the wait for the verdict.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errWriteStalled = errors.New("stalled: write deadline exceeded")

// stallingConn is a socket under back-pressure. A write parks until the write
// deadline it was handed and then fails the way a congested one does, so a test
// can see whether the caller's deadline ever reached the socket at all.
type stallingConn struct {
	recordingConn
	dl       sync.Mutex
	dlAt     time.Time
	writing  chan struct{}
	released chan struct{}
}

func newStallingConn() *stallingConn {
	return &stallingConn{writing: make(chan struct{}, 8), released: make(chan struct{})}
}

// release lets every parked write finish, so a test does not have to wait out
// the deadline of a write it only needed in order to occupy the writer.
func (c *stallingConn) release() { close(c.released) }

func (c *stallingConn) SetWriteDeadline(t time.Time) error {
	c.dl.Lock()
	defer c.dl.Unlock()
	c.dlAt = t
	return nil
}

func (c *stallingConn) WriteMessage(int, []byte) error {
	c.dl.Lock()
	at := c.dlAt
	c.dl.Unlock()
	select {
	case c.writing <- struct{}{}:
	default:
	}
	if d := time.Until(at); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-c.released:
		}
	}
	return errWriteStalled
}

// budgetCeiling is the slack a test allows over the caller's own deadline
// before it calls the budget broken. Well under the ten-second write deadline
// that used to apply regardless of what the caller asked for.
const budgetCeiling = 2 * time.Second

// TestAStreamFrameNeverOutstaysItsCaller — the deadline a refresh is given has
// to be the deadline the socket gets. A fixed ten-second write deadline is an
// order of magnitude past what the caller said it could spend, and on the
// progress path the caller is the daemon's transcript request.
func TestAStreamFrameNeverOutstaysItsCaller(t *testing.T) {
	conn := newStallingConn()
	sender := newWSSender(conn, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sender.respondStream(ctx, "REQ-42", "S-1", "<think>正在读取</think>", false)
	spent := time.Since(start)

	if err == nil {
		t.Fatal("a frame that never reached the socket reported success")
	}
	if spent > budgetCeiling {
		t.Fatalf("a frame with a 150ms budget held its caller for %v", spent)
	}
}

// TestARefreshGivesUpRatherThanQueueBehindALongWrite — the other half of the
// budget. Waiting for the writer is not bounded by the ack timeout or by
// anything else: a 20KB answer, a ping and a push all take the same mutex, and
// a refresh that queues behind one spends the caller's whole request there.
func TestARefreshGivesUpRatherThanQueueBehindALongWrite(t *testing.T) {
	conn := newStallingConn()
	sender := newWSSender(conn, testLogger())

	// A push already on the socket, holding the writer for its own budget.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sender.sendText("T-alex", chatTypeSingleInt, "一份很长的答案")
	}()
	select {
	case <-conn.writing:
	case <-time.After(2 * time.Second):
		t.Fatal("the push never took the writer")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_ = sender.respondStream(ctx, "REQ-42", "S-1", "<think>正在读取</think>", false)
	spent := time.Since(start)

	conn.release()
	<-done
	if spent > budgetCeiling {
		t.Fatalf("a refresh with a 150ms budget waited %v for the writer", spent)
	}
}

// TestAFireAndForgetWriteKeepsTheFullDeadline — the ping and the proactive push
// have no caller to answer to, so shortening a refresh's budget must not
// shorten theirs.
func TestAFireAndForgetWriteKeepsTheFullDeadline(t *testing.T) {
	conn := &deadlineConn{}
	sender := newWSSender(conn, testLogger())

	before := time.Now()
	if err := sender.sendText("T-alex", chatTypeSingleInt, "hi"); err != nil {
		t.Fatalf("sendText: %v", err)
	}
	if got := len(conn.sends()); got != 1 {
		t.Fatalf("wrote %d push frames, want 1", got)
	}
	if got := conn.last(); got.Before(before.Add(writeDeadline - time.Second)) {
		t.Errorf("push got a %v deadline, want the full %v", got.Sub(before), writeDeadline)
	}
}

// TestARefreshHandsItsOwnDeadlineToTheSocket pins the same rule from the other
// side: the deadline the socket is given is the caller's, not the connection's
// standing one.
func TestARefreshHandsItsOwnDeadlineToTheSocket(t *testing.T) {
	conn := &deadlineConn{}
	sender := newWSSender(conn, testLogger())

	ctx, cancel := context.WithTimeout(context.Background(), progressWriteTimeout)
	defer cancel()
	want, _ := ctx.Deadline()
	go func() { _ = sender.respondStream(ctx, "REQ-42", "S-1", "<think>正在读取</think>", false) }()
	waitForStreamFrames(t, &conn.recordingConn, 1)

	if got := conn.last(); got.After(want) {
		t.Errorf("socket deadline is %v past the caller's own", got.Sub(want))
	}
}

// deadlineConn records the write deadline it was given.
type deadlineConn struct {
	recordingConn
	mu   sync.Mutex
	seen time.Time
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = t
	return nil
}

func (c *deadlineConn) last() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen
}
