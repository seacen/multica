package wecom

// regression_disown_fallback_budget_test.go — guards the one write on the
// progress path that still spends somebody else's request without asking.
//
// stream_budget_test.go settled the rule for stream frames: a refresh runs on
// the goroutine that published the event — the daemon's POST of a run's
// transcript, five seconds, no retry — so the deadline the caller chose has to
// reach the socket. The disown fallback is the same goroutine and the same
// request, and it was left out. When the server refuses the next frame on a
// bubble (846608 / 846605) the refresh turns around and pushes a plain message
// telling the user where the rest of the round will appear, and that push is
// handed no deadline at all: it takes the connection's own ten-second write
// deadline instead. A congested socket then holds the daemon's request for ten
// seconds, the batch misses its five-second budget, and the transcript — the
// record every other surface in the product reads — is dropped.
//
// It only bites on the unluckiest turn there is: the user's bubble is already
// dead, and the cost of telling them so is the run's own history.
//
// Nothing else in the package joins the two halves. stream_budget_test.go
// exercises the frame path against a stalling socket and never reaches the
// fallback; stream_disown_test.go drives the fallback against a socket that
// always writes instantly and so never has to answer for time.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// fallbackBudgetCeiling is how long a test waits before it calls the caller's
// budget broken. Comfortably above the second a refresh gives itself and well
// under the ten-second write deadline that applies when nobody passes one, so
// neither a slow machine nor a fixed implementation lands near it.
const fallbackBudgetCeiling = 3 * time.Second

// congestedPushConn is the socket a disowned bubble runs into: it answers every
// stream frame at once with whatever verdict the test set, and then parks any
// plain push for exactly the write deadline it was handed before failing the
// way a congested one does. Parking for the deadline rather than a fixed time
// is the whole point — it is how a test sees whose budget the push is spending.
type congestedPushConn struct {
	ackingConn

	mu       sync.Mutex
	deadline time.Time
	budget   time.Duration // what the socket was given for the push, once one arrives

	pushed   chan struct{}
	release  sync.Once
	released chan struct{}
}

func newCongestedPushConn() *congestedPushConn {
	return &congestedPushConn{
		pushed:   make(chan struct{}, 8),
		released: make(chan struct{}),
	}
}

// unstall lets a parked push finish now, so a test that has already seen what
// it came for does not have to sit out the deadline it is complaining about.
func (c *congestedPushConn) unstall() { c.release.Do(func() { close(c.released) }) }

func (c *congestedPushConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = t
	return nil
}

// pushBudget is how long the socket was told to keep trying the push. Reported
// in the failure message only: what the test asserts on is the wall clock the
// caller lost, not the way any particular fix arranges to shorten it.
func (c *congestedPushConn) pushBudget() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.budget
}

func (c *congestedPushConn) WriteMessage(mt int, data []byte) error {
	var f struct {
		Cmd string `json:"cmd"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	if f.Cmd != cmdSendMsg {
		// Stream frames go straight through and get their verdict, so the test
		// reaches the fallback the same way a real turn does.
		return c.ackingConn.WriteMessage(mt, data)
	}

	c.mu.Lock()
	at := c.deadline
	c.budget = time.Until(at)
	c.mu.Unlock()
	select {
	case c.pushed <- struct{}{}:
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

// congestedRig is the package's standard rig with its socket swapped for one
// that disowns the bubble and then stalls the push, and with the bubble already
// open and already refused.
func congestedRig(t *testing.T, rig *streamRig) *congestedPushConn {
	t.Helper()
	conn := newCongestedPushConn()
	sender := newWSSender(conn, testLogger())
	conn.mu.Lock()
	conn.ackingConn.sender = sender
	conn.mu.Unlock()
	rig.senders.set(rig.inst.ID, sender)
	t.Cleanup(conn.unstall)

	rig.ingest(t, "REQ-42")
	conn.rejectWith(errcodeStreamExpired, "stream expired")
	return conn
}

// reportBudget fails the test when a refresh held its caller past the ceiling,
// and checks that the notice the refresh owed the user was not simply dropped
// on the way.
func reportBudget(t *testing.T, rig *streamRig, conn *congestedPushConn, budget, spent time.Duration) {
	t.Helper()
	if spent > fallbackBudgetCeiling {
		t.Errorf("a progress refresh with a %v budget held its caller for %v.\n"+
			"the server disowned the bubble (846608), and the message that tells the user where\n"+
			"the rest of the round will appear was pushed without the caller's deadline, so the\n"+
			"socket was given %v for it — the connection's own %v write deadline, not the caller's.\n"+
			"on this path the caller is the daemon's POST of a run's transcript: five seconds, no\n"+
			"retry. it misses its deadline and the whole batch is lost, so the run's history is\n"+
			"gone from every surface that reads it — and the only thing bought with it was one\n"+
			"line of apology in one chat.",
			budget, spent, conn.pushBudget(), writeDeadline)
	}
	if len(contentsOf(&conn.recordingConn)) == 0 && rig.senders.pending.depth(rig.inst.ID) == 0 {
		t.Errorf("the bubble is dead and the user was never told: the notice neither reached the\n" +
			"socket nor was held for the next connection. a stuck spinner and no word about where\n" +
			"the answer is going to appear is worse than the delay this test is about.")
	}
}

// runUnderCongestion drives one refresh and returns how long its caller was
// held. It gives up waiting at the ceiling rather than sitting out the full
// write deadline, then unstalls the socket so the call can finish and its
// bookkeeping settle before anything is asserted.
func runUnderCongestion(conn *congestedPushConn, refresh func()) time.Duration {
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		refresh()
	}()

	var spent time.Duration
	select {
	case <-done:
		spent = time.Since(start)
	case <-time.After(fallbackBudgetCeiling):
		spent = time.Since(start)
	}
	conn.unstall()
	<-done
	return spent
}

// TestATranscriptPostIsNotHeldByTheNoticeThatABubbleIsStuck is the defect on
// the path it actually happens on: a task message published by the daemon's own
// handler, on the daemon's own goroutine, against a bubble the server has just
// disowned. Everything the refresh does is bounded by progressWriteTimeout
// except the last thing it does.
func TestATranscriptPostIsNotHeldByTheNoticeThatABubbleIsStuck(t *testing.T) {
	rig, bus, _, _ := busRig(t)
	conn := congestedRig(t, rig)

	spent := runUnderCongestion(conn, func() {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "config.go"})))
	})

	reportBudget(t, rig, conn, progressWriteTimeout, spent)
}

// TestTheStuckNoticeHonoursTheDeadlineItsCallerGave pins the same rule from the
// caller's side, so it holds for whoever calls next. UpdateProgress takes a
// context because the work behind it belongs to whoever is waiting; a caller
// that says it can spend 150ms must not lose ten seconds to a chat.
func TestTheStuckNoticeHonoursTheDeadlineItsCallerGave(t *testing.T) {
	rig := newStreamRig(t)
	conn := congestedRig(t, rig)

	const budget = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	spent := runUnderCongestion(conn, func() {
		rig.typing.UpdateProgress(ctx, rig.session, "正在查日历")
	})

	reportBudget(t, rig, conn, budget, spent)
}
