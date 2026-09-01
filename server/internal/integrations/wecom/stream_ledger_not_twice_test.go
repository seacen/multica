package wecom

// stream_ledger_not_twice_test.go — the two ways the ledger used to order a
// SECOND copy of one run's ending.
//
// Both are the not-twice rule failing from the inside: nobody publishes the
// ending twice, the store itself hands out the right to say it again. One comes
// from reading a delivery nobody could confirm as a delivery that failed (I4),
// the other from deciding whether a run has already been spoken for by looking
// at the ADDRESS instead of the run.
//
// Neither is visible to go build, go vet or the race detector — the first is a
// boolean read one way rather than the other, the second is a branch order —
// so these tests are the only thing that holds them shut.

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ---- I4: an outcome nobody could confirm ----

// halfClosedConn is the socket whose failures prove nothing: every write is
// entered and then raises, which is what a half-closed TCP connection does to
// the local side for bytes the peer already has. writeLocked marks exactly this
// case errWriteAttempted, and unconfirmedReason reads that as "the message may
// already be in front of the user".
//
// It is the cheapest deterministic unknown. An ack timeout is the same class
// and takes five real seconds to produce.
type halfClosedConn struct {
	mu     sync.Mutex
	writes int
	// onWrite runs inside the write, before it raises. It is how a test says
	// "the Supervisor noticed the drop while this frame was going out".
	onWrite func()
}

func (c *halfClosedConn) WriteMessage(_ int, _ []byte) error {
	c.mu.Lock()
	c.writes++
	hook := c.onWrite
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return errors.New("write tcp 10.0.0.1:443: broken pipe")
}
func (c *halfClosedConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }
func (c *halfClosedConn) SetReadDeadline(time.Time) error   { return nil }
func (c *halfClosedConn) SetWriteDeadline(time.Time) error  { return nil }
func (c *halfClosedConn) Close() error                      { return nil }

func (c *halfClosedConn) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

// onto puts a different socket in front of the installation, the way the
// Supervisor does after a drop. Unlike reconnect it takes the connection, so a
// test can hand over one that answers nothing.
func (r *bubbleRig) onto(conn wsConn) {
	r.senders.set(r.instID, newWSSender(conn, nil))
}

// TestAnEndingNobodyCouldConfirmIsNotSaidAgain is I4, on the path that produces
// it in production: a run fails while the bot's socket is half-closed.
//
// Both routes out of writeClosing are entered — the closing frame, then the
// plain message it falls back to — and both raise after the write itself. The
// words may be on the user's screen and nothing that happens later will ever
// settle whether they are. task:failed has two publishers and a sweeper tick
// repeats either, so the repeat is not a hypothetical: if the ledger filed that
// unknown as a failure, the run went back on owed and the next publisher told
// the same person "这次没跑通" a second time under the first one.
//
// The rest of the ledger is unchanged by this: a send refused with
// errNoLiveConnection is provably nothing written and still leaves the run owed
// — TestEveryTerminalPathEndsInWordsOrLeavesTheRunOwed's "a refused delivery is
// not a delivery" is that half, and it has to keep passing alongside this one.
func TestAnEndingNobodyCouldConfirmIsNotSaidAgain(t *testing.T) {
	t.Parallel()
	rig := newBoundRoomRig(t)
	rig.askedInTheRoom(t, "task-1")
	rig.ran(t, "REQ-1", 1, "task-1")

	dead := &halfClosedConn{}
	rig.onto(dead)
	rig.failed(t, "task-1", false)

	if got := dead.attempts(); got != 2 {
		t.Fatalf("the failure notice entered the socket %d time(s), want 2 (the closing "+
			"frame and the plain message it falls back to) — this test is about an outcome "+
			"nobody could confirm, and a delivery that never reached the write is a "+
			"different case entirely", got)
	}

	// The socket comes back and the same task:failed is published again — a
	// sweeper tick, or the other publisher of it.
	conn := rig.reconnect()
	rig.failed(t, "task-1", false)

	if got := said(t, conn); len(got) != 0 {
		t.Fatalf("the repeat told the room %q about a run whose ending had already gone out "+
			"unconfirmed — the ledger read an unknown as a failure and ordered the same "+
			"words said a second time to somebody who may be reading the first", got)
	}
}

// TestAClosingFrameLostWithTheSocketIsNotSaidAgain is I4 across the PAIR of
// attempts writeClosing makes, which is where the rule is easiest to lose: the
// function returns one error for two sends.
//
// The socket drops while the closing frame is in the write, so the frame was
// entered and nothing came back (unknown — it may have sealed the bubble), and
// the plain message it falls back to finds no sender in the registry at all
// (definite — it never reached a socket). Returning the second one on its own
// tells the ledger nothing was said, and the repeat then writes the failure
// notice into a chat whose bubble may already be showing it.
func TestAClosingFrameLostWithTheSocketIsNotSaidAgain(t *testing.T) {
	t.Parallel()
	rig := newBoundRoomRig(t)
	rig.askedInTheRoom(t, "task-1")
	rig.ran(t, "REQ-1", 1, "task-1")

	dying := &halfClosedConn{}
	sender := newWSSender(dying, nil)
	dying.onWrite = func() { rig.senders.clear(rig.instID, sender) }
	rig.senders.set(rig.instID, sender)

	rig.failed(t, "task-1", false)
	if got := dying.attempts(); got != 1 {
		t.Fatalf("the socket took %d write(s), want 1 — the closing frame goes out and the "+
			"fallback is supposed to find the registry already empty", got)
	}

	conn := rig.reconnect()
	rig.failed(t, "task-1", false)

	if got := said(t, conn); len(got) != 0 {
		t.Fatalf("the repeat told the room %q — the closing frame's unknown outcome was "+
			"reported as the fallback's definite failure, so the ledger put the run back "+
			"on owed and the words went out under a bubble that may already say them", got)
	}
}

// ---- the not-twice rule, on a round that never got a bubble ----

// TestTwoEndingsForARoundWithNoBubbleSpeakOnce holds the branch order in
// beginEndingLocked.
//
// The round is bound to a run and never painted — the opening frame refused, or
// the flush arriving ahead of the goroutine that paints — so the note the first
// ending leaves has no address on it. The first delivery is still on the wire
// (the run is on speaking) when the second publisher of the same ending
// arrives. Asking "do I know where this round speaks" before "has this run been
// spoken for" sent that second publisher off to find its own chat in the
// binding row and say the same thing there.
//
// Driven at the store rather than through the bus because the interleaving is
// the subject: two goroutines racing for real reproduced it about once in
// twenty runs, which is not a test.
func TestTwoEndingsForARoundWithNoBubbleSpeakOnce(t *testing.T) {
	t.Parallel()
	s := newStreamStore()
	sid := bubbleSessionID(t)
	s.bind(sid, 7, "task-A")

	onTheWire := make(chan struct{})
	release := make(chan struct{})
	var spoke sync.WaitGroup
	spoke.Add(1)

	var first, second int
	go func() {
		defer spoke.Done()
		s.sayEnding(sid, byTask("task-A"), roundOver, nil,
			func(roundTurn) (roundAddress, error) {
				first++
				close(onTheWire)
				<-release
				// Nowhere to speak on this turn, so the delivery found the chat
				// itself — which is what makes the ledger record an address it
				// did not have.
				return roundAddress{InstallationID: pgtype.UUID{Valid: true}, ChatID: "CHAT_1"}, nil
			})
	}()

	<-onTheWire
	verdict, err := s.sayEnding(sid, byTask("task-A"), roundOver, nil,
		func(roundTurn) (roundAddress, error) {
			second++
			return roundAddress{}, nil
		})
	close(release)
	spoke.Wait()

	if err != nil {
		t.Fatalf("the second publisher returned %v, want nil — it has nothing to say, "+
			"which is not a failure", err)
	}
	if first != 1 || second != 0 {
		t.Fatalf("deliveries that actually spoke: first=%d second=%d, want 1 and 0 — "+
			"the run's ending was on the wire and the second publisher said it again, "+
			"because a round that never painted a bubble leaves the note with no address "+
			"and the address was asked about before the run was", first, second)
	}
	if verdict != roundToldAlready {
		t.Fatalf("the second publisher was told %v, want roundToldAlready", verdict)
	}
}
