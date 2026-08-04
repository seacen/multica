package wecom

// stream_disown_test.go — the round that outlives its bubble.
//
// 846608 says the server will take no further frame for this stream. The
// spinner it painted is on the user's screen and nothing can seal it, so the
// bubble is over — but the round is not. The agent is still working, and
// whatever it ends with, the user has been promised it in a new message.
// Everything here is about that promise being kept.

import (
	"context"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// disowned puts a rig in the state this file is about: a bubble open, the
// server refusing the next frame on it, and one progress refresh already run
// into that refusal. guardAfter of 0 leaves the manager's own default.
func disowned(t *testing.T, guardAfter time.Duration) *streamRig {
	t.Helper()
	rig := newStreamRig(t)
	if guardAfter > 0 {
		rig.typing.guardAfter = guardAfter
	}
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")
	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamStuck {
		t.Fatalf("setup: the user was told %v, want the notice that the bubble is stuck", got)
	}
	return rig
}

// failTheRun publishes what FailTask publishes for a chat run.
func failTheRun(rig *streamRig) {
	bus := events.New()
	rig.typing.Register(bus)
	bus.Publish(events.Event{
		Type:    protocol.EventTaskFailed,
		Payload: map[string]any{"chat_session_id": uuidText(rig.session)},
	})
}

// waitForContents blocks until the connection has recorded n plain messages,
// so a test can act on a message written by the guard's own goroutine without
// sleeping for a fixed time.
func waitForContents(t *testing.T, rig *streamRig, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := contentsOf(&rig.conn.recordingConn); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	got := contentsOf(&rig.conn.recordingConn)
	t.Fatalf("the user received %d messages, want %d: %v", len(got), n, got)
	return nil
}

// TestADisownedBubbleTakesNoMoreRefreshes — the handle is kept from here on,
// so the thing it must not buy is a refusal every 1.5s for the rest of the
// run, plus one more on the daemon's own request for every tool call after
// that.
func TestADisownedBubbleTakesNoMoreRefreshes(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "one.go"})))

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 2 {
		t.Fatalf("setup wrote %d frames, want the opening frame and the one the server refused", got)
	}
	for i := 0; i < 3; i++ {
		clock.advance(progressMinInterval)
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "two.go"})))
	}

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 2 {
		t.Errorf("wrote %d frames, want no further attempt on a stream the server has disowned", got)
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 {
		t.Errorf("the user was told %d times: %v", len(got), got)
	}
}

// TestAFailureAfterTheServerDisownsTheBubbleStillReachesTheUser is the hole
// this file was written for. StreamFailed is the only "that run did not go
// through" WeCom ever produces, and the handle is the only address it has. A
// bubble the server disowned used to take that address with it, so a run that
// then failed left the user with a dead spinner and a promise of a new message
// that never came.
func TestAFailureAfterTheServerDisownsTheBubbleStillReachesTheUser(t *testing.T) {
	rig := disowned(t, 0)

	failTheRun(rig)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 2 || got[1] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("the user received %v, want the stuck notice and then the failure", got)
	}
	if frames := len(streamViews(t, &rig.conn.recordingConn)); frames != 2 {
		t.Errorf("wrote %d frames; the closing frame was owed to a stream that takes none", frames)
	}
	if depth := rig.streams.depth(); depth != 0 {
		t.Errorf("store depth %d after the round ended", depth)
	}
}

// TestTheFailureNoticeWaitsForTheNextConnection — the same notice, sent during
// a reconnect window. Nothing about a disowned bubble may cost the fallback the
// holding queue that P1-4 built for it.
func TestTheFailureNoticeWaitsForTheNextConnection(t *testing.T) {
	rig := disowned(t, 0)
	rig.senders.clear(rig.inst.ID)

	failTheRun(rig)

	if n := rig.senders.pending.depth(rig.inst.ID); n != 1 {
		t.Fatalf("queue depth %d; the failure notice was not held for the next connection", n)
	}
	conn := &recordingConn{}
	rig.senders.set(rig.inst.ID, newWSSender(conn, testLogger()))
	rig.senders.flushPending(rig.inst.ID)

	if got := contentsOf(conn); len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("after the reconnect the user received %v, want the failure notice", got)
	}
}

// TestTheGuardStillEndsARoundOnADisownedBubble — dropping the handle stopped
// the guard with it. The guard is what accounts for a run still going at the
// five-minute mark, and a round whose bubble is already stuck needs that
// account more than any other, not less.
func TestTheGuardStillEndsARoundOnADisownedBubble(t *testing.T) {
	rig := disowned(t, 100*time.Millisecond)

	got := waitForContents(t, rig, 2)

	if got[1] != copyPacks[LocaleZhHans].StreamStillWorking {
		t.Errorf("the guard said %q, want the still-working copy as a plain message", got[1])
	}
	if frames := len(streamViews(t, &rig.conn.recordingConn)); frames != 2 {
		t.Errorf("wrote %d frames; the guard owes no frame to a stream that takes none", frames)
	}
	if depth := rig.streams.depth(); depth != 0 {
		t.Errorf("store depth %d after the guard fired", depth)
	}
}

// TestNoHandleOutlivesADisownedRound — the handle is kept past 846608 on
// purpose, so every way the round can end has to be a way the handle goes.
func TestNoHandleOutlivesADisownedRound(t *testing.T) {
	cases := []struct {
		name  string
		guard time.Duration
		end   func(t *testing.T, rig *streamRig)
	}{
		{
			name: "the answer",
			end: func(t *testing.T, rig *streamRig) {
				NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
					handleEvent(chatDoneEvent(rig.session, "答案是 42"))
			},
		},
		{
			name: "a failure",
			end:  func(t *testing.T, rig *streamRig) { failTheRun(rig) },
		},
		{
			name: "no run at all",
			end: func(t *testing.T, rig *streamRig) {
				rig.typing.OnSettled(context.Background(), rig.session)
			},
		},
		{
			name:  "nothing, until the guard",
			guard: 100 * time.Millisecond,
			end:   func(t *testing.T, rig *streamRig) { waitForContents(t, rig, 2) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := disowned(t, tc.guard)
			tc.end(t, rig)
			if depth := rig.streams.depth(); depth != 0 {
				t.Errorf("store depth %d; the handle outlived the round", depth)
			}
		})
	}
}

// TestTheMarkIsWhatEachCallerReads — the store contract in one test. After the
// mark there is no bubble to write to and there is still an address to speak
// from, and both have to be true at once.
func TestTheMarkIsWhatEachCallerReads(t *testing.T) {
	store := newStreamStore()
	session := uuidOf(3)
	if store.markUnusable(session, "S") {
		t.Error("marked a session with no bubble open")
	}
	if store.open(session, streamHandle{ReqID: "R", StreamID: "S"}) != roundOpened {
		t.Fatal("open refused on an empty store")
	}

	if !store.markUnusable(session, "S") {
		t.Fatal("the store would not record the server's verdict")
	}
	if store.markUnusable(session, "S") {
		t.Error("a second refusal claimed the mark too, and would say so to the user again")
	}
	if _, ok := store.peek(session); ok {
		t.Error("peek offered a bubble the server has disowned")
	}
	if _, _, ok := store.feedFor(session, "task-1"); ok {
		t.Error("feedFor offered a bubble the server has disowned")
	}
	if store.depth() != 1 {
		t.Error("the handle was forgotten; the round's ending has no address left")
	}

	h, ok := store.takeHead(session, roundOver)
	if !ok || !h.Unusable {
		t.Fatalf("take returned (%+v, %v), want the addressing carrying the server's verdict", h, ok)
	}
	if h.ReqID != "R" || h.StreamID != "S" {
		t.Errorf("take returned %+v, want the handle as claimed", h)
	}
}

// TestADisownedHandleStillAgesOut — the backstop. A round that never ends at
// all, because the daemon holding it went away, must not leave a key behind for
// the life of the process.
func TestADisownedHandleStillAgesOut(t *testing.T) {
	store := newStreamStore()
	base := time.Now()
	store.now = func() time.Time { return base }
	session := uuidOf(3)
	store.open(session, streamHandle{ReqID: "R", StreamID: "S"})
	store.markUnusable(session, "S")

	store.now = func() time.Time { return base.Add(streamMaxAge + time.Second) }
	store.open(uuidOf(4), streamHandle{ReqID: "R2", StreamID: "S2"})

	if _, ok := store.takeHead(session, roundOver); ok {
		t.Error("take returned a handle from a round the store should have swept")
	}
	if depth := store.depth(); depth != 1 {
		t.Errorf("store depth %d, want only the fresh round", depth)
	}
}

// TestTheAnswerAfterADisownedBubbleGoesOutAsANewMessage — what the stuck
// notice promised. The answer must not be spent on a frame the server has
// already said it will not take.
func TestTheAnswerAfterADisownedBubbleGoesOutAsANewMessage(t *testing.T) {
	rig := disowned(t, 0)

	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 2 || got[1] != "答案是 42" {
		t.Fatalf("the user received %v, want the stuck notice and then the answer", got)
	}
	if frames := len(streamViews(t, &rig.conn.recordingConn)); frames != 2 {
		t.Errorf("wrote %d frames; the answer was owed to a stream that takes none", frames)
	}
}
