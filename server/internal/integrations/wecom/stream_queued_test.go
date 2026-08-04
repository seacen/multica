package wecom

// stream_queued_test.go — the message that arrives while the last one is still
// running.
//
// One bubble per BURST, one bubble per ROUND. Messages closer together than
// the engine's debounce window are one run and share a bubble; a message past
// the window starts a round of its own, queued behind the run in flight — and
// it opens a bubble of its own immediately, with no words in it. The client's
// own loading affordance is the receipt; a wait with nothing on screen reads
// as a message that was lost, and a textual "queued" receipt is a receipt in
// somebody's language before there is anything to say (this used to be a plain
// StreamQueued message, said once per bubble — a group's second asker got
// nothing at all).
//
// One deliberate trade: a queued bubble is a stream frame, not a holding-queue
// message, so a socket that is down when the message arrives costs the bubble
// entirely (the answer still arrives later as a plain message). The old text
// receipt survived a reconnect; a stream frame cannot, because its req_id is
// only good for the connection that delivered the callback.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// queuedRig is a stream rig whose store clock the test drives, so the gap
// between two messages is set rather than slept through.
func queuedRig(t *testing.T) (*streamRig, *testClock) {
	t.Helper()
	rig := newStreamRig(t)
	clock := newTestClock()
	rig.streams.now = clock.now
	return rig, clock
}

// ---- a queued message gets a bubble ----

// TestAMessageBehindALongRunOpensItsOwnBubble is the whole design. The first
// run has been going for minutes; the second message is a round of its own and
// will wait for it — and the user sees a second loading bubble the moment it
// lands, carrying that message's own req_id and a stream id of its own.
func TestAMessageBehindALongRunOpensItsOwnBubble(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-2")

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("wrote %d bubbles, want one per round", len(frames))
	}
	if frames[0].ReqID != "REQ-1" || frames[1].ReqID != "REQ-2" {
		t.Errorf("bubbles rode req_ids %q, %q — each round must answer its own callback", frames[0].ReqID, frames[1].ReqID)
	}
	if frames[0].ID == frames[1].ID {
		t.Error("both bubbles share a stream id; the second frame would repaint the first bubble")
	}
	if frames[1].Content != streamThinkingPlaceholder {
		t.Errorf("the queued bubble opened with %q, want the wordless placeholder", frames[1].Content)
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user also received %v, want no textual receipt at all", got)
	}
	if h, ok := rig.streams.peek(rig.session); !ok || h.ReqID != "REQ-1" {
		t.Errorf("head = %+v (ok=%v), want the running round's REQ-1 untouched", h, ok)
	}
	if rig.streams.depth() != 2 {
		t.Errorf("store depth = %d, want both rounds open", rig.streams.depth())
	}
}

// TestEveryQueuedRoundGetsItsOwnBubble — the second message and the fifth are
// different rounds, and each one's arrival is acknowledged the same way. The
// old design told the user once and went silent on everyone after.
func TestEveryQueuedRoundGetsItsOwnBubble(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	for _, reqID := range []string{"REQ-2", "REQ-3", "REQ-4"} {
		clock.advance(time.Minute)
		rig.ingest(t, reqID)
	}

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 4 {
		t.Fatalf("wrote %d bubbles, want one per round", len(frames))
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v, want bubbles only", got)
	}
}

// TestAGroupChatsQueuedMessageGetsABubbleToo — the bubble carries no part of
// the run (a group bubble stays on the closed tier), so the rule that keeps
// the step list out of a room does not keep the receipt out. And because the
// bubble is per message, the SECOND asker in a room is acknowledged too — the
// per-session latch used to leave them with nothing.
func TestAGroupChatsQueuedMessageGetsABubbleToo(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.typing.OnIngested(context.Background(), rig.inst, groupInbound("REQ-1", "R-room", "T-bob"), rig.session)
	if h, ok := rig.streams.peek(rig.session); !ok || h.Level != progressLevelNone {
		t.Fatalf("handle = %+v (ok=%v), want a group bubble on the closed tier", h, ok)
	}

	clock.advance(2 * time.Minute)
	rig.typing.OnIngested(context.Background(), rig.inst, groupInbound("REQ-2", "R-room", "T-carol"), rig.session)

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("the room saw %d bubbles, want one per asker's round", len(frames))
	}
	if frames[1].ReqID != "REQ-2" {
		t.Errorf("the second bubble rode %q, want the second asker's REQ-2", frames[1].ReqID)
	}
}

// ---- the case that must not regress ----

// TestASecondMessageInTheDebounceWindowSaysNothing — two messages typed one
// after the other are one run and one bubble. A second bubble there would be
// two spinners for one answer, and one of them would never close.
func TestASecondMessageInTheDebounceWindowSaysNothing(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	clock.advance(sameRoundWindow - time.Second)
	rig.ingest(t, "REQ-2")

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Fatalf("want one bubble for the window, got %d", got)
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v; both messages are one run", got)
	}
	if h, _ := rig.streams.peek(rig.session); h.ReqID != "REQ-1" {
		t.Errorf("handle req_id = %q, want the first message's REQ-1", h.ReqID)
	}
}

// TestASlowBurstIsStillOneRound is why the gap and not the bubble's age
// decides. The batcher re-arms on every message, so four messages two seconds
// apart are one run however old the bubble has grown — an age threshold would
// call the last three a queue and open three bubbles nobody answers.
func TestASlowBurstIsStillOneRound(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	for _, reqID := range []string{"REQ-2", "REQ-3", "REQ-4"} {
		clock.advance(sameRoundWindow - time.Second)
		rig.ingest(t, reqID)
	}

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Fatalf("a re-arming burst opened %d bubbles, want 1", got)
	}
}

// ---- the store's own rules ----

// TestQueuedBehindIsRecordedOnTheLaterRound — the store marks which rounds
// spent their life in a queue, because their empty finish reads differently:
// "handled with the previous reply" instead of "nothing to say".
func TestQueuedBehindIsRecordedOnTheLaterRound(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})
	clock.advance(sameRoundWindow)
	store.open(session, streamHandle{ReqID: "R2", StreamID: "S2"})

	first, ok := store.takeHead(session, roundOver)
	if !ok || first.QueuedBehind {
		t.Fatalf("first round = %+v (ok=%v), want QueuedBehind=false", first, ok)
	}
	second, ok := store.takeHead(session, roundOver)
	if !ok || !second.QueuedBehind {
		t.Fatalf("second round = %+v (ok=%v), want QueuedBehind=true", second, ok)
	}
}

// TestTakeTaskPicksTheAdoptedRound — with two bubbles open, the ending that
// names a run must seal that run's bubble and no other.
func TestTakeTaskPicksTheAdoptedRound(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})
	clock.advance(sameRoundWindow)
	store.open(session, streamHandle{ReqID: "R2", StreamID: "S2"})

	if _, _, ok := store.feedFor(session, "task-1"); !ok {
		t.Fatal("the running head must adopt the first run to speak")
	}
	h, ok := store.takeTask(session, "task-1", roundOver)
	if !ok || h.StreamID != "S1" {
		t.Fatalf("takeTask(task-1) = %+v (ok=%v), want the adopted S1", h, ok)
	}
	if head, ok := store.peek(session); !ok || head.StreamID != "S2" {
		t.Errorf("head after the take = %+v (ok=%v), want the queued round promoted", head, ok)
	}
}

// TestAnUnadoptedHeadFallsToTheNamedTask — a run can finish before any of its
// progress reached the bubble. The head is the running round by serialization,
// so a task nobody adopted still seals it.
func TestAnUnadoptedHeadFallsToTheNamedTask(t *testing.T) {
	store := newStreamStore()
	session := uuidOf(3)
	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})

	h, ok := store.takeTask(session, "task-9", roundOver)
	if !ok || h.StreamID != "S1" {
		t.Fatalf("takeTask on an unadopted head = %+v (ok=%v), want S1", h, ok)
	}
}

// TestAHeadAdoptedByAnotherRunRefusesAForeignTask — the converse. If the head
// belongs to run A, run B's ending may not seal it; B's bubble is already gone
// and its ending belongs on the notes path.
func TestAHeadAdoptedByAnotherRunRefusesAForeignTask(t *testing.T) {
	store := newStreamStore()
	session := uuidOf(3)
	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})
	if _, _, ok := store.feedFor(session, "task-A"); !ok {
		t.Fatal("adoption failed")
	}

	if h, ok := store.takeTask(session, "task-B", roundOver); ok {
		t.Fatalf("takeTask(task-B) sealed %+v, want a refusal — that bubble is task-A's", h)
	}
}

// TestAQueuedBubbleMustNotAdoptAGuardClosedRun — the five-minute guard closes
// the running round's bubble while its run carries on publishing progress. The
// queued bubble next in line must not adopt that run and start painting the
// previous question's tool calls as its own — and the run's ENDING must not
// seize that bubble either: takeTask's unadopted-head fallback carries the
// same fence, so the ending falls through to the plain-message path.
func TestAQueuedBubbleMustNotAdoptAGuardClosedRun(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})
	if _, _, ok := store.feedFor(session, "task-A"); !ok {
		t.Fatal("adoption failed")
	}
	clock.advance(sameRoundWindow)
	store.open(session, streamHandle{ReqID: "R2", StreamID: "S2"})

	// The guard fires on the running bubble; its run continues.
	if _, ok := store.takeStream(session, "S1", roundContinues); !ok {
		t.Fatal("the guard could not take its own bubble")
	}

	if _, _, ok := store.feedFor(session, "task-A"); ok {
		t.Fatal("the queued bubble adopted the guard-closed run's progress")
	}
	if h, ok := store.takeTask(session, "task-A", roundOver); ok {
		t.Fatalf("takeTask(task-A) sealed %+v — the guard-closed run's ending stole the queued bubble", h)
	}
	if _, _, ok := store.feedFor(session, "task-B"); !ok {
		t.Fatal("the queued bubble must still be free for its own run")
	}
	if h, ok := store.takeTask(session, "task-B", roundOver); !ok || h.StreamID != "S2" {
		t.Fatalf("takeTask(task-B) = %+v (ok=%v), want the queued round's own S2", h, ok)
	}
}

// TestAWordlessGuardCloseStillFencesItsRun — the harder half. The guard closes
// a round NO run ever adopted (it died waiting, wordless), so there is no task
// id to fence by. Serialization says the first unseen run to speak is that
// round's — it must neither adopt the next bubble nor seize it as an ending,
// and once seen, its id is on file for the rest of its events.
func TestAWordlessGuardCloseStillFencesItsRun(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	// Round 1 waits so long its bubble dies unadopted; round 2 queues behind.
	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1"})
	clock.advance(sameRoundWindow)
	store.open(session, streamHandle{ReqID: "R2", StreamID: "S2"})
	if _, ok := store.takeStream(session, "S1", roundContinues); !ok {
		t.Fatal("the guard could not take the wordless bubble")
	}

	// Round 1's run finally speaks. It is unseen — and must be recognised as
	// the dead round's, not adopted by round 2's bubble.
	if _, _, ok := store.feedFor(session, "task-A"); ok {
		t.Fatal("the dead round's run was adopted by the next bubble")
	}
	// From here its id is stamped: later events hit the exact-id fence, and
	// its ending may not seize the queued bubble either.
	if _, _, ok := store.feedFor(session, "task-A"); ok {
		t.Fatal("the stamped fence did not hold on the second event")
	}
	if h, ok := store.takeTask(session, "task-A", roundOver); ok {
		t.Fatalf("takeTask(task-A) sealed %+v, want the plain-message path", h)
	}
	// Round 2's own run is unaffected.
	if _, _, ok := store.feedFor(session, "task-B"); !ok {
		t.Fatal("the queued bubble must still be free for its own run")
	}
}

// TestAnAnswersNoteMustNotEraseAGuardPromise — one note per session, and the
// guard's "I'll reply separately" is a debt. Another round finishing must not
// overwrite it, or the promised failure notice dies with it.
func TestAnAnswersNoteMustNotEraseAGuardPromise(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	// Round A adopts its run, guard-closes owed.
	store.open(session, streamHandle{ReqID: "R1", StreamID: "S1", ChatID: "T-alex"})
	if _, _, ok := store.feedFor(session, "task-A"); !ok {
		t.Fatal("adoption failed")
	}
	clock.advance(sameRoundWindow)
	store.open(session, streamHandle{ReqID: "R2", StreamID: "S2", ChatID: "T-alex"})
	if _, ok := store.takeStream(session, "S1", roundContinues); !ok {
		t.Fatal("guard take failed")
	}

	// Round B answers normally (its take files a roundOver note attempt).
	if _, ok := store.takeTask(session, "task-B", roundOver); !ok {
		t.Fatal("round B's answer could not take its bubble")
	}

	// Run A's failure must still find the owed promise.
	if _, verdict := store.claimEnding(session); verdict != roundOwesAnEnding {
		t.Fatalf("claimEnding = %v, want the guard's still-owed promise to survive B's ending", verdict)
	}
}

// ---- the socket ----

// TestTwoConcurrentStreamsOnOneSocketKeepTheirOwnVerdicts — a queued round's
// opening frame rides its own req_id while the running round's frame is still
// unacked. The sender's backpressure and ack routing are per req_id, so
// neither stream may block the other, and out-of-order acks must each land on
// their own caller — this is the wire-level fact the whole one-bubble-per-
// round design stands on.
func TestTwoConcurrentStreamsOnOneSocketKeepTheirOwnVerdicts(t *testing.T) {
	conn := &recordingConn{}
	sender := newWSSender(conn, testLogger())

	first := make(chan error, 1)
	go func() {
		first <- sender.respondStream(context.Background(), "REQ-1", "S-1", streamThinkingPlaceholder, false)
	}()
	waitForStreamFrames(t, conn, 1)

	second := make(chan error, 1)
	go func() {
		second <- sender.respondStream(context.Background(), "REQ-2", "S-2", streamThinkingPlaceholder, false)
	}()
	waitForStreamFrames(t, conn, 2)

	select {
	case err := <-second:
		if errors.Is(err, errStreamBusy) {
			t.Fatal("the queued round's opening frame was refused as busy — backpressure must be per req_id, not per socket")
		}
		t.Fatalf("the second frame returned before its ack: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	// Acks arrive out of order, and each must land on its own caller.
	sender.deliverAck("REQ-2", 0, "")
	if err := <-second; err != nil {
		t.Fatalf("REQ-2's own verdict was ok, got %v", err)
	}
	sender.deliverAck("REQ-1", errcodeStreamExpired, "stream expired")
	if err := <-first; !streamUnusable(err) {
		t.Fatalf("REQ-1's verdict was the server's refusal, got %v", err)
	}
}

// TestTheWindowIsTheEnginesDebounceWindow — the threshold is not a number of
// its own. It is the batcher's silence window, so a change to the debounce
// cannot leave this side behind.
func TestTheWindowIsTheEnginesDebounceWindow(t *testing.T) {
	if sameRoundWindow != engine.DefaultChatRunBatchWindow {
		t.Fatalf("sameRoundWindow = %v, want the engine's debounce window %v",
			sameRoundWindow, engine.DefaultChatRunBatchWindow)
	}
}
