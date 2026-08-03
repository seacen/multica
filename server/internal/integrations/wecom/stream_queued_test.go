package wecom

// stream_queued_test.go — the message that arrives while the last one is still
// running.
//
// One bubble per session is right for a burst and wrong for a queue. The
// difference is the engine's debounce window: messages closer together than
// that are one run and share the bubble, and a message that arrives after it
// starts a round of its own which waits behind the one in flight. The second
// case used to be answered with nothing at all — no bubble, no receipt, no way
// for the user to tell the bot had heard them.

import (
	"context"
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

// ---- the bug ----

// TestAMessageBehindALongRunIsToldItIsQueued is the whole fix. The first run
// has been going for minutes; the second message is a round of its own and
// will wait for it, and the user has to hear that it landed.
func TestAMessageBehindALongRunIsToldItIsQueued(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-2")

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Fatalf("wrote %d bubbles; the running round still owns the only one", got)
	}
	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamQueued {
		t.Fatalf("the user received %v, want the queued receipt", got)
	}
	if h, ok := rig.streams.peek(rig.session); !ok || h.ReqID != "REQ-1" {
		t.Errorf("handle = %+v (ok=%v), want the running round's REQ-1 untouched", h, ok)
	}
}

// TestTheQueuedReceiptGoesOutInEnglishToo — the copy is per installation, and
// a receipt in the wrong language is the one message a new installation is
// guaranteed to see.
func TestTheQueuedReceiptGoesOutInEnglishToo(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.inst.Platform = Installation{Locale: string(LocaleEn)}
	rig.ingest(t, "REQ-1")

	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-2")

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleEn].StreamQueued {
		t.Fatalf("the user received %v, want the English receipt", got)
	}
}

// ---- the case that must not regress ----

// TestASecondMessageInTheDebounceWindowSaysNothing — two messages typed one
// after the other are one run and one bubble. A receipt there would be the bot
// interrupting itself.
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
// call the last three a queue and say so three times.
func TestASlowBurstIsStillOneRound(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	for _, reqID := range []string{"REQ-2", "REQ-3", "REQ-4"} {
		clock.advance(sameRoundWindow - time.Second)
		rig.ingest(t, reqID)
	}

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v; a burst that keeps re-arming the window is one run", got)
	}
}

// ---- not twice ----

// TestTheQueuedReceiptIsSaidOnce — the round is queued once however many
// messages pile onto it. Three receipts for three messages is the bot shouting.
func TestTheQueuedReceiptIsSaidOnce(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")

	for _, reqID := range []string{"REQ-2", "REQ-3", "REQ-4"} {
		clock.advance(time.Minute)
		rig.ingest(t, reqID)
	}

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 {
		t.Fatalf("the user received %v, want exactly one receipt for the queued round", got)
	}
}

// ---- the tiers ----

// TestAGroupChatIsToldItsMessageIsQueued — the receipt carries no part of the
// run, so the rule that keeps the step list out of a room does not apply to
// it. A room that gets nothing back is the same silence the fix is about.
func TestAGroupChatIsToldItsMessageIsQueued(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.typing.OnIngested(context.Background(), rig.inst, groupInbound("REQ-1", "R-room", "T-bob"), rig.session)
	if h, ok := rig.streams.peek(rig.session); !ok || h.Level != progressLevelNone {
		t.Fatalf("handle = %+v (ok=%v), want a group bubble on the closed tier", h, ok)
	}

	clock.advance(2 * time.Minute)
	rig.typing.OnIngested(context.Background(), rig.inst, groupInbound("REQ-2", "R-room", "T-bob"), rig.session)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamQueued {
		t.Fatalf("the room received %v, want the queued receipt", got)
	}
	sends := rig.conn.sends()
	body, _ := sends[0]["body"].(map[string]any)
	if body["chatid"] != "R-room" {
		t.Errorf("receipt addressed to %v, want the room it was asked in", body["chatid"])
	}
	if chatType, _ := body["chat_type"].(float64); int(chatType) != chatTypeGroupInt {
		t.Errorf("chat_type = %v, want the group type %d", body["chat_type"], chatTypeGroupInt)
	}
}

// ---- the socket is down ----

// TestTheQueuedReceiptSurvivesAReconnect — a receipt is worth queueing. It
// goes out through the same holding queue the answers use, so a lease flip
// costs it latency rather than the message; a stream frame in its place would
// simply be lost.
func TestTheQueuedReceiptSurvivesAReconnect(t *testing.T) {
	rig, clock := queuedRig(t)
	rig.ingest(t, "REQ-1")
	rig.senders.clear(rig.inst.ID)

	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-2")

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("wrote %v down a socket that is gone", got)
	}

	reconnected := &recordingConn{}
	rig.senders.set(rig.inst.ID, newWSSender(reconnected, testLogger()))
	rig.senders.flushPending(rig.inst.ID)

	got := contentsOf(reconnected)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamQueued {
		t.Fatalf("the reconnect delivered %v, want the held receipt", got)
	}
}

// ---- the store's own rule ----

// TestFollowUpVerdicts pins the decision itself, away from the sockets: what
// the store answers for a session with no bubble, for a message inside the
// window, and for the ones after the receipt has gone out.
func TestFollowUpVerdicts(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	if got := store.followUp(session); got != followUpNoBubble {
		t.Fatalf("followUp with nothing on file = %v, want followUpNoBubble", got)
	}
	if !store.claim(session, streamHandle{ReqID: "R", StreamID: "S"}) {
		t.Fatal("claim on an empty store must take")
	}

	clock.advance(sameRoundWindow - time.Millisecond)
	if got := store.followUp(session); got != followUpSameRound {
		t.Fatalf("followUp inside the window = %v, want followUpSameRound", got)
	}
	clock.advance(sameRoundWindow)
	if got := store.followUp(session); got != followUpQueued {
		t.Fatalf("followUp past the window = %v, want followUpQueued", got)
	}
	clock.advance(sameRoundWindow)
	if got := store.followUp(session); got != followUpToldAlready {
		t.Fatalf("a second followUp past the window = %v, want followUpToldAlready", got)
	}
}

// TestANewBubbleStartsTheReceiptOver — the latch belongs to the bubble, not to
// the session. The next round is entitled to its own receipt.
func TestANewBubbleStartsTheReceiptOver(t *testing.T) {
	store := newStreamStore()
	clock := newTestClock()
	store.now = clock.now
	session := uuidOf(3)

	store.claim(session, streamHandle{ReqID: "R", StreamID: "S"})
	clock.advance(sameRoundWindow)
	if got := store.followUp(session); got != followUpQueued {
		t.Fatalf("followUp past the window = %v, want followUpQueued", got)
	}
	store.take(session, roundOver)

	if !store.claim(session, streamHandle{ReqID: "R2", StreamID: "S2"}) {
		t.Fatal("claim after the round ended must take")
	}
	clock.advance(sameRoundWindow)
	if got := store.followUp(session); got != followUpQueued {
		t.Fatalf("followUp on the next round = %v, want followUpQueued", got)
	}
}

// TestTheReceiptWindowIsTheEnginesDebounceWindow — the threshold is not a
// number of its own. It is the batcher's silence window, so a change to the
// debounce cannot leave this side behind.
func TestTheReceiptWindowIsTheEnginesDebounceWindow(t *testing.T) {
	if sameRoundWindow != engine.DefaultChatRunBatchWindow {
		t.Fatalf("sameRoundWindow = %v, want the engine's debounce window %v",
			sameRoundWindow, engine.DefaultChatRunBatchWindow)
	}
}
