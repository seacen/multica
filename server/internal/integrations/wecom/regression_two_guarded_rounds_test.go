package wecom

// regression_two_guarded_rounds_test.go — guards the promise the five-minute
// guard makes when it has to make it TWICE in one session.
//
// The guard closes a bubble with "still working, I'll reply separately" and
// leaves a note behind, and that note is the only thing that makes the promise
// keepable: the failure arrives ten minutes later with no handle, and the note
// is what tells it the round has not been spoken for yet. A session keeps ONE
// such note. Two questions that both wait past five minutes — a long first run
// with a second question queued behind it — both leave one, and the second
// overwrites the first, so only one debt survives for two promises. The answer
// to the first question then settles that single note, and the second
// question's run can fail without anybody being told: its asker is left with
// "I'll reply separately" and silence for good, in a chat with no other account
// of a failed run (StreamFailed is the only one WeCom ever produces).
//
// Nothing else in the package joins the two halves. The guard tests in
// stream_late_failure_test.go drive one round at a time, and the queued-round
// tests in stream_queued_test.go stop at one guard firing — no test opens two
// rounds and lets both guards fire.

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// waitForSealedBubbles blocks until n bubbles have been closed with a
// finish=true frame, so a test can act on the guards' own goroutines without
// sleeping a fixed time.
func waitForSealedBubbles(t *testing.T, rig *streamRig, n int) []streamView {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var sealed []streamView
		for _, f := range streamViews(t, &rig.conn.recordingConn) {
			if f.Finish {
				sealed = append(sealed, f)
			}
		}
		if len(sealed) >= n {
			return sealed
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d bubbles were sealed; a guard never fired", len(sealed), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// chatDoneFromRun is chatDoneEvent with the run named — which is what
// broadcastChatDone actually publishes, and the only thing that tells two
// rounds of one session apart.
func chatDoneFromRun(sessionID pgtype.UUID, taskID, content string) events.Event {
	return events.Event{
		Type:          protocol.EventChatDone,
		ChatSessionID: uuidText(sessionID),
		Payload: protocol.ChatDonePayload{
			ChatSessionID: uuidText(sessionID),
			TaskID:        taskID,
			Content:       content,
		},
	}
}

// failTheNamedRun publishes what FailTask publishes for a chat run, naming
// which run died.
func failTheNamedRun(bus *events.Bus, sessionID pgtype.UUID, taskID string) {
	bus.Publish(events.Event{
		Type:   protocol.EventTaskFailed,
		TaskID: taskID,
		Payload: map[string]any{
			"chat_session_id": uuidText(sessionID),
			"task_id":         taskID,
		},
	})
}

// TestASecondGuardedRoundStillGetsItsFailureAfterTheFirstIsAnswered — two
// questions, both waiting long enough for the guard to promise each of them a
// separate reply. The first is answered; the second's run then dies.
//
// What breaks for a person when this regresses: they asked twice, were told
// twice that a reply was coming separately, got one answer, and never heard
// another word about the other question. Nothing on their screen distinguishes
// that from a run still going, they cannot tell which of the two the answer
// belonged to, and no later event will ever revisit it — WeCom has no edit and
// no unsend, and StreamFailed is the only "that run did not go through" this
// adapter ever produces.
func TestASecondGuardedRoundStillGetsItsFailureAfterTheFirstIsAnswered(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	rig.typing.guardAfter = 40 * time.Millisecond

	runA := uuidText(uuidOf(51))
	runB := uuidText(uuidOf(52))

	// The first question starts a run that will outlive its bubble, and the
	// run speaks so the round is its.
	rig.ingest(t, "REQ-A")
	bus.Publish(taskMessageEvent(runA, toolUse("Read", map[string]any{"file_path": "one.go"})))

	// The second question lands two minutes later: past the debounce window, so
	// a round of its own, queued behind the run in flight and given its own
	// bubble straight away.
	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-B")
	if depth := rig.streams.depth(); depth != 2 {
		t.Fatalf("setup: store depth %d, want a bubble open for each question", depth)
	}

	// Both bubbles reach the guard. Each is sealed with the same promise, and
	// both runs carry on.
	sealed := waitForSealedBubbles(t, rig, 2)
	for i, f := range sealed[:2] {
		if f.Content != copyPacks[LocaleZhHans].StreamStillWorking {
			t.Fatalf("setup: bubble %d was sealed with %q, want the still-working promise", i, f.Content)
		}
	}
	if depth := rig.streams.depth(); depth != 0 {
		t.Fatalf("setup: store depth %d, want both guards to have taken their handles", depth)
	}

	// The first question is answered. Its bubble is long gone, so the answer
	// arrives as a plain message — and it accounts for the first round only.
	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneFromRun(rig.session, runA, "答案是 42"))
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("setup: the user received %v, want the first question's answer", got)
	}

	// The second question's run then dies.
	failTheNamedRun(bus, rig.session, runB)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 2 {
		t.Fatalf("the user received %v — the second question was promised a separate reply, its run died, and nobody ever told them", got)
	}
	if got[1] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("the second question's ending was %q, want the failure notice %q",
			got[1], copyPacks[LocaleZhHans].StreamFailed)
	}
}
