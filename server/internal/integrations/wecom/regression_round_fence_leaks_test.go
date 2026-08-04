package wecom

// regression_round_fence_leaks_test.go — guards the rule that a question's
// bubble belongs to that question's run, in a chat that has been going for a
// while.
//
// Two pieces of bookkeeping decide who may write into an open bubble: the
// session's ended-note, which fences off a run whose own bubble is already
// gone, and the unadopted-debt counter, which stands in for that fence when the
// guard closed a bubble before its run ever spoke. Both are per session and
// both outlive the round that filed them, so both can be spent on the wrong
// round — and when they are, the user sees it. Three leaks are pinned here: a
// stale note that lets a long-finished run seize a new bubble; a debt that is
// never settled because the guard closed the last round the session had, which
// costs the next question both its progress and its answer; and that same debt
// accruing again and again, so that a run of slow questions breaks as many of
// the questions that follow.
//
// Everything is driven through the real path — a message ingested, the run's
// transcript on the bus, the answer published as chat:done — because the
// symptom is what lands in the chat, not what the counters hold. The existing
// guard and queue tests all keep a SECOND round open across the guard's close,
// which is exactly the case where the bookkeeping settles itself; none of them
// follow a session past two rounds.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// bubbleFrames pulls every frame written to one bubble, in order: the opening
// frame the question painted, the refreshes, and whatever sealed it.
func bubbleFrames(frames []streamView, streamID string) []streamView {
	var out []streamView
	for _, f := range frames {
		if f.ID == streamID {
			out = append(out, f)
		}
	}
	return out
}

// askAndOpen ingests one question — carrying its own callback req_id, the way
// every real message does — and returns the stream id of the bubble it opened.
// A question that opens no bubble of its own is a broken setup, not a finding,
// so it fails here rather than downstream.
func askAndOpen(t *testing.T, rig *streamRig, reqID string) string {
	t.Helper()
	seen := map[string]bool{}
	for _, f := range streamViews(t, &rig.conn.recordingConn) {
		seen[f.ID] = true
	}
	rig.ingest(t, reqID)
	var opened []string
	for _, f := range streamViews(t, &rig.conn.recordingConn) {
		if !seen[f.ID] {
			seen[f.ID] = true
			opened = append(opened, f.ID)
		}
	}
	if len(opened) != 1 {
		t.Fatalf("setup: %q opened %d bubbles, want exactly one", reqID, len(opened))
	}
	return opened[0]
}

// sealedWith reports the content of the frame that closed a bubble, and whether
// anything closed it. An unsealed bubble is a spinner that never stops.
func sealedWith(frames []streamView, streamID string) (string, bool) {
	for _, f := range bubbleFrames(frames, streamID) {
		if f.Finish {
			return f.Content, true
		}
	}
	return "", false
}

// TestAnEarlierQuestionsWorkNeverAppearsInALaterQuestionsBubble — the session's
// note carries ONE run id as the fence that keeps a finished round's trailing
// transcript out of the next round's bubble, and every ending overwrites it. So
// the fence only ever protects against the run that answered LAST.
//
// What breaks for a person: they ask a third question, and the loading bubble
// under it starts listing files and commands from the first question — work
// that finished two answers ago, read as what is happening right now. Their
// third question's own run is then locked out of the bubble it opened, so its
// answer arrives as a loose message underneath, and the bubble keeps spinning
// until the five-minute guard replaces it with "still working, I'll reply
// separately" — a promise made after the answer was already on screen. WeCom
// has no edit and no unsend, so all of it stands.
func TestAnEarlierQuestionsWorkNeverAppearsInALaterQuestionsBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	out := NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger())

	runOne := uuidText(uuidOf(61))
	runTwo := uuidText(uuidOf(62))
	runThree := uuidText(uuidOf(63))

	// First question: asked, worked on, answered in its own bubble.
	askAndOpen(t, rig, "REQ-1")
	bus.Publish(taskMessageEvent(runOne, toolUse("Read", map[string]any{"file_path": "round-one.go"})))
	out.handleEvent(chatDoneFromRun(rig.session, runOne, "答案一"))

	// Second question, minutes later: the same, and its ending is the one the
	// session now remembers.
	clock.advance(2 * time.Minute)
	askAndOpen(t, rig, "REQ-2")
	bus.Publish(taskMessageEvent(runTwo, toolUse("Read", map[string]any{"file_path": "round-two.go"})))
	out.handleEvent(chatDoneFromRun(rig.session, runTwo, "答案二"))
	if rig.streams.depth() != 0 {
		t.Fatalf("setup: store depth %d, want both answered rounds closed", rig.streams.depth())
	}

	// Third question. Its bubble opens at ingest, before its run exists, so it
	// has adopted nobody yet.
	clock.advance(2 * time.Minute)
	third := askAndOpen(t, rig, "REQ-3")

	// The first question's transcript is still being flushed in arrears — the
	// daemon posts a run's messages after the fact, and they resolve to the same
	// chat session.
	bus.Publish(taskMessageEvent(runOne, toolUse("Read", map[string]any{"file_path": "still-round-one.go"})))

	for _, f := range bubbleFrames(streamViews(t, &rig.conn.recordingConn), third) {
		if strings.Contains(f.Content, "still-round-one.go") {
			t.Errorf("the third question's bubble is showing the FIRST question's work: %q", f.Content)
		}
	}

	// And the third question's own run must still own the bubble it opened.
	out.handleEvent(chatDoneFromRun(rig.session, runThree, "答案三"))
	content, sealed := sealedWith(streamViews(t, &rig.conn.recordingConn), third)
	if !sealed {
		t.Errorf("the third question's bubble was never sealed — it spins until the guard promises a reply that already arrived")
	} else if content != "答案三" {
		t.Errorf("the third question's bubble was sealed with %q, want its own answer", content)
	}
	for _, got := range contentsOf(&rig.conn.recordingConn) {
		if got == "答案三" {
			t.Errorf("the third answer was sent as a loose message under a still-spinning bubble, not into it")
		}
	}
}

// TestAQuestionAskedAfterALongOneStillShowsItsOwnProgress — when the guard
// closes a bubble no run had spoken for yet, the store counts the round instead
// of naming it, and that count is meant to be spent by the round's own run.
// With no other round open there is nothing left for that run to reach, so both
// settlers return early and the count survives the run it was standing in for.
// The next question's run then pays it.
//
// What breaks for a person: the first question was slow, and they were told a
// reply is coming separately. They ask a second question. Its bubble opens and
// then never moves — none of the work done for it reaches the screen — because
// the store believes that run belongs to the round it already closed.
func TestAQuestionAskedAfterALongOneStillShowsItsOwnProgress(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.typing.guardAfter = 40 * time.Millisecond

	slowRun := uuidText(uuidOf(71))
	nextRun := uuidText(uuidOf(72))

	// A question whose run takes longer than the stream window and has not
	// posted a single transcript message by the time the guard fires.
	askAndOpen(t, rig, "REQ-slow")
	sealed := waitForSealedBubbles(t, rig, 1)
	if sealed[0].Content != copyPacks[LocaleZhHans].StreamStillWorking {
		t.Fatalf("setup: the bubble was sealed with %q, want the guard's promise", sealed[0].Content)
	}
	if rig.streams.depth() != 0 {
		t.Fatalf("setup: store depth %d, want the guard to have taken the session's only bubble", rig.streams.depth())
	}
	rig.typing.guardAfter = 0 // the rest of this test is about what comes next

	// That run then dies, which is the last anyone hears of it.
	failTheNamedRun(bus, rig.session, slowRun)

	// A new question, minutes later, with a run of its own.
	clock.advance(2 * time.Minute)
	second := askAndOpen(t, rig, "REQ-next")
	bus.Publish(taskMessageEvent(nextRun, toolUse("Read", map[string]any{"file_path": "second-question.go"})))

	painted := false
	for _, f := range bubbleFrames(streamViews(t, &rig.conn.recordingConn), second) {
		if strings.Contains(f.Content, "second-question.go") {
			painted = true
		}
	}
	if !painted {
		t.Errorf("the new question's bubble never showed its OWN run's work — the store spent the previous round's debt on it and fenced it out of the bubble it opened")
	}
}

// TestAQuestionAskedAfterALongOneIsStillAnsweredInItsOwnBubble — the same leak,
// followed to the end of the round. A run that finishes without posting any
// transcript at all spends the stale debt on its ANSWER, so the answer cannot
// seal the bubble its own question opened.
//
// What breaks for a person: they ask, a bubble appears, and the answer shows up
// as a separate message below a bubble that goes on spinning. In a chat with no
// edit and no unsend, the spinner is then sealed minutes later with "still
// working, I'll reply separately" — under an answer they have already read.
func TestAQuestionAskedAfterALongOneIsStillAnsweredInItsOwnBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	out := NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger())
	rig.typing.guardAfter = 40 * time.Millisecond

	slowRun := uuidText(uuidOf(81))
	nextRun := uuidText(uuidOf(82))

	askAndOpen(t, rig, "REQ-slow")
	if sealed := waitForSealedBubbles(t, rig, 1); sealed[0].Content != copyPacks[LocaleZhHans].StreamStillWorking {
		t.Fatalf("setup: the bubble was sealed with %q, want the guard's promise", sealed[0].Content)
	}
	rig.typing.guardAfter = 0
	failTheNamedRun(bus, rig.session, slowRun)

	// The next question is answered straight away, with nothing to show in
	// between — a short run, or one whose transcript never made it out.
	clock.advance(2 * time.Minute)
	second := askAndOpen(t, rig, "REQ-next")
	out.handleEvent(chatDoneFromRun(rig.session, nextRun, "答案是 42"))

	content, sealed := sealedWith(streamViews(t, &rig.conn.recordingConn), second)
	if !sealed {
		t.Errorf("the new question's bubble was never sealed — its own answer could not close it, so it spins on under the reply")
	} else if content != "答案是 42" {
		t.Errorf("the new question's bubble was sealed with %q, want its own answer", content)
	}
	for _, got := range contentsOf(&rig.conn.recordingConn) {
		if got == "答案是 42" {
			t.Errorf("the answer was delivered as a loose message instead of into the bubble the question opened")
		}
	}
}

// TestOneSlowQuestionAfterAnotherDoesNotBreakEveryQuestionThatFollows — the
// debt is per session and nothing ever clears it while the session stays busy,
// so the harm is not one round deep. Three slow questions leave three of them,
// and the next three questions each pay one.
//
// What breaks for a person: a bad afternoon on a slow agent poisons the rest of
// the conversation. Every question after it gets a bubble that never becomes
// its answer, one for one, with no way for the user to tell why or to reset it
// short of a new chat session.
func TestOneSlowQuestionAfterAnotherDoesNotBreakEveryQuestionThatFollows(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	out := NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger())
	rig.typing.guardAfter = 40 * time.Millisecond

	// Three questions in a row whose runs say nothing before the guard closes
	// their bubbles, and then die. Each rides its own callback's req_id, the
	// way each real message does.
	for i := 0; i < 3; i++ {
		askAndOpen(t, rig, fmt.Sprintf("REQ-slow-%d", i))
		waitForSealedBubbles(t, rig, i+1)
		failTheNamedRun(bus, rig.session, uuidText(uuidOf(byte(91+i))))
		clock.advance(2 * time.Minute)
	}
	rig.typing.guardAfter = 0

	// Three ordinary questions afterwards, each answered by a run of its own.
	answered := 0
	for i := 0; i < 3; i++ {
		bubble := askAndOpen(t, rig, fmt.Sprintf("REQ-ok-%d", i))
		answer := "答案 " + string(rune('A'+i))
		out.handleEvent(chatDoneFromRun(rig.session, uuidText(uuidOf(byte(101+i))), answer))
		if content, sealed := sealedWith(streamViews(t, &rig.conn.recordingConn), bubble); sealed && content == answer {
			answered++
		}
		clock.advance(2 * time.Minute)
	}
	if answered != 3 {
		t.Errorf("%d of 3 later questions were answered in their own bubble — the slow rounds left a debt per round and every question after them pays one", answered)
	}
}
