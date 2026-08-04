package wecom

// regression_thinking_seam_test.go — the whitespace at the seam between two
// thinking increments is the entire separator between them.
//
// The agent's reasoning reaches the bubble as 500ms increments of one
// continuous stream, cut wherever the batching fell rather than at a sentence
// or even at a word. Model tokens carry their own boundary spaces, so a flush
// routinely ends mid-phrase with the separating space as its last character,
// and Cursor opens a new reasoning block by prefixing the delta with a blank
// line for exactly the same reason. The buffer is joined by plain
// concatenation, which makes both of those load-bearing: whatever whitespace
// sits at a seam is all that keeps the two sides apart. This file guards the
// joined text as the person in WeCom reads it, across a flush boundary.

import (
	"strings"
	"testing"
)

// TestReasoningCutMidSentenceDoesNotWeldTwoWordsTogether — if this goes red, a
// person watching a run in WeCom reads reasoning with words jammed together at
// every 500ms boundary: "再看router.go" where the agent wrote "再看 router.go".
// It is unreadable in the small, and worse in mixed script, where the fused
// pair reads as one unfamiliar token rather than as two words.
//
// The existing multi-increment test feeds chunks with no whitespace at their
// edges, so it passes whether the seam is preserved or not.
func TestReasoningCutMidSentenceDoesNotWeldTwoWordsTogether(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	// One sentence of reasoning, cut where the batch happened to fall: the
	// space that separates the two words is the last character of the earlier
	// flush, which is where the transcript put it.
	const head = "先看 handler.go 里的分支，再看 "
	const tail = "router.go 的注册顺序。"

	bus.Publish(taskMessageEvent(chatTaskID(), thinking(head)))
	clock.advance(progressMinInterval)
	bus.Publish(taskMessageEvent(chatTaskID(), thinking(tail)))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) < 3 {
		t.Fatalf("want the opening frame and both thinking refreshes, got %d: %+v", len(frames), frames)
	}
	body := frames[len(frames)-1].Content

	// Keeping the seam by dropping the reasoning is not a fix — the panel is
	// there to show the user what the agent is working out.
	if !strings.Contains(body, "里的分支") || !strings.Contains(body, "的注册顺序") {
		t.Fatalf("frame = %q lost the reasoning on one side of the seam", body)
	}

	gap, ok := textBetween(body, "再看", "router.go")
	if !ok {
		t.Fatalf("frame = %q does not carry both halves of the sentence", body)
	}
	if strings.TrimSpace(gap) != "" {
		t.Fatalf("the two markers matched the wrong place; %q sits between them:\n%s", gap, body)
	}
	if gap == "" {
		t.Errorf("the seam between two thinking increments lost its space: the bubble reads %q where the agent wrote %q.\n"+
			"Every 500ms boundary that falls between two words welds them into one.\nwhole frame:\n%s",
			"再看router.go", "再看 router.go", body)
	}
}

// TestTwoReasoningBlocksDoNotRunTogetherAcrossAFlush — if this goes red, a run
// whose reasoning moves on to a new block at the moment a flush lands shows
// the user two blocks concatenated into one unbroken paragraph, with the
// sentence that ended one thought and the sentence that opened the next
// sitting side by side as if they were one.
//
// A reasoning delta announces a new block by opening with a blank line,
// because concatenation is how the buffer is built. That blank line is leading
// whitespace of its increment, and it is the only thing marking the break.
func TestTwoReasoningBlocksDoNotRunTogetherAcrossAFlush(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	// Second block, arriving as its own increment with the separator the
	// provider put in front of it.
	const first = "先确认 handler.go 里的分支。"
	const second = "\n\n再决定要不要改 router.go 的注册顺序。"

	bus.Publish(taskMessageEvent(chatTaskID(), thinking(first)))
	clock.advance(progressMinInterval)
	bus.Publish(taskMessageEvent(chatTaskID(), thinking(second)))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) < 3 {
		t.Fatalf("want the opening frame and both thinking refreshes, got %d: %+v", len(frames), frames)
	}
	body := frames[len(frames)-1].Content

	if !strings.Contains(body, "里的分支") || !strings.Contains(body, "的注册顺序") {
		t.Fatalf("frame = %q lost one of the two reasoning blocks", body)
	}

	gap, ok := textBetween(body, "里的分支。", "再决定")
	if !ok {
		t.Fatalf("frame = %q does not carry both reasoning blocks", body)
	}
	if strings.TrimSpace(gap) != "" {
		t.Fatalf("the two markers matched the wrong place; %q sits between them:\n%s", gap, body)
	}
	if !strings.Contains(gap, "\n") {
		t.Errorf("the break between two reasoning blocks was dropped at the flush boundary: they are separated by %q, so the bubble runs them together as one paragraph.\n"+
			"The agent sent a blank line between them.\nwhole frame:\n%s", gap, body)
	}
}

// textBetween returns what sits between the first occurrence of left and the
// first occurrence of right after it — the seam itself, which is what these
// tests are about.
func textBetween(body, left, right string) (string, bool) {
	i := strings.Index(body, left)
	if i < 0 {
		return "", false
	}
	rest := body[i+len(left):]
	j := strings.Index(rest, right)
	if j < 0 {
		return "", false
	}
	return rest[:j], true
}
