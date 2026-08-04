package wecom

// regression_split_think_literal_test.go — the thinking panel has to stay
// wrapped around the whole bubble even when the agent spells </think> across
// two transcript flushes.
//
// The progress bubble IS a <think> block. Everything the WeCom client folds
// away sits between the wrapper's own tags, and whatever follows the first
// closing tag renders in the chat as the bot's answer, with no edit and no
// unsend to take it back. The agent's reasoning is the one text in that block
// it wrote freely, and it arrives as 500ms increments cut wherever the
// batching happened to fall. Neutralising the literal one increment at a time
// is not enough: "</th" and "ink>" each look harmless on their own and are a
// live closing tag the moment the buffer joins them. This file guards the
// joined buffer, which is what the user actually reads.

import (
	"strings"
	"testing"
)

// TestReasoningSplitAcrossFlushesCannotCloseTheThinkingPanel — if this goes
// red, a person asking a question in WeCom watches the thinking fold snap shut
// mid-run and the rest of the agent's reasoning appear as the reply: the
// premises of their own question, restated, sent into the chat as an answer.
//
// The existing guard test feeds a whole </think> in one increment, which is
// exactly the case the per-increment escape already handles. The transcript
// does not promise increment boundaries anywhere in particular, so an agent
// reasoning about this very feature can land one across the literal.
func TestReasoningSplitAcrossFlushesCannotCloseTheThinkingPanel(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	// One sentence of reasoning, cut where the batch fell: the closing literal
	// is half in this flush and half in the next.
	const head = "气泡外层是 <think> 包起来的，所以增量里的 </th"
	const tail = "ink> 必须先处理掉，否则面板会提前收起来。"

	bus.Publish(taskMessageEvent(chatTaskID(), thinking(head)))
	clock.advance(progressMinInterval)
	bus.Publish(taskMessageEvent(chatTaskID(), thinking(tail)))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) < 3 {
		t.Fatalf("want the opening frame and both thinking refreshes, got %d: %+v", len(frames), frames)
	}
	body := frames[len(frames)-1].Content

	const closing = "</think>"
	first := strings.Index(body, closing)
	if first < 0 {
		t.Fatalf("frame = %q has no thinking wrapper at all", body)
	}
	if want := len(body) - len(closing); first != want {
		t.Errorf("the thinking panel closes %d bytes early; everything after it —\n\t%q\n"+
			"— leaves the fold and renders in the chat as the bot's answer.\nwhole frame:\n%s",
			want-first, body[first+len(closing):], body)
	}
	if n := strings.Count(body, closing); n != 1 {
		t.Errorf("frame carries %d closing tags, want exactly the wrapper's own:\n%s", n, body)
	}

	// A fix that drops the reasoning rather than defusing the literal is not a
	// fix — the panel exists to show the user what the agent is working out.
	if !strings.Contains(body, "包起来的") || !strings.Contains(body, "必须先处理掉") {
		t.Errorf("frame = %q lost the reasoning it was defusing the tag out of", body)
	}
}
