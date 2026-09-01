package wecom

// long_reply_test.go — what happens to an answer longer than one WeCom
// message.
//
// aibot caps a single body at 20480 utf8 bytes and refuses anything past it
// WHOLE: it does not clip, it answers 45002 and writes nothing. So a long
// answer used to reach the person in one of two ways, both of them bad. Down
// the plain path it never arrived at all — the frame was refused and the only
// record was a log line. Into a streaming bubble it arrived clipped, ending in
// an ellipsis, with no way to read the rest of it anywhere: WeCom has no edit
// and no unsend, and the tail of a code review or a pasted log is not filler.
//
// The invariant these tests hold the code to is the person's, not the wire's:
// whatever the agent wrote, the person can read all of it in the chat. How
// many messages that takes is an implementation detail; losing any of it is
// the defect.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// capEnforcingConn is the server's own rule: a markdown body past the cap is
// refused with 45002 and never written into the chat. Modelling the refusal
// rather than just recording the write is the point — delivered() returns what
// the person can actually read, so a test cannot pass by writing a frame
// nobody ever saw.
type capEnforcingConn struct {
	mu     sync.Mutex
	frames []frameEnvelope
	seen   []string // contents of the frames the server accepted
	sender *wsSender
}

func (c *capEnforcingConn) WriteMessage(_ int, data []byte) error {
	var env frameEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	content := markdownContentOf(env)
	code, msg := 0, ""
	if len(content) > sendMsgContentLimit {
		code, msg = 45002, "content exceed max length"
	}
	c.mu.Lock()
	c.frames = append(c.frames, env)
	if code == 0 && env.Cmd == cmdSendMsg {
		c.seen = append(c.seen, content)
	}
	s := c.sender
	c.mu.Unlock()
	if s != nil {
		s.routeResponse(frameEnvelope{Headers: frameHeaders{ReqID: env.Headers.ReqID}, ErrCode: code, ErrMsg: msg})
	}
	return nil
}

func (c *capEnforcingConn) ReadMessage() (int, []byte, error) { return 0, nil, nil }
func (c *capEnforcingConn) SetReadDeadline(time.Time) error   { return nil }
func (c *capEnforcingConn) SetWriteDeadline(time.Time) error  { return nil }
func (c *capEnforcingConn) Close() error                      { return nil }

// delivered is everything the person can read, in the order it arrived.
func (c *capEnforcingConn) delivered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string{}, c.seen...)
}

// markdownContentOf pulls the body text out of an aibot_send_msg frame.
func markdownContentOf(env frameEnvelope) string {
	if env.Cmd != cmdSendMsg {
		return ""
	}
	var body map[string]any
	if json.Unmarshal(env.Body, &body) != nil {
		return ""
	}
	md, _ := body["markdown"].(map[string]any)
	if md == nil {
		return ""
	}
	s, _ := md["content"].(string)
	return s
}

// pieceMarker is the continuation counter splitForWire appends. It is stripped
// before reassembly because it is the adapter's word, not the agent's.
var pieceMarker = regexp.MustCompile(`\n\n\(\d+/\d+\)$`)

func reassemble(pieces []string) string {
	var b strings.Builder
	for _, p := range pieces {
		b.WriteString(pieceMarker.ReplaceAllString(p, ""))
	}
	return b.String()
}

// aLongAnswer is a body over two frames' worth with no line breaks in it, so
// the split falls on a rune boundary and reassembly is byte-exact. Multi-byte
// runes on purpose: a cut through one would corrupt the text either side of it.
func aLongAnswer() string {
	return strings.Repeat("答案很长，这是第一段。", sendMsgContentLimit/10)
}

// TestALongAnswerReachesTheChatWhole is the plain path — no bubble, the way
// every reply arrived before streaming and the way one still arrives when the
// bubble is gone. WeCom refuses the oversized frame outright, so without a
// split the person asks a question and gets nothing back at all.
func TestALongAnswerReachesTheChatWhole(t *testing.T) {
	t.Parallel()
	conn := &capEnforcingConn{}
	sender := newWSSender(conn, nil)
	conn.sender = sender

	answer := aLongAnswer()
	if err := sender.sendTextCtx(context.Background(), "CHAT_1", chatTypeSingleInt, answer); err != nil {
		t.Fatalf("sending a %d-byte answer failed: %v", len(answer), err)
	}

	got := conn.delivered()
	if len(got) == 0 {
		t.Fatalf("a %d-byte answer produced nothing the person can read: WeCom refuses a body past %d bytes "+
			"whole, so the question went unanswered and the only record is a log line",
			len(answer), sendMsgContentLimit)
	}
	for i, piece := range got {
		if len(piece) > sendMsgContentLimit {
			t.Fatalf("piece %d is %d bytes, past the %d-byte cap the server refuses — including its own continuation marker",
				i+1, len(piece), sendMsgContentLimit)
		}
	}
	if whole := reassemble(got); whole != answer {
		t.Fatalf("the person can read %d bytes of a %d-byte answer; %d bytes of what the agent wrote never reached the chat",
			len(whole), len(answer), len(answer)-len(whole))
	}
}

// TestAShortAnswerIsUntouched: splitting must cost the ordinary reply nothing —
// no extra frame, and above all no counter appended to an answer that is one
// message long.
func TestAShortAnswerIsUntouched(t *testing.T) {
	t.Parallel()
	conn := &capEnforcingConn{}
	sender := newWSSender(conn, nil)
	conn.sender = sender

	const answer = "答案是 42"
	if err := sender.sendTextCtx(context.Background(), "CHAT_1", chatTypeSingleInt, answer); err != nil {
		t.Fatalf("sendTextCtx: %v", err)
	}
	got := conn.delivered()
	if len(got) != 1 || got[0] != answer {
		t.Fatalf("a short answer went out as %q, want exactly [%q]", got, answer)
	}
}

// TestEachPieceSaysTheAnswerContinues: the reader has to know a message is
// part of something longer, or they read the first piece as the whole answer
// and act on half of it. The counter belongs to no language on purpose — it
// needs no translation, and it cannot contradict an answer written in one.
func TestEachPieceSaysTheAnswerContinues(t *testing.T) {
	t.Parallel()
	pieces := splitForWire(aLongAnswer())
	if len(pieces) < 2 {
		t.Fatalf("the fixture did not split: %d piece(s)", len(pieces))
	}
	for i, p := range pieces[:len(pieces)-1] {
		want := "\n\n(" + strconv.Itoa(i+1) + "/" + strconv.Itoa(len(pieces)) + ")"
		if !strings.HasSuffix(p, want) {
			t.Fatalf("piece %d does not end in %q, so the reader has nothing telling them the answer continues; it ends %q",
				i+1, want, tail(p, 12))
		}
	}
	if last := pieces[len(pieces)-1]; pieceMarker.MatchString(last) {
		t.Fatalf("the final piece carries a continuation marker (%q), which promises more that never comes", tail(last, 12))
	}
}

// TestAPieceNeverEndsMidCharacter: a cut through a multi-byte rune shows up in
// the chat as a replacement glyph on both sides of the seam.
func TestAPieceNeverEndsMidCharacter(t *testing.T) {
	t.Parallel()
	for i, p := range splitForWire(aLongAnswer()) {
		if !utf8.ValidString(p) {
			t.Fatalf("piece %d is not valid utf8 — the cut went through a character, "+
				"and the reader sees a replacement glyph on both sides of the seam", i+1)
		}
	}
}

// TestTheCutPrefersALineBoundary: a long answer is usually a log or a code
// block, and a piece that ends mid-line reads far worse than one that ends
// where the text already ended. The preference is bounded — a break near the
// start of the budget would waste most of a message.
func TestTheCutPrefersALineBoundary(t *testing.T) {
	t.Parallel()
	line := strings.Repeat("x", 79) + "\n"
	pieces := splitForWire(strings.Repeat(line, sendMsgContentLimit/len(line)*2+40))
	if len(pieces) < 2 {
		t.Fatalf("the fixture did not split: %d piece(s)", len(pieces))
	}
	for i, p := range pieces[:len(pieces)-1] {
		body := pieceMarker.ReplaceAllString(p, "")
		for j, got := range strings.Split(body, "\n") {
			if len(got) != len(line)-1 {
				t.Fatalf("piece %d line %d is %d characters of a %d-character line — the cut fell mid-line",
					i+1, j+1, len(got), len(line)-1)
			}
		}
	}
}

// TestALongAnswerInABubbleIsReadableToTheEnd is the streaming half, driven
// through the real Outbound over the rig the bubble tests established.
//
// A closing stream frame is capped at the same 20480 bytes, and it is CLIPPED
// to fit rather than refused. That is the worse failure of the two: the answer
// looks complete, ends in an ellipsis, and the rest of it exists nowhere the
// reader can get to.
func TestALongAnswerInABubbleIsReadableToTheEnd(t *testing.T) {
	rig := newBubbleRig(t)
	rig.ask(t, "REQ-LONG", 1)
	rig.runStarted(t, 1, "task-1")

	answer := aLongAnswer()
	rig.answer(t, answer, "task-1")

	// Everything the person can read: what the sealed bubble holds, then the
	// messages underneath it.
	var readable []string
	for _, s := range rig.conn.streamFrames(t) {
		if s["finish"] == true {
			readable = append(readable, s["content"].(string))
		}
	}
	if len(readable) == 0 {
		t.Fatal("the bubble was never sealed, so it spins forever and the answer is nowhere")
	}
	for _, push := range rig.conn.pushes(t) {
		md, _ := push["markdown"].(map[string]any)
		if md == nil {
			continue
		}
		readable = append(readable, md["content"].(string))
	}

	whole := reassemble(readable)
	if strings.Contains(whole, "…") && !strings.Contains(answer, "…") {
		t.Fatalf("the answer was clipped to fit one frame and ends in an ellipsis; "+
			"the remaining %d bytes exist nowhere the reader can reach them", len(answer)-len(whole))
	}
	if whole != answer {
		t.Fatalf("the person can read %d bytes of a %d-byte answer; %d bytes never reached the chat",
			len(whole), len(answer), len(answer)-len(whole))
	}
	for i, part := range readable {
		if len(part) > sendMsgContentLimit {
			t.Fatalf("part %d is %d bytes, past the %d-byte cap", i+1, len(part), sendMsgContentLimit)
		}
	}
}

// TestABubbleThatStillFitsIsNotSplit: the common answer must still be one
// sealed bubble and nothing else. A follow-up message under a short reply
// would be a new defect, not a fix.
func TestABubbleThatStillFitsIsNotSplit(t *testing.T) {
	rig := newBubbleRig(t)
	rig.ask(t, "REQ-SHORT", 1)
	rig.runStarted(t, 1, "task-1")
	rig.answer(t, "答案是 42", "task-1")

	if n := len(rig.conn.pushes(t)); n != 0 {
		t.Fatalf("a short answer produced %d message(s) under the bubble, want none", n)
	}
}

// A run that ends without a bubble still answers, and that answer takes the
// plain path — so the split has to be there too, not only in the bubble
// branch. Driven through the real Outbound with no bubble ever opened.
func TestALongAnswerWithNoBubbleStillArrivesWhole(t *testing.T) {
	rig := newBubbleRig(t)
	answer := aLongAnswer()
	// The run exists; it is the BUBBLE that does not. runStarted is what
	// normally files the row, and this test deliberately never calls it, so
	// the row is filed on its own — otherwise the origin gate reads the run as
	// cancelled and reaped, and the answer this test is about never goes out.
	rig.q.fileTask(t, taskUUID(t, "task-1"))

	if err := rig.out.processEvent(context.Background(), events.Event{
		ChatSessionID: bubbleSession,
		TaskID:        taskUUID(t, "task-1"),
		Payload:       protocol.ChatDonePayload{Content: answer},
	}); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	// The whole answer is one queue row; the split into server-sized pieces
	// happens where the row is written to the socket, so the drain is part of
	// what this test is checking.
	var readable []string
	for _, push := range rig.conn.pushes(t) {
		md, _ := push["markdown"].(map[string]any)
		if md == nil {
			continue
		}
		readable = append(readable, md["content"].(string))
	}
	if len(readable) == 0 {
		t.Fatal("a long answer with no bubble produced no message at all")
	}
	for i, part := range readable {
		if len(part) > sendMsgContentLimit {
			t.Fatalf("message %d is %d bytes, past the %d-byte cap the server refuses whole", i+1, len(part), sendMsgContentLimit)
		}
	}
	if whole := reassemble(readable); whole != answer {
		t.Fatalf("the person can read %d bytes of a %d-byte answer; %d bytes never reached the chat",
			len(whole), len(answer), len(answer)-len(whole))
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// ---- when it breaks in the middle ----
//
// The tests above are about the answer arriving whole. These are about the
// operator's half of the same invariant: what gets recorded when it does not.
//
// A long answer goes out as several messages, and a failure on any but the
// first leaves the ones before it in the chat. WeCom has no unsend, so the
// person is reading the opening of an answer whose remainder exists nowhere,
// and they have no way to know it stops early.
//
// Both routes reach that screen. Under a bubble the head goes into the sealed
// frame and the rest follow as messages; with no bubble left, sendTextCtx
// splits and sends them all. They used to disagree completely about what had
// happened — the bubble route recorded a plain delivered, the message route
// recorded a drop — so the same thing on the person's screen moved opposite
// counters depending on an implementation detail nobody outside can see. An
// operator reading the drop as "resend it" would print the opening twice.
//
// So: delivered, because words did reach the person and the denominator has to
// keep counting them, PLUS truncated, because that is the part neither of the
// other counters can say. Both routes, the same pair.

// truncationRig is a bubbleRig whose Outbound and registry report to one
// counting sink, and whose log the test can read. newBubbleRig wires neither,
// because everything else it is used for is about frames.
func truncationRig(t *testing.T) (*bubbleRig, *countingMetrics, *bytes.Buffer) {
	t.Helper()
	rig := newBubbleRig(t)
	mx := newCountingMetrics()
	logs := &bytes.Buffer{}
	rig.senders.WithMetrics(mx)
	rig.out = NewOutbound(rig.q, rig.senders, rig.streams,
		slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		WithOutboundMetrics(mx))
	return rig, mx, logs
}

// assertTruncated is the pair both routes have to record, stated once.
func assertTruncated(t *testing.T, mx *countingMetrics, logs *bytes.Buffer) {
	t.Helper()
	if got := mx.get("outbound_truncated"); got != 1 {
		t.Errorf("truncated = %d, want 1 — the person is reading part of an answer and "+
			"nothing says so. log:\n%s", got, logs.String())
	}
	if got := mx.get("outbound_delivered"); got != 1 {
		t.Errorf("delivered = %d, want 1 — words DID reach the person, and a truncated reply "+
			"missing from the denominator makes the rate unreadable. log:\n%s", got, logs.String())
	}
	if got := mx.get("outbound_dropped"); got != 0 {
		t.Errorf("dropped = %d, want 0 — the opening of the answer is in the chat, and an "+
			"operator acting on a drop would resend it and print that opening twice", got)
	}
	if !strings.Contains(logs.String(), "only part of a long answer reached the user") {
		t.Errorf("nothing in the log says the answer stopped early:\n%s", logs.String())
	}
}

// TestABubbleAnswerThatBreaksAfterTheBubbleIsCountedTruncated — the bubble
// route. The head seals the bubble, the first message under it is refused.
//
// REVERSE VERIFICATION: make sendRest swallow its error again (drop the
// return, and the o.truncated call in deliverAnswer with it) and this fails on
// truncated = 0 while delivered stays 1 — which is exactly the reading that
// hid the bug: a third of an answer on the screen, filed as a whole reply
// delivered. `go build` and `go vet` are silent either way; so is every other
// test in this package, including the long-answer tests right above, because
// they assert on what the socket carried and not on what was recorded.
func TestABubbleAnswerThatBreaksAfterTheBubbleIsCountedTruncated(t *testing.T) {
	rig, mx, logs := truncationRig(t)
	rig.conn.refusePushesFrom = 1 // nothing under the bubble gets through

	rig.ran(t, "REQ-TRUNC-B", 1, "task-1")
	rig.answer(t, aLongAnswer(), "task-1")

	// The premise: the bubble was sealed and nothing followed it.
	sealed := 0
	for _, s := range rig.conn.streamFrames(t) {
		if s["finish"] == true {
			sealed++
		}
	}
	if sealed != 1 {
		t.Fatalf("the bubble was sealed %d time(s), want 1 — this test's premise is gone", sealed)
	}
	if got := rig.conn.readablePushes(); got != 0 {
		t.Fatalf("%d message(s) landed under the bubble, want 0 — the answer did not break", got)
	}
	assertTruncated(t, mx, logs)
}

// TestAPlainAnswerThatBreaksMidwayIsCountedTruncated — the same screen by the
// other route. No bubble, so the whole answer goes down sendTextCtx, which
// splits it; the server takes the first piece and refuses the second.
//
// Driven through handleEvent rather than processEvent because the drop is
// classified there: an error returned from processEvent is what USED to move
// outbound_dropped for this exact case, and a test calling processEvent
// directly could not tell that apart from no drop at all.
//
// REVERSE VERIFICATION: remove the errors.Is(err, errPartiallySent) branch
// from sendAsMessage and this fails with dropped = 1, delivered = 0,
// truncated = 0 — the old reading, and the opposite of what the sibling test
// above records for the same thing on the person's screen. Build and vet stay
// silent.
func TestAPlainAnswerThatBreaksMidwayIsCountedTruncated(t *testing.T) {
	rig, mx, logs := truncationRig(t)
	rig.conn.refusePushesFrom = 2 // the first piece lands, the second does not
	// The run exists; the BUBBLE does not — the case a restart mid-run leaves
	// behind. Filed directly, the way TestALongAnswerWithNoBubbleStillArrives
	// Whole does, so the origin gate does not read the run as reaped.
	rig.q.fileTask(t, taskUUID(t, "task-1"))

	rig.out.handleEvent(events.Event{
		Type:          protocol.EventChatDone,
		ChatSessionID: bubbleSession,
		TaskID:        taskUUID(t, "task-1"),
		Payload:       protocol.ChatDonePayload{Content: aLongAnswer()},
	})

	if got := rig.conn.readablePushes(); got != 1 {
		t.Fatalf("the person can read %d message(s), want exactly 1 — this test is about an "+
			"answer that broke after the first piece", got)
	}
	assertTruncated(t, mx, logs)
}

// TestALongAnswerThatArrivesWholeIsNotCountedTruncated — the guard. A counter
// that fires on every long answer says nothing about any of them.
func TestALongAnswerThatArrivesWholeIsNotCountedTruncated(t *testing.T) {
	rig, mx, logs := truncationRig(t)

	rig.ran(t, "REQ-TRUNC-OK", 1, "task-1")
	rig.answer(t, aLongAnswer(), "task-1")

	if got := mx.get("outbound_truncated"); got != 0 {
		t.Errorf("truncated = %d on an answer that arrived whole. log:\n%s", got, logs.String())
	}
	if got := mx.get("outbound_delivered"); got != 1 {
		t.Errorf("delivered = %d, want 1", got)
	}
}
