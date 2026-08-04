package wecom

// regression_stream_bookkeeping_overload_test.go — what a busy socket must not
// cost a turn that is still running.
//
// The per-turn bookkeeping in ws_sender.go is what makes a verdict trustworthy:
// an ack frame carries the req_id and nothing else, so the Nth verdict on a
// req_id belongs to its Nth frame and to no other. That accounting is capped, and
// the cap is reached on one installation's socket when enough distinct turns are
// opened inside the stream window. These tests pin what happens to a turn that is
// mid-flight when the cap is hit: its place in its own write order has to survive,
// because losing it hands an earlier frame's "accepted" to the closing frame — and
// a refused answer that reads as delivered is never sent anywhere at all. The user
// watches a spinner that never stops and never learns the reply went missing,
// which is the one failure this adapter is built to make impossible.
//
// Written against the delivery the user sees, not against how the cap is kept:
// raise it, sweep it differently, or refuse the frame that cannot be accounted
// for — any of those is a fix, and none of them should change these tests.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// crowdedConn is the server's half for a socket carrying many turns at once. It
// acks every frame as it is written, except while the test holds one back, and
// it plays the sequence that matters for a closing frame: the verdict still owed
// to an earlier frame of the same turn arrives first — acks come back in the
// order the frames went out — and the closing frame's own verdict, a refusal,
// arrives behind it.
type crowdedConn struct {
	recordingConn
	mu      sync.Mutex
	sender  *wsSender
	holdAck bool
}

func (c *crowdedConn) WriteMessage(t int, data []byte) error {
	if err := c.recordingConn.WriteMessage(t, data); err != nil {
		return err
	}
	var f struct {
		Cmd     string       `json:"cmd"`
		Headers frameHeaders `json:"headers"`
		Body    struct {
			Stream struct {
				Finish bool `json:"finish"`
			} `json:"stream"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	if f.Cmd != cmdRespondMsg {
		return nil
	}
	c.mu.Lock()
	sender, hold := c.sender, c.holdAck
	c.mu.Unlock()
	if sender == nil || hold {
		return nil
	}
	if f.Body.Stream.Finish {
		// The refresh's verdict, which this server sat on until now.
		sender.deliverAck(f.Headers.ReqID, 0, "")
		// The closing frame's own, and the whole point of the test: this
		// bubble is past its window, so the answer has to go out as a plain
		// message instead.
		sender.deliverAck(f.Headers.ReqID, errcodeStreamExpired, "stream expired")
		return nil
	}
	sender.deliverAck(f.Headers.ReqID, 0, "")
	return nil
}

func (c *crowdedConn) hold(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.holdAck = v
}

// crowdTheSocket opens more turns on one sender than its bookkeeping is sized
// for, the way a busy installation does inside the stream window. Frames that
// fail are not the subject — refusing them is one legitimate way to honour the
// cap — so only the traffic matters here, not its verdicts.
func crowdTheSocket(sender *wsSender) {
	for i := 0; i < streamAcksMax+16; i++ {
		_ = sender.respondStream(context.Background(), fmt.Sprintf("REQ-OTHER-%d", i), "S-other", "<think></think>", false)
	}
}

// TestAnAnswerRefusedOnACrowdedSocketStillReachesTheUser is the end this file
// exists for. A turn is running, its bubble is open, and while it runs the socket
// fills with other people's turns. The answer arrives, the server refuses the
// closing frame — that bubble is past its window — and the adapter's whole
// contract is that the refusal sends the answer out as a plain message instead.
//
// When the turn loses its place in its own write order, the closing frame is
// handed the verdict owed to an earlier frame of the same turn, which said
// "accepted". The delivery reports success, the fallback never runs, and the
// answer the agent worked out is sent nowhere: the user is left with a bubble
// that spins forever and no reply in the chat.
func TestAnAnswerRefusedOnACrowdedSocketStillReachesTheUser(t *testing.T) {
	rig := newStreamRig(t)
	conn := &crowdedConn{}
	sender := newWSSender(conn, testLogger())
	conn.mu.Lock()
	conn.sender = sender
	conn.mu.Unlock()
	rig.senders.set(rig.inst.ID, sender)

	// The turn: a question, a bubble, and one progress refresh whose verdict is
	// still on the wire when the answer goes out. That is the ordinary mid-run
	// shape — the refresh budget is shorter than the server's worst case on
	// purpose, so its caller stops waiting and the server answers anyway.
	sender.ackTimeout = 20 * time.Millisecond
	rig.ingest(t, "REQ-42")
	h, ok := rig.streams.peek(rig.session)
	if !ok {
		t.Fatal("setup: no bubble was opened, so there is nothing for the answer to be refused from")
	}
	conn.hold(true)
	if err := rig.senders.stream(context.Background(), h, "<think>正在读取 x.go</think>", false); !errors.Is(err, errStreamAckTimeout) {
		t.Fatalf("setup: the refresh returned %v, want its caller to have given up on the verdict", err)
	}
	conn.hold(false)
	sender.ackTimeout = 2 * time.Second

	// Everyone else's turns, on the same installation's socket.
	crowdTheSocket(sender)

	// The answer.
	const answer = "答案是 42"
	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, answer))

	got := contentsOf(&conn.recordingConn)
	if len(got) != 1 || got[0] != answer {
		t.Fatalf("the server refused the closing frame and the user received %v, want the answer %q as a plain message — "+
			"the reply is nowhere: not in the bubble, which is still spinning, and not in the chat", got, answer)
	}
}

// TestACrowdedSocketDoesNotHandAStaleVerdictToTheAnswer pins the same rule one
// layer down, at the seam every caller reads: the verdict a closing frame comes
// back with must be its own. An earlier frame of the same turn is still owed one,
// and being handed it is what makes a refused answer report success — the caller
// then has no reason to fall back, and nothing is ever sent.
func TestACrowdedSocketDoesNotHandAStaleVerdictToTheAnswer(t *testing.T) {
	conn := &ackingConn{}
	sender := newWSSender(conn, testLogger())
	conn.mu.Lock()
	conn.sender = sender
	conn.silent = true
	conn.mu.Unlock()
	sender.ackTimeout = 20 * time.Millisecond

	// The refresh whose caller gave up. The server will answer it eventually.
	if err := sender.respondStream(context.Background(), "REQ-42", "S-1", "<think>正在读取</think>", false); !errors.Is(err, errStreamAckTimeout) {
		t.Fatalf("setup: the refresh returned %v, want its caller to have given up on the verdict", err)
	}

	conn.mu.Lock()
	conn.silent = false
	conn.mu.Unlock()
	sender.ackTimeout = 2 * time.Second
	crowdTheSocket(sender)
	conn.mu.Lock()
	conn.silent = true // the answer's verdicts are played by hand, in order
	conn.mu.Unlock()

	before := len(framesOf(&conn.recordingConn, cmdRespondMsg))
	answered := make(chan error, 1)
	go func() {
		answered <- sender.respondStream(context.Background(), "REQ-42", "S-1", "答案是 42", true)
	}()
	waitForStreamFrames(t, &conn.recordingConn, before+1)

	// The refresh's verdict, at last. It says the frame was accepted, and it
	// belongs to the refresh and to nothing else.
	sender.deliverAck("REQ-42", 0, "")
	select {
	case err := <-answered:
		t.Fatalf("the answer was handed the refresh's verdict (%v) instead of waiting for its own — "+
			"a closing frame the server refuses now reports success, so the answer is never sent to the user at all", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Its own verdict: a refusal, which is exactly what makes the caller send
	// the answer as a plain message rather than lose it.
	sender.deliverAck("REQ-42", errcodeStreamExpired, "stream expired")
	if err := <-answered; !streamUnusable(err) {
		t.Fatalf("the answer's verdict was %v, want the server's refusal so the caller falls back to a plain message", err)
	}
}
