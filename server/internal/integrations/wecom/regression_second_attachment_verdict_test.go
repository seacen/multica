package wecom

// regression_second_attachment_verdict_test.go — guards the one thing a turn
// carrying two files depends on: that the verdict each file is judged by is the
// server's answer about THAT file.
//
// The passive media reply is addressed by the turn's callback req_id
// (media_upload.go sendMedia), and the waiter for its answer is filed under the
// same id (ws_sender.go awaitReply). An answer may carry as many files as the
// agent produced, and sendAttachments walks them one at a time on that one
// req_id — so the key belongs to the turn and not to the upload. Once one
// verdict runs late, and the ack window is five seconds while the chunk path
// already retries for exactly this reason, the answer that finally arrives is
// handed to whichever upload happens to be waiting under that key. A file WeCom
// refused then reads as delivered, and a refusal is the only thing that makes
// the adapter tell anyone a file did not arrive.
//
// media_upload_test.go and outbound_media_test.go both stop short of this: the
// sendMedia tests send one file per turn, and the one test that sends two
// (TestSeveralFilesAllArrive) is answered by a server that never runs late, so
// the two uploads never overlap on the key.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// lateVerdictConn plays WeCom for a turn carrying two files. It scripts the one
// thing fakeAibotServer has no dial for — WHEN each passive reply is answered —
// and leaves everything else (the uploads, the pushes) to the fake server
// underneath it.
//
// The first file's verdict is taken and withheld until the second file's frame
// is on the wire, which is what a late ack looks like from the client's side.
// The second file is refused.
type lateVerdictConn struct {
	*fakeAibotServer

	// firstVerdict is what the server decided about the first file, delivered
	// after that file's caller has stopped waiting. secondVerdict is its answer
	// about the second file, on time.
	firstVerdict  int
	secondVerdict int

	smu      sync.Mutex
	responds int
	held     []byte
}

func (c *lateVerdictConn) WriteMessage(msgType int, data []byte) error {
	var env struct {
		Cmd     string         `json:"cmd"`
		Headers frameHeaders   `json:"headers"`
		Body    map[string]any `json:"body"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	if env.Cmd != cmdRespondMsg {
		return c.fakeAibotServer.WriteMessage(msgType, data)
	}

	c.fakeAibotServer.mu.Lock()
	c.fakeAibotServer.posts = append(c.fakeAibotServer.posts,
		map[string]any{"cmd": env.Cmd, "body": env.Body})
	c.fakeAibotServer.mu.Unlock()

	c.smu.Lock()
	c.responds++
	n := c.responds
	if n == 1 {
		c.held = c.response(env.Headers.ReqID, c.firstVerdict, nil)
		c.smu.Unlock()
		return nil // taken, and not answered inside the ack window
	}
	held := c.held
	c.held = nil
	c.smu.Unlock()

	// The first file's answer finally arrives. It carries the turn's req_id,
	// because that is the only thing addressing a passive reply, and it lands
	// while the second file is waiting under the same id.
	if held != nil {
		c.out <- held
	}
	c.reply(env.Headers.ReqID, c.secondVerdict, nil)
	return nil
}

// TestASecondFileWecomRefusedIsNotReportedAsDelivered — what breaks for a
// person when this regresses: they ask for two files, the agent produces both,
// and WeCom throws the second one away. They receive one file and are told
// nothing about the other, because the delivery path recorded it as sent — the
// verdict it read was the first file's, arriving late on the shared key. The
// notice that exists for exactly this ("一个文件没能发送") never mentions it, and
// the log line for the turn says the file went out.
//
// The two files here are one turn's attachments, handed to the transport the
// way sendAttachments hands them over: the same installation, the same chat,
// and the same callback req_id for both. Nothing above this can see which file
// failed — the notice is one per turn either way — so the verdict handed back
// per file is where the loss is visible.
func TestASecondFileWecomRefusedIsNotReportedAsDelivered(t *testing.T) {
	srv := newFakeAibotServer()
	// aibot_send_msg is refused, so each file takes the documented fallback and
	// answers the turn instead (media_upload.go sendMedia).
	srv.sendErr = 40058

	sender := wireUpload(t, srv)
	// Short enough that a withheld verdict is a test rather than a wait.
	// Production allows five seconds.
	sender.ackTimeout = 100 * time.Millisecond
	script := &lateVerdictConn{
		fakeAibotServer: srv,
		firstVerdict:    0,     // the deck did land; its ack was merely late
		secondVerdict:   40058, // the chart WeCom refused outright
	}
	sender.conn = script

	inst := uuidOf(31)
	senders := NewSendersRegistry()
	senders.log = testLogger()
	senders.set(inst, sender)

	const turnReqID = "REQ-42"
	ctx := context.Background()
	deck := senders.sendMedia(ctx, inst, "T-alex", chatTypeSingleInt, turnReqID, mediaSend{
		Kind: mediaTypeFile, MediaID: "MEDIA-deck",
	})
	chart := senders.sendMedia(ctx, inst, "T-alex", chatTypeSingleInt, turnReqID, mediaSend{
		Kind: mediaTypeImage, MediaID: "MEDIA-chart",
	})

	// Staging check, not the assertion: the first file's verdict is withheld
	// past its whole ack window, so its caller cannot come away believing it
	// landed however the second file is fixed.
	if deck == nil {
		t.Fatalf("the rig did not stage a late verdict: the first file reported success "+
			"though the server never answered it inside the %v ack window", sender.ackTimeout)
	}

	// The server refused the second file, and refused the push before it. No
	// route delivered it, so no fix can make reporting success right — whether
	// the reply gets a key of its own, the straggler is matched to the frame it
	// answers, or the turn only ever gets one passive reply.
	if chart == nil {
		t.Fatalf("WeCom refused the second file of the turn (errcode %d on both routes) and the "+
			"delivery reported it sent.\n"+
			"both files answered the same turn, so both filed their waiter under the one callback "+
			"req_id (%s): the verdict that came back late for the first file was handed to the "+
			"second, and the second's own refusal arrived to find nobody waiting.\n"+
			"the person asked for two attachments, receives one, and is never told the other was "+
			"thrown away — a refusal is the only signal that makes the adapter say a file did not "+
			"arrive.\nframes the server received: %v",
			script.secondVerdict, turnReqID, srv.postFrames())
	}
}

// TestTwoFilesOnSeparateTurnsAreEachJudgedOnTheirOwnVerdict is the control for
// the test above, and it passes today. Same rig, same withheld verdict, same
// refusal — the only difference is that the two files answer two different
// turns, so the late verdict has no later waiter to be handed to. It is here so
// a red run above cannot be read as the rig refusing everything: what makes the
// second file's refusal reach its caller is nothing but a key of its own.
func TestTwoFilesOnSeparateTurnsAreEachJudgedOnTheirOwnVerdict(t *testing.T) {
	srv := newFakeAibotServer()
	srv.sendErr = 40058

	sender := wireUpload(t, srv)
	sender.ackTimeout = 100 * time.Millisecond
	sender.conn = &lateVerdictConn{fakeAibotServer: srv, firstVerdict: 0, secondVerdict: 40058}

	inst := uuidOf(31)
	senders := NewSendersRegistry()
	senders.log = testLogger()
	senders.set(inst, sender)

	ctx := context.Background()
	senders.sendMedia(ctx, inst, "T-alex", chatTypeSingleInt, "REQ-42", mediaSend{
		Kind: mediaTypeFile, MediaID: "MEDIA-deck",
	})
	chart := senders.sendMedia(ctx, inst, "T-alex", chatTypeSingleInt, "REQ-43", mediaSend{
		Kind: mediaTypeImage, MediaID: "MEDIA-chart",
	})

	if chart == nil {
		t.Fatalf("WeCom refused a file on both routes and the delivery reported it sent, "+
			"with no other turn's verdict in flight on its req_id.\n"+
			"frames the server received: %v", srv.postFrames())
	}
}
