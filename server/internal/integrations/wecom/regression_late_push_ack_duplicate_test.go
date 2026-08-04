package wecom

// regression_late_push_ack_duplicate_test.go — guards the one thing a person
// notices the instant it breaks: the same file lands in the chat twice.
//
// sendMedia (media_upload.go) pushes the file with aibot_send_msg and waits for
// a verdict, then treats anything that is not a clean verdict as a refusal and
// re-sends the SAME media_id down the passive route. A verdict that never came
// back is not a refusal. Nothing drains the socket while a turn is being
// handled — dispatchFrame calls the handler on the read goroutine and the engine
// Router runs EnsureSession and AppendMessage inline — so the ack for a frame
// WeCom accepted on time can go unread until after the ack window has closed.
// The push landed, the file is already in the conversation, and the fallback
// puts a second copy of it there.
//
// The distinction is the whole point, because the fallback is right for the
// other case: a push WeCom genuinely refused reached nobody, and re-sending it
// is the only thing that gets the file to the person who asked. Both tests here
// are the same delivery through the same rig, driven through
// Outbound.processEvent the way a finished turn drives it. The only difference
// between them is whether the server said no or merely said nothing in time.
//
// The invariant is deliberately not a shape: one accepted upload puts at most
// one copy of the file in front of the person. Whether a fix stops reading
// silence as a refusal, waits differently, or reports the delivery as failed is
// its own call — two copies in the chat is the defect.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// mediaPushConn plays a WeCom that has its own opinion about the media frames,
// and leaves everything else — the three-step upload, the words of the answer,
// the streaming bubble — to the fake server underneath it.
//
// It scripts the one thing fakeAibotServer has no dial for: a push the server
// ACCEPTS and whose acknowledgement the client does not get to read. From the
// client's side that is indistinguishable from a verdict that is merely late,
// which is the case the read loop produces on every busy turn.
type mediaPushConn struct {
	*fakeAibotServer

	// pushVerdict is what WeCom decided about the aibot_send_msg carrying the
	// file. withholdPushAck decides whether the client ever hears it.
	pushVerdict     int
	withholdPushAck bool

	dmu sync.Mutex
	// accepted is one entry per media frame the server took, which is one
	// entry per copy of the file the person can see in the chat.
	accepted []acceptedFile
}

// acceptedFile is one copy of one file, and the route that put it there.
type acceptedFile struct {
	cmd     string
	mediaID string
}

func (c *mediaPushConn) WriteMessage(msgType int, data []byte) error {
	var env struct {
		Cmd     string         `json:"cmd"`
		Headers frameHeaders   `json:"headers"`
		Body    map[string]any `json:"body"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	mediaID, carriesMedia := mediaIDOfFrame(env.Body)
	if !carriesMedia || (env.Cmd != cmdSendMsg && env.Cmd != cmdRespondMsg) {
		return c.fakeAibotServer.WriteMessage(msgType, data)
	}

	c.fakeAibotServer.mu.Lock()
	c.fakeAibotServer.posts = append(c.fakeAibotServer.posts,
		map[string]any{"cmd": env.Cmd, "body": env.Body})
	c.fakeAibotServer.mu.Unlock()

	verdict := c.respondErr
	if env.Cmd == cmdSendMsg {
		verdict = c.pushVerdict
	}
	if verdict == 0 {
		// The server took the frame. Whatever the client concludes from here,
		// the file is in the chat.
		c.dmu.Lock()
		c.accepted = append(c.accepted, acceptedFile{cmd: env.Cmd, mediaID: mediaID})
		c.dmu.Unlock()
	}
	if env.Cmd == cmdSendMsg && c.withholdPushAck {
		return nil // taken, and its verdict never reaches the client in time
	}
	c.reply(env.Headers.ReqID, verdict, nil)
	return nil
}

// copies reports what reached the chat, in the order it got there.
func (c *mediaPushConn) copies() []acceptedFile {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	return append([]acceptedFile(nil), c.accepted...)
}

// mediaIDOfFrame picks the media_id out of a frame body that carries a file,
// and reports whether the frame carries one at all — the stream frames of the
// answer and the ordinary text pushes share these two cmds.
func mediaIDOfFrame(body map[string]any) (string, bool) {
	kind, _ := body["msgtype"].(string)
	switch mediaMsgType(kind) {
	case mediaTypeFile, mediaTypeImage, mediaTypeVoice, mediaTypeVideo:
	default:
		return "", false
	}
	nested, ok := body[kind].(map[string]any)
	if !ok {
		return "", false
	}
	id, _ := nested["media_id"].(string)
	return id, true
}

// TestAFileWecomAlreadyAcceptedIsNotSentASecondTime — what breaks for a person
// when this regresses: they ask for a chart, the agent makes one, and they get
// two of it. Two identical images seconds apart in the same conversation, with
// nothing to say which is which or whether the agent produced two things. In a
// group chat everyone else sees the double post too, and on a big deck it is
// the upload paid for twice.
//
// The turn here answered into a streaming bubble, which is what leaves a
// callback req_id behind for the passive route — the ordinary shape of a turn
// that produced a file, and the only shape in which the fallback can fire at
// all.
func TestAFileWecomAlreadyAcceptedIsNotSentASecondTime(t *testing.T) {
	rig := newMediaRig(t)

	sender := rig.senders.get(rig.inst)
	if sender == nil {
		t.Fatal("the rig has no live sender for the installation")
	}
	// Production waits five seconds for a verdict. Standing still for five
	// seconds to observe a verdict the rig is deliberately withholding would
	// only make the test slow.
	sender.ackTimeout = 100 * time.Millisecond
	script := &mediaPushConn{
		fakeAibotServer: rig.srv,
		pushVerdict:     0, // WeCom took the push: the file is in the chat
		withholdPushAck: true,
	}
	sender.conn = script

	rig.openBubble(t, "REQ-42")
	rig.attach("chart.png", "image/png", payload(4096))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("图在下面。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	sent := script.copies()

	// Staging check, not the assertion. If the push never reached the server
	// this run says nothing about a frame that landed.
	if len(sent) == 0 {
		t.Fatalf("the rig did not stage the case it is named for: WeCom accepted no media frame at all, "+
			"so nothing here is a landed push whose verdict ran late.\nframes the server received: %v",
			rig.srv.postFrames())
	}

	if len(sent) != 1 {
		t.Fatalf("WeCom accepted the push carrying %s and put the file in the chat; only its verdict "+
			"was late, and the delivery read that silence as a refusal and sent the same media_id again "+
			"on the passive route — the server accepted %d copies: %v.\n"+
			"the person asked for one chart and receives two identical ones, seconds apart in the same "+
			"conversation, with nothing to tell them whether the agent produced two things.\n"+
			"a verdict that never came back is not a refusal: the read loop that routes acks is the same "+
			"goroutine that handles the turn, so an ack WeCom sent on time is read after the %v window "+
			"has closed. A refusal reached nobody and is worth re-sending; a frame that landed is not.\n"+
			"frames the server received: %v",
			rig.srv.mediaID, len(sent), sent, sender.ackTimeout, rig.srv.postFrames())
	}
}

// TestAFileWecomActuallyRefusedIsStillDeliveredOnce is the control for the test
// above, and it passes today. Same rig, same file, same turn — the only
// difference is that the server answers the push with a refusal instead of
// staying silent. A refusal reached nobody, so the passive route has to carry
// the file, and exactly one copy arrives.
//
// It is here so a red run above cannot be answered by deleting the fallback:
// that would leave the person with no file at all in this case, which is the
// worse of the two failures.
func TestAFileWecomActuallyRefusedIsStillDeliveredOnce(t *testing.T) {
	rig := newMediaRig(t)

	sender := rig.senders.get(rig.inst)
	if sender == nil {
		t.Fatal("the rig has no live sender for the installation")
	}
	sender.ackTimeout = 100 * time.Millisecond
	script := &mediaPushConn{
		fakeAibotServer: rig.srv,
		pushVerdict:     40058, // WeCom will not take media on this cmd
	}
	sender.conn = script

	rig.openBubble(t, "REQ-42")
	rig.attach("chart.png", "image/png", payload(4096))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("图在下面。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	sent := script.copies()
	if len(sent) != 1 {
		t.Fatalf("WeCom refused the push outright (errcode %d) and the chat ended up with %d copies "+
			"of the file: %v.\nthe person asked for a chart: a refused push reached nobody, so the "+
			"documented fallback has to put it there, exactly once.\nframes the server received: %v",
			script.pushVerdict, len(sent), sent, rig.srv.postFrames())
	}
	if sent[0].cmd != cmdRespondMsg {
		t.Fatalf("the one copy that arrived went out on %s, want the passive route (%s) — the push "+
			"was refused and cannot have delivered anything", sent[0].cmd, cmdRespondMsg)
	}
}
