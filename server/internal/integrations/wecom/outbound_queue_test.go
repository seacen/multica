package wecom

// outbound_queue_test.go — the aibot socket is the only way out, so every
// window where it is down (lease flip, reconnect backoff, process just
// started) used to swallow an agent reply whole. These tests pin the
// hold-and-resend behaviour that replaced the drop.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func uuidOf(b byte) pgtype.UUID { return pgtype.UUID{Bytes: [16]byte{b}, Valid: true} }

// uuidText is the string form the events carry.
func uuidText(u pgtype.UUID) string { return util.UUIDToString(u) }

// contentsOf pulls the markdown body out of every aibot_send_msg frame.
func contentsOf(conn *recordingConn) []string {
	var out []string
	for _, f := range conn.sends() {
		body, _ := f["body"].(map[string]any)
		md, _ := body["markdown"].(map[string]any)
		if s, ok := md["content"].(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// TestRegistrySendHoldsMessageUntilConnectionReturns is the core of the fix:
// with no live sender the message waits, and the reconnect delivers it.
func TestRegistrySendHoldsMessageUntilConnectionReturns(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(1)

	if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: "答案是 42"}); err != nil {
		t.Fatalf("send with no connection must not fail: %v", err)
	}

	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)

	got := contentsOf(conn)
	if len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("reconnect delivered %v, want the held reply", got)
	}
	// A second flush must not re-send it.
	reg.flushPending(inst)
	if n := len(contentsOf(conn)); n != 1 {
		t.Fatalf("flush replayed a delivered message: %d sends", n)
	}
}

// TestRegistrySendGoesStraightOutWhenConnected keeps the fast path intact.
func TestRegistrySendGoesStraightOutWhenConnected(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(2)
	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))

	if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: "now"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := contentsOf(conn); len(got) != 1 {
		t.Fatalf("want an immediate send, got %v", got)
	}
}

// TestQueueKeepsOrderAcrossTheOutage — a conversation resent out of order
// reads as nonsense.
func TestQueueKeepsOrderAcrossTheOutage(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(3)
	for _, s := range []string{"one", "two", "three"} {
		if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: s}); err != nil {
			t.Fatalf("send %q: %v", s, err)
		}
	}
	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)

	got := contentsOf(conn)
	want := []string{"one", "two", "three"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// TestQueueDropsOldestPastTheCap — an installation that stays down must not
// grow the queue without bound.
func TestQueueDropsOldestPastTheCap(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(4)
	for i := 0; i < maxPendingPerInstallation+3; i++ {
		if err := reg.send(inst, pendingSend{
			ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: string(rune('a' + i%26)),
		}); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	if n := reg.pending.depth(inst); n != maxPendingPerInstallation {
		t.Fatalf("queue depth %d, want the %d cap", n, maxPendingPerInstallation)
	}
}

// TestQueueDropsExpiredOnFlush — a day-old reply is noise, not a reply.
func TestQueueDropsExpiredOnFlush(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(5)
	clock := time.Now()
	reg.pending.now = func() time.Time { return clock }

	if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: "stale"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	clock = clock.Add(pendingTTL + time.Minute)
	if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: "fresh"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)

	got := contentsOf(conn)
	if len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("flush delivered %v, want only the fresh message", got)
	}
}

// TestFlushRequeuesWhenTheSocketDiesMidDrain — the socket can drop again
// while we are catching up; what did not go out must stay queued.
func TestFlushRequeuesWhenTheSocketDiesMidDrain(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(6)
	for _, s := range []string{"one", "two"} {
		if err := reg.send(inst, pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: s}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	reg.set(inst, newWSSender(&failingConn{}, testLogger()))
	reg.flushPending(inst)

	if n := reg.pending.depth(inst); n != 2 {
		t.Fatalf("queue depth %d after a failed drain, want both messages held", n)
	}

	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)
	if got := contentsOf(conn); len(got) != 2 || got[0] != "one" {
		t.Fatalf("retry delivered %v, want both in order", got)
	}
}

// TestRegistrySendRejectsAnUnsendableMessage — a body the wire will never
// accept must not sit in the queue forever.
func TestRegistrySendRejectsAnUnsendableMessage(t *testing.T) {
	reg := NewSendersRegistry()
	inst := uuidOf(7)
	if err := reg.send(inst, pendingSend{ChatID: "", ChatType: chatTypeSingleInt, Content: "x"}); err == nil {
		t.Fatal("send with no chat id must fail rather than queue")
	}
	if n := reg.pending.depth(inst); n != 0 {
		t.Fatalf("queue depth %d, want the malformed message rejected", n)
	}
}

// ---- through the real EventChatDone subscriber ----

type fakeOutboundQueries struct {
	binding     db.ChannelChatSessionBinding
	bindingErr  error
	install     db.ChannelInstallation
	installErr  error
	memberBind  db.ChannelUserBinding
	memberErr   error
	workspace   db.Workspace
	workspaceEr error
}

func (f *fakeOutboundQueries) GetChannelChatSessionBindingBySession(context.Context, db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error) {
	return f.binding, f.bindingErr
}

func (f *fakeOutboundQueries) GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	return f.install, f.installErr
}

func (f *fakeOutboundQueries) FindChannelBindingForMember(context.Context, db.FindChannelBindingForMemberParams) (db.ChannelUserBinding, error) {
	return f.memberBind, f.memberErr
}

func (f *fakeOutboundQueries) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return f.workspace, f.workspaceEr
}

// TestAgentReplySurvivesAReconnect is the user-visible statement of the bug:
// the agent finishes while the socket is down, and the answer still arrives.
// On the pre-change code processEvent returned "connection not ready" and the
// reply was gone for good.
func TestAgentReplySurvivesAReconnect(t *testing.T) {
	inst := uuidOf(8)
	session := uuidOf(9)
	q := &fakeOutboundQueries{
		binding: db.ChannelChatSessionBinding{
			InstallationID: inst,
			ChannelChatID:  "T-alex",
			ChatType:       "p2p",
		},
		install: db.ChannelInstallation{ID: inst, Status: string(InstallationActive)},
	}
	reg := NewSendersRegistry()
	o := NewOutbound(q, reg, testLogger())

	sessionStr := uuidText(session)
	err := o.processEvent(context.Background(), events.Event{
		ChatSessionID: sessionStr,
		Payload:       protocol.ChatDonePayload{Content: "答案是 42"},
	})
	if err != nil {
		t.Fatalf("a reply produced while the socket is down must not error: %v", err)
	}

	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)

	got := contentsOf(conn)
	if len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("after reconnect the user received %v, want the agent's reply", got)
	}
}

// TestInboxPushSurvivesAReconnect — same for the inbox card.
func TestInboxPushSurvivesAReconnect(t *testing.T) {
	inst := uuidOf(10)
	q := &fakeOutboundQueries{
		memberBind: db.ChannelUserBinding{InstallationID: inst, ChannelUserID: "T-alex"},
		workspace:  db.Workspace{Slug: "acme"},
		install:    db.ChannelInstallation{ID: inst, Status: string(InstallationActive)},
	}
	reg := NewSendersRegistry()
	o := NewOutbound(q, reg, testLogger())

	item := map[string]any{
		"recipient_type": "member",
		"recipient_id":   uuidText(uuidOf(11)),
		"workspace_id":   uuidText(uuidOf(12)),
		"type":           "issue_assigned",
		"title":          "修一下登录",
	}
	o.tryDeliverInbox(context.Background(), item, uuidText(uuidOf(11)), uuidText(uuidOf(12)))

	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, testLogger()))
	reg.flushPending(inst)

	if n := len(contentsOf(conn)); n != 1 {
		t.Fatalf("after reconnect the inbox card arrived %d times, want 1", n)
	}
}
