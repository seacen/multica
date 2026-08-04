package wecom

// replier_test.go — what an unbound user actually reads. A reused token has
// no raw secret to build a URL from, so the prompt has to change shape rather
// than emit a link to nowhere.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

type stubMinter struct {
	token BindingToken
	calls int
}

func (s *stubMinter) Mint(context.Context, pgtype.UUID, pgtype.UUID, string) (BindingToken, error) {
	s.calls++
	return s.token, nil
}

// replierOffline builds a replier whose installation has no live socket —
// the state during a lease flip, the Supervisor's backoff, or the seconds
// after a revoke.
func replierOffline(t *testing.T, minter bindingMinter) (*OutboundReplier, *sendersRegistry, engine.ResolvedInstallation, channel.InboundMessage) {
	t.Helper()
	reg := NewSendersRegistry()
	inst := engine.ResolvedInstallation{ID: uuidOf(3), WorkspaceID: uuidOf(4)}

	r := NewOutboundReplier(OutboundReplierConfig{
		Binding: minter,
		Senders: reg,
		AppURL:  "https://multica.example",
		Logger:  testLogger(),
	})
	msg := channel.InboundMessage{
		Source: channel.Source{SenderID: "T-alex", ChatID: "T-alex", ChatType: channel.ChatTypeP2P},
	}
	return r, reg, inst, msg
}

func replierUnder(t *testing.T, minter bindingMinter) (*OutboundReplier, *sendersRegistry, *recordingConn, engine.ResolvedInstallation, channel.InboundMessage) {
	t.Helper()
	r, reg, inst, msg := replierOffline(t, minter)
	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	return r, reg, conn, inst, msg
}

// TestBindingPromptSendsTheLinkOnAFreshToken keeps the normal path intact.
func TestBindingPromptSendsTheLinkOnAFreshToken(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, _, conn, inst, msg := replierUnder(t, minter)

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	got := contentsOf(conn)
	if len(got) != 1 {
		t.Fatalf("want one prompt, got %v", got)
	}
	if !strings.Contains(got[0], "https://multica.example/wecom/bind?token=s3cret") {
		t.Fatalf("prompt did not carry the bind link: %q", got[0])
	}
}

// TestBindingPromptPointsBackWhenTheTokenWasReused: only the hash is stored,
// so a reused token cannot rebuild its URL. The prompt must say so instead of
// shipping a link with an empty token.
func TestBindingPromptPointsBackWhenTheTokenWasReused(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Reused: true, ExpiresAt: time.Now().Add(5 * time.Minute)}}
	r, _, conn, inst, msg := replierUnder(t, minter)

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	got := contentsOf(conn)
	if len(got) != 1 {
		t.Fatalf("want one prompt, got %v", got)
	}
	if strings.Contains(got[0], "token=") {
		t.Fatalf("a reused token must not produce a link: %q", got[0])
	}
	if got[0] == "" {
		t.Fatal("the user must still be told something")
	}
}

// TestReplierHoldsItsNoticesThroughAReconnect is the user-visible statement of
// the bug: an unbound user writes in while the socket is down. The replier used
// to bail with "connection not ready", so the binding prompt — the one message
// that would let the user get anywhere — was dropped, and the user was left
// with silence and no way to bind.
func TestReplierHoldsItsNoticesThroughAReconnect(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, reg, inst, msg := replierOffline(t, minter)
	c := copyFor(DefaultLocale)

	if err := r.sendBindingPrompt(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding}, c); err != nil {
		t.Fatalf("a prompt raised while the socket is down must not fail: %v", err)
	}
	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeAgentOffline})

	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	reg.flushPending(inst.ID)

	got := contentsOf(conn)
	if len(got) != 2 {
		t.Fatalf("after the reconnect the user received %v, want both notices", got)
	}
	if !strings.Contains(got[0], "https://multica.example/wecom/bind?token=s3cret") {
		t.Fatalf("the bind prompt should have arrived first, got %q", got[0])
	}
	if got[1] != c.AgentOffline {
		t.Fatalf("the offline notice should have arrived second, got %q", got[1])
	}
}

// ---- the binding token must never reach a room ----

// sentFrame is one aibot_send_msg as the socket saw it: who it was addressed
// to, in what kind of chat, and what it said.
type sentFrame struct {
	chatID   string
	chatType int
	content  string
}

func sendViews(conn *recordingConn) []sentFrame {
	var out []sentFrame
	for _, f := range conn.sends() {
		body, _ := f["body"].(map[string]any)
		if body == nil {
			continue
		}
		md, _ := body["markdown"].(map[string]any)
		content, _ := md["content"].(string)
		chatID, _ := body["chatid"].(string)
		chatType := 0
		if n, ok := body["chat_type"].(float64); ok {
			chatType = int(n)
		}
		out = append(out, sentFrame{chatID: chatID, chatType: chatType, content: content})
	}
	return out
}

// groupTrigger is an unbound member writing in a room: Source.ChatID is the
// GROUP, Source.SenderID is the person. Confusing the two is the whole bug.
func groupTrigger() channel.InboundMessage {
	return channel.InboundMessage{
		Source: channel.Source{
			SenderID: "T-alex",
			ChatID:   "wr-group-1",
			ChatType: channel.ChatTypeGroup,
		},
	}
}

// TestBindingTokenNeverReachesTheGroup is the security assertion. A live token
// posted to a room is an account-takeover primitive: whoever clicks first has
// the sender's WeCom userid bound to THEIR Multica account, and every later
// message from the sender — /issue included — runs as the hijacker.
func TestBindingTokenNeverReachesTheGroup(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, _, conn, inst, _ := replierUnder(t, minter)
	msg := groupTrigger()

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	for _, f := range sendViews(conn) {
		if f.chatID == msg.Source.ChatID && strings.Contains(f.content, "s3cret") {
			t.Fatalf("the raw token was posted to the group: %q", f.content)
		}
		if f.chatType == chatTypeGroupInt && strings.Contains(f.content, "token=") {
			t.Fatalf("a group frame carried a bind link: %q", f.content)
		}
	}
}

// TestBindingPromptTargetsTheTriggerSender: the private send goes to the person
// who wrote, addressed as a 1:1 — the same address outbound.go pushes inbox
// cards to.
func TestBindingPromptTargetsTheTriggerSender(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, _, conn, inst, _ := replierUnder(t, minter)
	msg := groupTrigger()

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	var carrier *sentFrame
	frames := sendViews(conn)
	for i := range frames {
		if strings.Contains(frames[i].content, "s3cret") {
			carrier = &frames[i]
		}
	}
	if carrier == nil {
		t.Fatal("nobody was sent the bind link at all")
	}
	if carrier.chatID != msg.Source.SenderID {
		t.Fatalf("bind link addressed to %q, want the sender %q", carrier.chatID, msg.Source.SenderID)
	}
	if carrier.chatType != chatTypeSingleInt {
		t.Fatalf("bind link sent with chat_type=%d, want %d (single)", carrier.chatType, chatTypeSingleInt)
	}
}

// TestBindingPromptStillAnswersTheGroup: silence in a room reads as a broken
// bot, so the room gets a line — one that names nobody and carries no token.
func TestBindingPromptStillAnswersTheGroup(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, _, conn, inst, _ := replierUnder(t, minter)
	msg := groupTrigger()
	c := copyFor(DefaultLocale)

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	var room []sentFrame
	for _, f := range sendViews(conn) {
		if f.chatID == msg.Source.ChatID {
			room = append(room, f)
		}
	}
	if len(room) != 1 {
		t.Fatalf("the room should get exactly one token-free line, got %v", room)
	}
	if room[0].content != c.BindingSentPrivately {
		t.Fatalf("room was told %q, want %q", room[0].content, c.BindingSentPrivately)
	}
	if room[0].chatType != chatTypeGroupInt {
		t.Fatalf("the room line went out as chat_type=%d, want %d (group)", room[0].chatType, chatTypeGroupInt)
	}
}

// TestReusedTokenInAGroupAlsoStaysPrivate: the throttled branch prints no URL,
// but "tap the link I sent you" still points at a private message and must not
// be shouted at the room as if the link were there.
func TestReusedTokenInAGroupAlsoStaysPrivate(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Reused: true, ExpiresAt: time.Now().Add(5 * time.Minute)}}
	r, _, conn, inst, _ := replierUnder(t, minter)
	msg := groupTrigger()
	c := copyFor(DefaultLocale)

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	for _, f := range sendViews(conn) {
		if f.chatID == msg.Source.ChatID && f.content != c.BindingSentPrivately {
			t.Fatalf("the room saw %q, want only the token-free line", f.content)
		}
		if f.chatID == msg.Source.SenderID && f.content != c.BindingPending {
			t.Fatalf("the sender saw %q, want the pending notice", f.content)
		}
	}
}

// TestBindingPromptInADirectChatSendsOneMessage: a 1:1 trigger already IS the
// private chat, so the fix must not double up there.
func TestBindingPromptInADirectChatSendsOneMessage(t *testing.T) {
	minter := &stubMinter{token: BindingToken{Raw: "s3cret", ExpiresAt: time.Now().Add(BindingTokenTTL)}}
	r, _, conn, inst, msg := replierUnder(t, minter)

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	got := sendViews(conn)
	if len(got) != 1 {
		t.Fatalf("a 1:1 prompt should be one message, got %v", got)
	}
	if got[0].chatID != msg.Source.SenderID || got[0].chatType != chatTypeSingleInt {
		t.Fatalf("1:1 prompt addressed to %q/%d, want %q/%d",
			got[0].chatID, got[0].chatType, msg.Source.SenderID, chatTypeSingleInt)
	}
}
