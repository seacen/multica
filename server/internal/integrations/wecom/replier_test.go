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
