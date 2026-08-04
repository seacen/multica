package wecom

// regression_binding_prompt_delivery_test.go — guards the assumption the mint
// throttle makes and never checks: that the link it is suppressing actually
// reached the person it was for.
//
// sendBindingPrompt hands the prompt to the transport and returns nil the
// moment the transport takes it. Taking it is not delivering it — the aibot
// socket acks asynchronously and a non-zero errcode is only logged, and the
// holding queue can drop a message it already accepted (over the cap, or past
// its shelf life). When the first prompt is lost that way the token row it
// wrote still looks live, so Mint answers every message the user sends for the
// next BindingTokenMintInterval with Reused, and the replier tells them to tap
// a link that was never delivered. Ten minutes of a bot insisting it already
// answered, with no way to link an account and nothing on screen to explain it.
//
// The file exists because nothing else in the package joins the two halves:
// binding_test.go exercises the throttle against a fake table that has no
// notion of delivery, and replier_test.go drives the reused branch with a stub
// minter and a prompt that is assumed to have landed.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// carriesBindLink says whether a message hands its reader something they can
// actually click: the bind page with a non-empty token on it.
func carriesBindLink(text string) bool {
	const marker = "/wecom/bind?token="
	i := strings.Index(text, marker)
	if i < 0 {
		return false
	}
	token := text[i+len(marker):]
	if cut := strings.IndexAny(token, " \t\n"); cut >= 0 {
		token = token[:cut]
	}
	return token != ""
}

// TestABindingPromptThatNeverArrivedIsNotTreatedAsSent — an unbound user who
// keeps asking has to end up holding a link they can click. Here their first
// prompt is accepted by the transport and then lost before it reaches them, so
// by the time they write again the only record of it is a token row: to Mint
// the link looks delivered, and the user is answered with "I already sent you
// one". Nobody upstream ever learns the message did not arrive, and the user
// spends the whole mint window unable to bind.
//
// The loss here runs through the outbound queue's own cap — the bot is down and
// a busy workspace pushes the prompt out of the backlog — because that path is
// deterministic and needs no socket. The WeCom-side rejection reaches the same
// dead end by the same route: sendBindingPrompt returned nil, the token row
// says a link is live, and nothing checks.
func TestABindingPromptThatNeverArrivedIsNotTreatedAsSent(t *testing.T) {
	ctx := context.Background()
	svc, _, clock := newThrottledService()
	reg := NewSendersRegistry()
	inst := engine.ResolvedInstallation{ID: uuidOf(7), WorkspaceID: uuidOf(8)}

	r := NewOutboundReplier(OutboundReplierConfig{
		Binding: svc,
		Senders: reg,
		AppURL:  "https://multica.example",
		Logger:  testLogger(),
	})
	alex := channel.InboundMessage{
		Source: channel.Source{SenderID: "T-alex", ChatID: "T-alex", ChatType: channel.ChatTypeP2P},
	}

	// Alex writes to the bot for the first time while the socket is down — a
	// lease flip, the Supervisor's backoff, the seconds after a revoke. A token
	// is minted and the prompt is held for the reconnect.
	r.Reply(ctx, inst, alex, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	// The bot stays down and the workspace is busy: answers owed to everyone
	// else pile in behind Alex's prompt until the backlog is over its cap, and
	// the oldest entry — the prompt — is dropped to make room. Every one of
	// these sends reported success, the drop included.
	for i := 0; i < maxPendingPerInstallation; i++ {
		if err := reg.send(inst.ID, pendingSend{
			ChatID:   fmt.Sprintf("T-colleague-%d", i),
			ChatType: chatTypeSingleInt,
			Content:  "an answer owed to somebody else",
		}); err != nil {
			t.Fatalf("queueing backlog message #%d: %v", i, err)
		}
	}

	// Two minutes on, the socket is back and the backlog drains. Well inside
	// BindingTokenMintInterval, so the token minted above is still live.
	*clock = clock.Add(2 * time.Minute)
	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	reg.flushPending(inst.ID)

	// Alex, who has seen nothing at all from the bot, writes again.
	r.Reply(ctx, inst, alex, engine.Result{Outcome: engine.OutcomeNeedsBinding})

	// Whatever shape the fix takes — a prompt the queue refuses to drop, a
	// throttle that only suppresses a link known to have landed, a resend — the
	// user-visible requirement is one thing: something Alex received has a
	// usable bind link in it.
	var received []string
	for _, f := range sendViews(conn) {
		if f.chatID == alex.Source.SenderID {
			received = append(received, f.content)
		}
	}
	for _, text := range received {
		if carriesBindLink(text) {
			return
		}
	}
	t.Fatalf("an unbound user asked twice and never got a link they could use.\n"+
		"the first prompt was accepted by the transport and lost before delivery; the second\n"+
		"message was answered from the mint throttle, which points at a link that was never\n"+
		"received. this repeats for the full %v window: the bot keeps saying it already sent\n"+
		"one, and the user cannot link their account at all.\n"+
		"everything the user actually received: %q",
		BindingTokenMintInterval, received)
}
