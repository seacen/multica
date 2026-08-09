package engine

// router_media_abandon_test.go — what a person is owed when the photo they
// sent runs out of budget before it is bound.
//
// The Router already handles the budget expiring: it drops the refs, logs a
// warning and lets the run proceed on the "[Image]" placeholder. From the
// resolver's side nothing failed — it was cut off, or it never started — so
// the notice a resolver owns for its own failures cannot fire. The photo is
// gone, the agent answers the line typed beside it as if there were no photo,
// and the only record is a server log the sender cannot read.
//
// Both tests here drive the real Router through the real detached media path.
// The second one is the guard for the channels that have not opted in: the
// interface is optional, and a resolver that does not implement it must come
// out of this path with exactly the behaviour it had before.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// abandonAwareMedia is a MediaResolver that also implements
// MediaAbandonNotifier — the shape an adapter takes when it can push an
// unsolicited message into the chat the attachment came from.
type abandonAwareMedia struct {
	*fakeMedia

	noteMu   sync.Mutex
	notes    int
	liveCtx  bool
	noteMsg  channel.InboundMessage
	noteInst ResolvedInstallation
}

func (m *abandonAwareMedia) NotifyMediaAbandoned(ctx context.Context, inst ResolvedInstallation, msg channel.InboundMessage) {
	m.noteMu.Lock()
	defer m.noteMu.Unlock()
	m.notes++
	// Recorded, not asserted here: a notice handed the context that just
	// expired cannot be written to the chat, which would make the whole call
	// ceremonial.
	m.liveCtx = ctx.Err() == nil
	m.noteInst = inst
	m.noteMsg = msg
}

func (m *abandonAwareMedia) observed() (int, bool, ResolvedInstallation, channel.InboundMessage) {
	m.noteMu.Lock()
	defer m.noteMu.Unlock()
	return m.notes, m.liveCtx, m.noteInst, m.noteMsg
}

// registerMediaSet re-registers the harness's resolver set with a different
// media resolver, leaving every other seam as newHarness built it.
func registerMediaSet(h *harness, media MediaResolver) {
	h.router.Register(channel.TypeFeishu, ResolverSet{
		Installation: h.inst,
		Identity:     h.ident,
		Dedup:        h.dedup,
		Session:      h.binder,
		Audit:        h.audit,
		Replier:      h.replier,
		Typing:       h.typing,
		Media:        media,
		OriginType:   "lark_chat",
	})
}

// TestRouter_MediaBudgetExpiredTellsAResolverThatCanSaySo is the defect. The
// budget runs out while ResolveMedia is still working, so the resolver returns
// having observed no failure of its own; without this call nothing anywhere
// tells the person who sent the attachment that it did not arrive.
func TestRouter_MediaBudgetExpiredTellsAResolverThatCanSaySo(t *testing.T) {
	h := newHarness(t)
	h.router = NewRouter(h.issues, h.tasks, h.reader, RouterConfig{MediaTimeout: 10 * time.Millisecond, Logger: discardLogger()})
	h.media.waitForCancel = true
	media := &abandonAwareMedia{fakeMedia: h.media}
	registerMediaSet(h, media)

	msg := p2pMessage(t)
	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { n, _, _, _ := media.observed(); return n > 0 }) {
		t.Fatal("the media budget expired, the refs were dropped, and the resolver was never told: " +
			"nothing on this path can tell the sender their attachment did not arrive, " +
			"so the bot answers the line they typed beside the picture as if there were no picture")
	}
	notes, liveCtx, inst, gotMsg := media.observed()
	if notes != 1 {
		t.Fatalf("the resolver was told %d times about one abandoned message; the sender reads one apology per notice", notes)
	}
	if !liveCtx {
		t.Fatal("the notice was handed the context that had just expired — nothing can be written to the chat on it, " +
			"so the sender is told nothing after all")
	}
	if !inst.ID.Valid {
		t.Fatal("the notice carries no installation, so the resolver cannot work out which socket to write to")
	}
	if gotMsg.MessageID != msg.MessageID {
		t.Fatalf("the notice carries message %q, want %q — the resolver needs the original message to find the chat and the attachments",
			gotMsg.MessageID, msg.MessageID)
	}
	// The refs are still dropped and the run still proceeds on the
	// placeholder. Telling the sender is an addition to that outcome, not a
	// replacement for it.
	if refs := h.binder.boundMedia().MediaRefs; len(refs) != 0 {
		t.Fatalf("timed-out media refs must still not attach: %+v", refs)
	}
}

// TestRouter_MediaBudgetExpiredLeavesAResolverThatCannotNotifyAlone is the
// blast-radius guard. MediaAbandonNotifier is shared engine code, and lark and
// dingtalk hand the Router resolvers that do not implement it. For them the
// type assertion must simply miss and the expiry path must end exactly where
// it ended before: refs dropped, message still ingested, placeholder task
// still promoted, no panic, no extra call to anything.
func TestRouter_MediaBudgetExpiredLeavesAResolverThatCannotNotifyAlone(t *testing.T) {
	h := newHarness(t)
	h.router = NewRouter(h.issues, h.tasks, h.reader, RouterConfig{MediaTimeout: 10 * time.Millisecond, Logger: discardLogger()})
	h.media.waitForCancel = true
	// fakeMedia is the sibling shape: HasMedia + ResolveMedia and nothing else.
	if _, ok := MediaResolver(h.media).(MediaAbandonNotifier); ok {
		t.Fatal("this test is only a guard while the plain resolver does NOT implement the notifier")
	}
	registerMediaSet(h, h.media)

	if err := h.router.Handle(context.Background(), p2pMessage(t)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return h.media.calls() == 1 }) {
		t.Fatalf("media resolver calls = %d, want 1", h.media.calls())
	}
	if refs := h.binder.boundMedia().MediaRefs; len(refs) != 0 {
		t.Fatalf("timed-out media refs must not attach: %+v", refs)
	}
	if !waitFor(2*time.Second, h.tasks.wasCalled) {
		t.Fatal("a channel that cannot send a notice lost its chat run: the message must still be ingested")
	}
	if !waitFor(2*time.Second, func() bool { return h.tasks.promotionCalls() >= 1 }) {
		t.Fatal("a channel that cannot send a notice lost its placeholder promotion")
	}
	if h.dedup.releases() != 0 {
		t.Fatalf("media timeout must not release a durably appended message, got %d", h.dedup.releases())
	}
}
