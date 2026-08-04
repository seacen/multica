package wecom

// regression_media_budget_silence_test.go — what a person is owed when the
// photo they sent does not make it.
//
// The adapter already apologises for an attachment the resolver itself failed
// on: a dead COS link, a file over the size cap. But the resolver is not the
// only thing that can give up on an attachment. The engine Router wraps the
// whole ResolveMedia call in a media budget of its own, and when that budget
// runs out the Router drops the attachment references and lets the agent run
// on the "[图片]" placeholder. From the resolver's side nothing failed, so the
// notice it owns is never written — the photo is gone and the chat says
// nothing about it. The person watches the bot answer the sentence they typed
// beside the picture as if the picture had never existed, and the only place
// it is recorded is a server log they cannot see.
//
// The invariant these tests hold the code to is deliberately not a shape: a
// photo either reaches the chat message (so the agent can open it) or the
// sender is told it did not. Silence with no attachment is the defect,
// wherever it gets fixed — inside ResolveMedia, in the Router, or somewhere
// new. Both tests drive a real aibot_msg_callback frame through the real
// engine.Router and the real wecom resolvers, the rig inbound_media_test.go
// established, and read the outcome off the two places a person can observe
// it: the refs bound to the message, and the frames on the socket.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ackAfterBudgetStorage is an object store that writes the object and then
// takes a moment too long to say so. That moment is the whole point: the
// bytes are durably in the store, the resolver is handed a success, and the
// budget has expired by the time either of them can act on it. This is the
// ordinary shape of a big file arriving right on the edge of the deadline,
// not an exotic one.
type ackAfterBudgetStorage struct {
	*fakeMediaStorage
}

func (s ackAfterBudgetStorage) Upload(ctx context.Context, key string, data []byte, contentType, filename string) (string, error) {
	link, err := s.fakeMediaStorage.Upload(ctx, key, data, contentType, filename)
	if err != nil {
		return "", err
	}
	// The object is written. Only the acknowledgement is late.
	select {
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		// Safety net so a rig that never expires fails loudly instead of
		// hanging until the whole package times out.
	}
	return link, nil
}

// mediaBudgetRig wires the real Router to the real wecom resolvers with the
// media budget the test wants, and puts a live sender on the socket so an
// outbound notice has somewhere real to land. One conn carries everything the
// person would see.
func mediaBudgetRig(t *testing.T, budget time.Duration, storage mediaStorage) (*wecomChannel, *recordingConn, *bindingSessionBinder, *engine.Router) {
	t.Helper()

	conn := &recordingConn{}
	senders := NewSendersRegistry()
	senders.set(uuidOf(1), newWSSender(conn, testLogger()))

	binder := newBindingSessionBinder()
	router := engine.NewRouter(fakeIssueCreator{}, &fakeTaskEnqueuer{}, fakeSessionReader{}, engine.RouterConfig{
		MediaTimeout: budget,
		Logger:       testLogger(),
	})
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: &installationResolver{store: &fakeInstallationLookup{inst: Installation{
			ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3),
			Status: InstallationActive, BotID: "wb-1",
		}}},
		Identity: &identityResolver{store: &fakeIdentityLookup{
			binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, member: true,
		}},
		Dedup:   &deduper{q: &fakeDedupQueries{claimToken: uuidOf(9)}},
		Session: &sessionBinder{session: binder},
		Audit:   &auditor{q: &fakeAuditQueries{}},
		// The replier is wired to the same registry as the resolver, so a
		// notice raised through either seam shows up on the same socket and
		// the assertion does not care which one a fix chooses.
		Replier:    NewOutboundReplier(OutboundReplierConfig{Senders: senders, Logger: testLogger()}),
		Media:      NewMediaResolver(storage, newFakeMediaLedger(nil), senders, nil, testLogger()),
		OriginType: originWecomChat,
	})
	t.Cleanup(func() { router.Drain(context.Background()) })

	c, _, _ := testChannel(t, router.Handle)
	return c, conn, binder, router
}

// assertTheSenderIsNotLeftGuessing is the invariant both cases share. It runs
// after the detached media path has settled: by then the photo has either
// bound to the chat message or it has not, and the socket has either carried
// a notice or it has not.
func assertTheSenderIsNotLeftGuessing(t *testing.T, bound engine.BindMediaInput, conn *recordingConn, stored int, when string) {
	t.Helper()
	said := contentsOf(conn)
	if len(bound.MediaRefs) > 0 || len(said) > 0 {
		return
	}
	t.Fatalf("%s: the photo never reached the chat message (0 attachments bound, %d object(s) in storage) "+
		"and the chat was told nothing about it (0 messages on the socket). "+
		"The person sees the bot answer the line they typed beside the picture as if there were no picture, "+
		"with nothing to tell them to send it again.", when, stored)
}

// TestAPhotoTheMediaBudgetGaveUpOnIsNotSilentlyDropped: the budget is already
// spent by the time the job comes up, so the download never even starts. The
// resolver observes no failure because it is never asked to do anything —
// which is exactly why the notice it owns cannot fire.
func TestAPhotoTheMediaBudgetGaveUpOnIsNotSilentlyDropped(t *testing.T) {
	cos := cosServer(t, []byte("\x89PNG\r\n\x1a\nthe photo they wanted looked at"), `attachment; filename="chart.png"`)
	defer cos.Close()

	storage := &fakeMediaStorage{}
	// A budget this short is gone before the queued job is scheduled, which is
	// what a real 45s budget looks like behind a burst of earlier attachments
	// in the same room.
	c, conn, binder, router := mediaBudgetRig(t, time.Nanosecond, storage)

	if err := c.dispatchFrame(context.Background(), imageFrame("msg-budget-gone", cos.URL, testAESKey), newWSSender(conn, testLogger()), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	bound := binder.waitBound(t)
	router.Drain(context.Background())

	assertTheSenderIsNotLeftGuessing(t, bound, conn, len(storage.stored()), "the media budget expired before the download started")
}

// TestAPhotoStoredAfterTheBudgetExpiredIsNotSilentlyDropped is the worse half
// of the same defect: the file did arrive. It was downloaded, decrypted and
// written to object storage; only the store's acknowledgement came back after
// the budget ran out. Every attachment the resolver looked at succeeded, so it
// has nothing to apologise for, and the Router quietly throws away the
// references to an object that is sitting in the bucket. The bytes are paid
// for and stored, the agent cannot see them, and neither can the person who
// sent them.
func TestAPhotoStoredAfterTheBudgetExpiredIsNotSilentlyDropped(t *testing.T) {
	cos := cosServer(t, []byte("\x89PNG\r\n\x1a\nthe whiteboard from the workshop"), `attachment; filename="whiteboard.png"`)
	defer cos.Close()

	storage := &fakeMediaStorage{}
	// Long enough for a localhost fetch and decrypt to finish comfortably;
	// the upload is what runs past it, by construction rather than by racing.
	c, conn, binder, router := mediaBudgetRig(t, 750*time.Millisecond, ackAfterBudgetStorage{fakeMediaStorage: storage})

	if err := c.dispatchFrame(context.Background(), imageFrame("msg-budget-late", cos.URL, testAESKey), newWSSender(conn, testLogger()), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	bound := binder.waitBound(t)
	router.Drain(context.Background())

	stored := len(storage.stored())
	if stored == 0 {
		t.Fatal("the rig did not reproduce the case it is named for: nothing reached object storage, " +
			"so this run says nothing about an object that was stored and then forgotten")
	}
	assertTheSenderIsNotLeftGuessing(t, bound, conn, stored, "the object was stored but the budget expired before it could be bound")
}

// TestAPhotoTheResolverItselfCouldNotFetchStillTellsTheSender is the positive
// control for the two above: the same rig, a budget nobody runs out of, and a
// download that fails the way the resolver already knows how to apologise for.
// It passes today. It is here so a future red run says which half broke — the
// wiring that carries a notice to the socket, or the budget cases specifically.
func TestAPhotoTheResolverItselfCouldNotFetchStillTellsTheSender(t *testing.T) {
	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer gone.Close()

	storage := &fakeMediaStorage{}
	c, conn, binder, router := mediaBudgetRig(t, 10*time.Second, storage)

	if err := c.dispatchFrame(context.Background(), imageFrame("msg-link-expired", gone.URL, testAESKey), newWSSender(conn, testLogger()), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	bound := binder.waitBound(t)
	router.Drain(context.Background())

	assertTheSenderIsNotLeftGuessing(t, bound, conn, len(storage.stored()), "the signed link had expired")
}
