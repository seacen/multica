package wecom

// inbound_media_test.go — what happens between "a colleague drops a photo in
// the chat" and "the agent can open it".
//
// The first half is the read loop: a photo is a message now, not something to
// apologise for. The second half runs the whole thing — a forged
// aibot_msg_callback frame through the real engine.Router, the real wecom
// resolvers, a real HTTP server holding really-encrypted bytes — and asserts
// the attachment reaches the chat message.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// imageFrame is the frame WeCom pushes when someone sends a photo.
func imageFrame(msgID, url, aeskey string) frameEnvelope {
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "wb-1",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "image",
		"image":    map[string]any{"url": url, "aeskey": aeskey},
	})
	return frameEnvelope{Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-1"}, Body: body}
}

// TestAPhotoIsAMessageNow: the old behaviour was a "please send it as text"
// receipt. A photo now reaches the engine carrying a placeholder body and the
// descriptors the resolver needs.
func TestAPhotoIsAMessageNow(t *testing.T) {
	var got channel.InboundMessage
	c, conn, _ := testChannel(t, func(_ context.Context, m channel.InboundMessage) error {
		got = m
		return nil
	})
	sender := newWSSender(conn, nil)

	if err := c.dispatchFrame(context.Background(), imageFrame("msg-photo", "https://cos.invalid/p.enc", testAESKey), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if n := len(conn.sends()); n != 0 {
		t.Fatalf("a photo must not draw a 'send it as text' receipt, got %d", n)
	}
	if got.Text != copyFor(DefaultLocale).MediaImage {
		t.Fatalf("Text = %q, want the image placeholder", got.Text)
	}
	if got.Type != channel.MsgTypeImage {
		t.Fatalf("Type = %v, want image", got.Type)
	}
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 1 {
		t.Fatalf("Raw carried %d attachments, want 1", len(wm.Media))
	}
	if wm.Media[0].URL != "https://cos.invalid/p.enc" || wm.Media[0].AESKey != testAESKey || wm.Media[0].Kind != channel.MsgTypeImage {
		t.Fatalf("attachment descriptor = %+v", wm.Media[0])
	}
}

// TestMixedRendersItsRunsInOrder: "look at this" written above a picture has
// to still read that way in the stored body, or the agent loses which
// sentence went with which attachment.
func TestMixedRendersItsRunsInOrder(t *testing.T) {
	var got channel.InboundMessage
	c, conn, _ := testChannel(t, func(_ context.Context, m channel.InboundMessage) error {
		got = m
		return nil
	})
	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-mixed-media",
		"aibotid":  "wb-1",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "mixed",
		"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "看下这张图"}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/a.enc", "aeskey": testAESKey}},
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "第二季度的"}},
		}},
	})
	if err := c.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	want := "看下这张图\n" + copyFor(DefaultLocale).MediaImage + "\n第二季度的"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q", got.Text, want)
	}
	wm, _ := wecomMsgFromRaw(got)
	if len(wm.Media) != 1 {
		t.Fatalf("Raw carried %d attachments, want 1", len(wm.Media))
	}
}

// TestAPhotoWithNoUrlStillGetsAReceipt: the type is one we handle, but this
// particular frame has nothing to fetch and nothing to say. Ingesting a bare
// placeholder would start an agent run over an empty picture.
func TestAPhotoWithNoUrlStillGetsAReceipt(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error {
		t.Fatal("an image with no url must not reach the engine handler")
		return nil
	})
	if err := c.dispatchFrame(context.Background(), imageFrame("msg-nourl", "", testAESKey), newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if n := len(conn.sends()); n != 1 {
		t.Fatalf("want one receipt, got %d", n)
	}
}

// ---- end to end ----

type fakeIssueCreator struct{}

func (fakeIssueCreator) Create(context.Context, service.IssueCreateParams, service.IssueCreateOpts) (service.IssueCreateResult, error) {
	return service.IssueCreateResult{}, nil
}

type fakeTaskEnqueuer struct {
	mu       sync.Mutex
	promoted []pgtype.UUID
}

func (f *fakeTaskEnqueuer) EnqueueChatTask(context.Context, db.ChatSession, pgtype.UUID, bool) (db.AgentTaskQueue, error) {
	return db.AgentTaskQueue{}, nil
}

func (f *fakeTaskEnqueuer) PromoteChannelChatTasksIfMediaReady(_ context.Context, sessionID pgtype.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoted = append(f.promoted, sessionID)
	return nil
}

func (f *fakeTaskEnqueuer) promotions() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.promoted)
}

type fakeSessionReader struct{}

func (fakeSessionReader) GetChatSession(context.Context, pgtype.UUID) (db.ChatSession, error) {
	return db.ChatSession{ID: uuidOf(6)}, nil
}
func (fakeSessionReader) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return db.Workspace{}, nil
}

// bindingSessionBinder is fakeSessionBinder plus a signal, so the test can
// wait for the detached media path instead of sleeping on it.
type bindingSessionBinder struct {
	fakeSessionBinder
	mu    sync.Mutex
	bound []engine.BindMediaInput
	done  chan struct{}
}

func newBindingSessionBinder() *bindingSessionBinder {
	b := &bindingSessionBinder{done: make(chan struct{})}
	b.sessionID = uuidOf(6)
	return b
}

func (b *bindingSessionBinder) BindMediaRefs(_ context.Context, in engine.BindMediaInput) error {
	b.mu.Lock()
	b.bound = append(b.bound, in)
	b.mu.Unlock()
	close(b.done)
	return nil
}

func (b *bindingSessionBinder) waitBound(t *testing.T) engine.BindMediaInput {
	t.Helper()
	select {
	case <-b.done:
	case <-time.After(10 * time.Second):
		t.Fatal("media was never bound to the chat message")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bound[0]
}

// TestAPhotoTravelsFromTheSocketToTheChatMessage is the whole chain: a frame
// off the wire, through the Router's pipeline, out to a real HTTP fetch and a
// real decrypt, and back into BindMedia with a reference the agent can open.
func TestAPhotoTravelsFromTheSocketToTheChatMessage(t *testing.T) {
	plaintext := []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("photo bytes", 200))
	cos := cosServer(t, plaintext, `attachment; filename="whiteboard.png"`)
	defer cos.Close()

	storage := &fakeMediaStorage{}
	ledger := newFakeMediaLedger(storage)
	binder := newBindingSessionBinder()
	tasks := &fakeTaskEnqueuer{}

	router := engine.NewRouter(fakeIssueCreator{}, tasks, fakeSessionReader{}, engine.RouterConfig{Logger: testLogger()})
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: &installationResolver{store: &fakeInstallationLookup{inst: Installation{
			ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3),
			Status: InstallationActive, BotID: "wb-1",
		}}},
		Identity: &identityResolver{store: &fakeIdentityLookup{
			binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, member: true,
		}},
		Dedup:      &deduper{q: &fakeDedupQueries{claimToken: uuidOf(9)}},
		Session:    &sessionBinder{session: binder},
		Audit:      &auditor{q: &fakeAuditQueries{}},
		Media:      NewMediaResolver(storage, ledger, nil, nil, testLogger()),
		OriginType: originWecomChat,
	})
	defer router.Drain(context.Background())

	c, conn, _ := testChannel(t, router.Handle)
	if err := c.dispatchFrame(context.Background(), imageFrame("msg-e2e", cos.URL, testAESKey), newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if n := len(conn.sends()); n != 0 {
		t.Fatalf("the photo drew %d receipts; it should have been ingested", n)
	}

	// The message itself is durable before any download starts, carrying the
	// placeholder and the budget the run waits out.
	appended := binder.fakeSessionBinder.appended
	if appended.Body != copyFor(DefaultLocale).MediaImage {
		t.Fatalf("stored body = %q, want the placeholder", appended.Body)
	}
	if appended.MediaPendingSeconds <= 0 {
		t.Fatal("no media budget was persisted; the agent run would start before the photo lands")
	}

	bound := binder.waitBound(t)
	if len(bound.MediaRefs) != 1 {
		t.Fatalf("bound %d refs, want 1", len(bound.MediaRefs))
	}
	ref := bound.MediaRefs[0]
	if ref.Filename != "whiteboard.png" || ref.Type != channel.MsgTypeImage {
		t.Fatalf("bound ref = %+v", ref)
	}
	if bound.MessageID != uuidOf(5) || bound.SessionID != uuidOf(6) || bound.WorkspaceID != uuidOf(2) || bound.Sender != uuidOf(7) {
		t.Fatalf("ref bound to the wrong message: %+v", bound)
	}
	uploads := storage.stored()
	if len(uploads) != 1 || string(uploads[0].data) != string(plaintext) {
		t.Fatal("the object in storage is not the decrypted photo")
	}
	if len(ledger.records) != 1 || ledger.records[0].StorageKey != ref.StorageKey {
		t.Fatalf("no intent row covers the uploaded object: %+v", ledger.records)
	}
	deadline := time.After(5 * time.Second)
	for tasks.promotions() == 0 {
		select {
		case <-deadline:
			t.Fatal("the deferred agent run was never promoted after the photo bound")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestATextMessagePaysNothingForTheMediaPath: HasMedia is the gate. A plain
// sentence must not persist a media budget or wait on a binding that will
// never come.
func TestATextMessagePaysNothingForTheMediaPath(t *testing.T) {
	binder := newBindingSessionBinder()
	router := engine.NewRouter(fakeIssueCreator{}, &fakeTaskEnqueuer{}, fakeSessionReader{}, engine.RouterConfig{Logger: testLogger()})
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: &installationResolver{store: &fakeInstallationLookup{inst: Installation{
			ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3),
			Status: InstallationActive, BotID: "wb-1",
		}}},
		Identity: &identityResolver{store: &fakeIdentityLookup{
			binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, member: true,
		}},
		Dedup:      &deduper{q: &fakeDedupQueries{claimToken: uuidOf(9)}},
		Session:    &sessionBinder{session: binder},
		Audit:      &auditor{q: &fakeAuditQueries{}},
		Media:      NewMediaResolver(&fakeMediaStorage{}, newFakeMediaLedger(nil), nil, nil, testLogger()),
		OriginType: originWecomChat,
	})
	defer router.Drain(context.Background())

	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-plain",
		"aibotid":  "wb-1",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": "早上好"},
	})
	c, conn, _ := testChannel(t, router.Handle)
	if err := c.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if got := binder.fakeSessionBinder.appended.MediaPendingSeconds; got != 0 {
		t.Fatalf("MediaPendingSeconds = %v for a text message, want 0", got)
	}
	select {
	case <-binder.done:
		t.Fatal("a text message went down the media binding path")
	case <-time.After(200 * time.Millisecond):
	}
}
