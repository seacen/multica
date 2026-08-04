package wecom

// media_ingest_test.go — the resolver, driven against a real HTTP server
// serving really-encrypted bytes. Nothing here stubs the download or the
// decrypt: the test server encrypts a known payload with the same algorithm
// WeCom uses, and the assertion is that exactly those bytes reach storage.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// ---- fakes for the two durable seams ----

type storedObject struct {
	key         string
	data        []byte
	contentType string
	filename    string
}

type fakeMediaStorage struct {
	mu      sync.Mutex
	uploads []storedObject
	err     error
}

func (s *fakeMediaStorage) Upload(_ context.Context, key string, data []byte, contentType, filename string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return "", s.err
	}
	s.uploads = append(s.uploads, storedObject{key: key, data: append([]byte{}, data...), contentType: contentType, filename: filename})
	return s.ObjectURL(key), nil
}

func (s *fakeMediaStorage) ObjectURL(key string) string { return "https://objects.invalid/" + key }

func (s *fakeMediaStorage) stored() []storedObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storedObject{}, s.uploads...)
}

type fakeMediaLedger struct {
	mu      sync.Mutex
	records []engine.RecordPendingMediaObjectParams
	ok      bool
	err     error
	// seenUploads snapshots how many objects were already in storage when
	// each intent row was written, so the ordering can be asserted.
	seenUploads []int
	storage     *fakeMediaStorage
}

func newFakeMediaLedger(storage *fakeMediaStorage) *fakeMediaLedger {
	return &fakeMediaLedger{ok: true, storage: storage}
}

func (l *fakeMediaLedger) RecordPendingMediaObject(_ context.Context, p engine.RecordPendingMediaObjectParams) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, p)
	if l.storage != nil {
		l.seenUploads = append(l.seenUploads, len(l.storage.stored()))
	}
	return l.ok, l.err
}

type recordedNotice struct {
	installation pgtype.UUID
	msg          pendingSend
}

type fakeMediaNotifier struct {
	mu      sync.Mutex
	notes   []recordedNotice
	sendErr error
}

func (n *fakeMediaNotifier) send(id pgtype.UUID, msg pendingSend) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notes = append(n.notes, recordedNotice{installation: id, msg: msg})
	return n.sendErr
}

func (n *fakeMediaNotifier) sent() []recordedNotice {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]recordedNotice{}, n.notes...)
}

// ---- harness ----

// cosServer stands in for the Tencent COS address a callback points at: it
// serves one encrypted payload, with whatever Content-Disposition the test
// wants, and records that no credential was presented.
func cosServer(t *testing.T, plaintext []byte, disposition string) *httptest.Server {
	t.Helper()
	ciphertext := encryptLikeWecom(t, testAESKey, plaintext)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if disposition != "" {
			w.Header().Set("Content-Disposition", disposition)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(ciphertext)
	}))
}

// mediaMessage builds the envelope the Router hands the resolver, going
// through the real frame decoder so the test cannot drift from the wire.
func mediaMessage(t *testing.T, msgType string, body map[string]any) channel.InboundMessage {
	t.Helper()
	full := map[string]any{
		"msgid":    "MSGID-MEDIA",
		"aibotid":  "wb-1",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  msgType,
	}
	for k, v := range body {
		full[k] = v
	}
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	var mc aibotMsgCallback
	if err := json.Unmarshal(raw, &mc); err != nil {
		t.Fatalf("decode callback: %v", err)
	}
	text, ok := mc.routableText(copyFor(DefaultLocale))
	if !ok {
		t.Fatalf("callback of type %q is not routable; the fixture is wrong", msgType)
	}
	return channelMessageFromCallback("wb-1", mc, copyFor(DefaultLocale), text, "req-1")
}

func mediaInstallation() engine.ResolvedInstallation {
	return engine.ResolvedInstallation{
		ID:          uuidOf(1),
		WorkspaceID: uuidOf(2),
		AgentID:     uuidOf(3),
		Active:      true,
		Platform:    Installation{ID: uuidOf(1), BotID: "wb-1"},
	}
}

// ---- HasMedia ----

func TestHasMediaLooksOnlyAtWhatIsAlreadyInHand(t *testing.T) {
	r := NewMediaResolver(&fakeMediaStorage{}, newFakeMediaLedger(nil), nil, nil, testLogger())
	cases := []struct {
		name    string
		msgType string
		body    map[string]any
		want    bool
	}{
		{"text", "text", map[string]any{"text": map[string]any{"content": "hi"}}, false},
		{"voice is text by the time we see it", "voice", map[string]any{"voice": map[string]any{"content": "hi"}}, false},
		{"image", "image", map[string]any{"image": map[string]any{"url": "https://cos.invalid/a", "aeskey": testAESKey}}, true},
		{"file", "file", map[string]any{"file": map[string]any{"url": "https://cos.invalid/b", "aeskey": testAESKey}}, true},
		{"video", "video", map[string]any{"video": map[string]any{"url": "https://cos.invalid/c", "aeskey": testAESKey}}, true},
		{"mixed with a picture", "mixed", map[string]any{"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "look"}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/d", "aeskey": testAESKey}},
		}}}, true},
		{"mixed, text only", "mixed", map[string]any{"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "look"}},
		}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.HasMedia(mediaMessage(t, tc.msgType, tc.body)); got != tc.want {
				t.Fatalf("HasMedia = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasMediaSaysNoWhenThereIsNoUrlToFetch(t *testing.T) {
	r := NewMediaResolver(&fakeMediaStorage{}, newFakeMediaLedger(nil), nil, nil, testLogger())
	// A body with an aeskey and no url is not something we can download; it
	// must not buy the message a media deadline and a deferred agent run.
	mc := aibotMsgCallback{MsgType: "image"}
	mc.Image = mediaBody{AESKey: testAESKey}
	msg := channelMessageFromCallback("wb-1", mc, copyFor(DefaultLocale), "[Image]", "req-1")
	if r.HasMedia(msg) {
		t.Fatal("HasMedia said yes to a body with no url")
	}
}

// ---- the whole ingest ----

// TestResolveMediaStoresTheDecryptedFile is the load-bearing one: real HTTP,
// real AES, and the bytes in storage are the bytes the user sent.
func TestResolveMediaStoresTheDecryptedFile(t *testing.T) {
	plaintext := []byte("\x89PNG\r\n\x1a\n" + strings.Repeat("pixels", 500))
	srv := cosServer(t, plaintext, `attachment; filename*=UTF-8''%E5%AD%A3%E6%8A%A5.png`)
	defer srv.Close()

	storage := &fakeMediaStorage{}
	ledger := newFakeMediaLedger(storage)
	r := NewMediaResolver(storage, ledger, nil, nil, testLogger())

	msg := mediaMessage(t, "image", map[string]any{
		"image": map[string]any{"url": srv.URL, "aeskey": testAESKey},
	})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

	if len(got.MediaRefs) != 1 {
		t.Fatalf("MediaRefs = %d, want 1", len(got.MediaRefs))
	}
	uploads := storage.stored()
	if len(uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(uploads))
	}
	if string(uploads[0].data) != string(plaintext) {
		t.Fatalf("stored %d bytes, want the %d decrypted bytes the user sent", len(uploads[0].data), len(plaintext))
	}
	ref := got.MediaRefs[0]
	if ref.Type != channel.MsgTypeImage {
		t.Errorf("ref type = %v, want image", ref.Type)
	}
	if ref.Filename != "季报.png" {
		t.Errorf("filename = %q, want the Content-Disposition name", ref.Filename)
	}
	if ref.MimeType != "image/png" {
		t.Errorf("mime = %q, want image/png from the .png extension", ref.MimeType)
	}
	if ref.SizeBytes != int64(len(plaintext)) {
		t.Errorf("size = %d, want the decrypted length %d", ref.SizeBytes, len(plaintext))
	}
	if ref.StorageKey != uploads[0].key || ref.StorageURL != storage.ObjectURL(ref.StorageKey) {
		t.Errorf("ref does not point at what was uploaded: %+v", ref)
	}
	if !strings.Contains(ref.StorageKey, "wecom") {
		t.Errorf("storage key %q does not name the channel", ref.StorageKey)
	}
}

// TestResolveMediaRecordsIntentBeforeUploading pins the ordering the whole
// no-inline-delete design rests on: nothing is written to the object store
// until a ledger row exists to cover it.
func TestResolveMediaRecordsIntentBeforeUploading(t *testing.T) {
	srv := cosServer(t, []byte("payload"), "")
	defer srv.Close()

	storage := &fakeMediaStorage{}
	ledger := newFakeMediaLedger(storage)
	r := NewMediaResolver(storage, ledger, nil, nil, testLogger())

	msg := mediaMessage(t, "file", map[string]any{
		"file": map[string]any{"url": srv.URL, "aeskey": testAESKey},
	})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

	if len(ledger.records) != 1 {
		t.Fatalf("intent rows = %d, want 1", len(ledger.records))
	}
	if ledger.seenUploads[0] != 0 {
		t.Fatal("the intent row was written after the upload; a crash in between would leave an uncovered object")
	}
	rec := ledger.records[0]
	if rec.StorageKey != storage.stored()[0].key {
		t.Errorf("intent key %q != uploaded key %q", rec.StorageKey, storage.stored()[0].key)
	}
	if rec.StorageURL != got.MediaRefs[0].StorageURL {
		t.Errorf("intent url %q != attachment url %q", rec.StorageURL, got.MediaRefs[0].StorageURL)
	}
	if rec.ChatMessageID != uuidOf(5) || rec.WorkspaceID != uuidOf(2) || rec.InstallationID != uuidOf(1) {
		t.Errorf("intent row is not scoped to the message: %+v", rec)
	}
}

// TestResolveMediaSkipsAKeyTheReconcilerOwns: ok=false means the row has left
// 'pending'. Uploading anyway would resurrect an object that is being deleted.
func TestResolveMediaSkipsAKeyTheReconcilerOwns(t *testing.T) {
	srv := cosServer(t, []byte("payload"), "")
	defer srv.Close()

	storage := &fakeMediaStorage{}
	ledger := newFakeMediaLedger(storage)
	ledger.ok = false
	r := NewMediaResolver(storage, ledger, nil, nil, testLogger())

	msg := mediaMessage(t, "image", map[string]any{
		"image": map[string]any{"url": srv.URL, "aeskey": testAESKey},
	})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)
	if len(storage.stored()) != 0 {
		t.Fatal("uploaded into a key the reconciler owns")
	}
	if len(got.MediaRefs) != 0 {
		t.Fatal("produced a ref for an object that was never written")
	}
}

// TestResolveMediaKeepsGoingWhenOneAttachmentFails: a 图文混排 with a good
// picture and an expired one must still deliver the good one.
func TestResolveMediaKeepsGoingWhenOneAttachmentFails(t *testing.T) {
	good := cosServer(t, []byte("the good picture"), `attachment; filename="ok.png"`)
	defer good.Close()
	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer gone.Close()

	storage := &fakeMediaStorage{}
	notifier := &fakeMediaNotifier{}
	r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, notify: notifier, languages: fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}, logger: testLogger()}

	msg := mediaMessage(t, "mixed", map[string]any{"mixed": map[string]any{"msg_item": []any{
		map[string]any{"msgtype": "text", "text": map[string]any{"content": "these two"}},
		map[string]any{"msgtype": "image", "image": map[string]any{"url": gone.URL, "aeskey": testAESKey}},
		map[string]any{"msgtype": "image", "image": map[string]any{"url": good.URL, "aeskey": testAESKey}},
	}}})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

	if len(got.MediaRefs) != 1 {
		t.Fatalf("MediaRefs = %d, want the one that downloaded", len(got.MediaRefs))
	}
	if got.MediaRefs[0].Filename != "ok.png" {
		t.Errorf("kept the wrong attachment: %+v", got.MediaRefs[0])
	}
	if n := len(notifier.sent()); n != 1 {
		t.Fatalf("the sender was told %d times, want exactly one notice", n)
	}
}

// TestResolveMediaTellsTheSenderWhatWentWrong: too big and did-not-arrive get
// different wording, because they need different things done about them. Two
// failures of the same kind are still one notice.
func TestResolveMediaTellsTheSenderWhatWentWrong(t *testing.T) {
	oversize := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "209715200")
		w.WriteHeader(http.StatusOK)
	}))
	defer oversize.Close()
	expired := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer expired.Close()

	t.Run("one notice names the size limit", func(t *testing.T) {
		notifier := &fakeMediaNotifier{}
		storage := &fakeMediaStorage{}
		r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, notify: notifier, languages: fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}, logger: testLogger()}
		msg := mediaMessage(t, "file", map[string]any{
			"file": map[string]any{"url": oversize.URL, "aeskey": testAESKey},
		})
		r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

		sent := notifier.sent()
		if len(sent) != 1 {
			t.Fatalf("notices = %d, want 1", len(sent))
		}
		if sent[0].msg.Content != copyFor(LocaleEn).MediaTooLarge {
			t.Fatalf("notice = %q, want the too-large wording", sent[0].msg.Content)
		}
		if sent[0].msg.ChatID != "T-alex" || sent[0].msg.ChatType != chatTypeSingleInt {
			t.Fatalf("notice went to %+v, want the chat the attachment came from", sent[0].msg)
		}
		if sent[0].installation != uuidOf(1) {
			t.Fatalf("notice sent on installation %v", sent[0].installation)
		}
	})

	t.Run("two failures of one kind are one notice", func(t *testing.T) {
		notifier := &fakeMediaNotifier{}
		storage := &fakeMediaStorage{}
		r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, notify: notifier, languages: fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}, logger: testLogger()}
		msg := mediaMessage(t, "mixed", map[string]any{"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "image", "image": map[string]any{"url": expired.URL, "aeskey": testAESKey}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": expired.URL, "aeskey": testAESKey}},
		}}})
		r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

		sent := notifier.sent()
		if len(sent) != 1 {
			t.Fatalf("notices = %d, want 1 — saying it twice helps nobody", len(sent))
		}
		if sent[0].msg.Content != copyFor(LocaleEn).MediaUnreadable {
			t.Fatalf("notice = %q, want the unreadable wording once", sent[0].msg.Content)
		}
	})

	t.Run("nothing is said when everything worked", func(t *testing.T) {
		srv := cosServer(t, []byte("fine"), "")
		defer srv.Close()
		notifier := &fakeMediaNotifier{}
		storage := &fakeMediaStorage{}
		r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, notify: notifier, languages: fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}, logger: testLogger()}
		msg := mediaMessage(t, "image", map[string]any{
			"image": map[string]any{"url": srv.URL, "aeskey": testAESKey},
		})
		r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)
		if n := len(notifier.sent()); n != 0 {
			t.Fatalf("sent %d notices for a message that worked", n)
		}
	})
}

// TestResolveMediaRefusesAnUndecryptablePayload: a wrong or missing key must
// never put ciphertext in the object store labelled as a picture.
func TestResolveMediaRefusesAnUndecryptablePayload(t *testing.T) {
	srv := cosServer(t, []byte("secret report"), "")
	defer srv.Close()

	for _, tc := range []struct {
		name   string
		aeskey string
	}{
		{"no key on the frame", ""},
		{"a key that is not the one it was encrypted with", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := &fakeMediaStorage{}
			r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, logger: testLogger()}
			mc := aibotMsgCallback{MsgID: "MSGID-BAD", ChatID: "T-alex", ChatType: "single", MsgType: "image"}
			mc.Image = mediaBody{URL: srv.URL, AESKey: tc.aeskey}
			msg := channelMessageFromCallback("wb-1", mc, copyFor(DefaultLocale), "[Image]", "req-1")

			got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)
			if len(storage.stored()) != 0 {
				t.Fatal("stored bytes it could not decrypt")
			}
			if len(got.MediaRefs) != 0 {
				t.Fatal("produced a ref for an object that was never written")
			}
		})
	}
}

// TestResolveMediaNamesAnUnnamedFile: COS does not always send a
// Content-Disposition. The name then has to come from somewhere, and two
// attachments in one message must not collide on it.
func TestResolveMediaNamesAnUnnamedFile(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + strings.Repeat("x", 64))
	srv := cosServer(t, png, "")
	defer srv.Close()

	storage := &fakeMediaStorage{}
	r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, logger: testLogger()}
	msg := mediaMessage(t, "mixed", map[string]any{"mixed": map[string]any{"msg_item": []any{
		map[string]any{"msgtype": "image", "image": map[string]any{"url": srv.URL, "aeskey": testAESKey}},
		map[string]any{"msgtype": "image", "image": map[string]any{"url": srv.URL, "aeskey": testAESKey}},
	}}})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)

	if len(got.MediaRefs) != 2 {
		t.Fatalf("MediaRefs = %d, want 2", len(got.MediaRefs))
	}
	first, second := got.MediaRefs[0], got.MediaRefs[1]
	if first.Filename == second.Filename {
		t.Fatalf("both attachments are called %q", first.Filename)
	}
	if first.StorageKey == second.StorageKey {
		t.Fatal("both attachments share a storage key; the second would overwrite the first")
	}
	if !strings.HasSuffix(first.Filename, ".png") {
		t.Errorf("filename %q does not carry the sniffed type", first.Filename)
	}
	if first.MimeType != "image/png" {
		t.Errorf("mime = %q, want image/png sniffed from the decrypted bytes", first.MimeType)
	}
}

// TestResolveMediaKeepsTheKeyPerChatMessage: the same WeCom message can be
// ingested twice once its dedup claim goes stale. Sharing a key would run the
// second ingest into the first one's ledger row.
func TestResolveMediaKeepsTheKeyPerChatMessage(t *testing.T) {
	srv := cosServer(t, []byte("same bytes"), "")
	defer srv.Close()

	keyFor := func(chatMessageID pgtype.UUID) string {
		storage := &fakeMediaStorage{}
		r := &wecomMediaResolver{storage: storage, ledger: newFakeMediaLedger(storage), http: &http.Client{}, logger: testLogger()}
		msg := mediaMessage(t, "image", map[string]any{
			"image": map[string]any{"url": srv.URL, "aeskey": testAESKey},
		})
		got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), chatMessageID, msg)
		if len(got.MediaRefs) != 1 {
			t.Fatalf("MediaRefs = %d", len(got.MediaRefs))
		}
		return got.MediaRefs[0].StorageKey
	}
	if keyFor(uuidOf(5)) == keyFor(uuidOf(8)) {
		t.Fatal("two chat messages derived the same object key")
	}
	if keyFor(uuidOf(5)) != keyFor(uuidOf(5)) {
		t.Fatal("the key is not a function of the message; a retry would orphan the first object")
	}
}

// TestResolveMediaWithoutStorageLeavesThePlaceholder: a deployment with no
// object store must degrade to the text, not to a panic or a broken ref.
func TestResolveMediaWithoutStorageLeavesThePlaceholder(t *testing.T) {
	r := NewMediaResolver(nil, nil, nil, nil, testLogger())
	msg := mediaMessage(t, "image", map[string]any{
		"image": map[string]any{"url": "https://cos.invalid/a", "aeskey": testAESKey},
	})
	got := r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, uuidOf(6), uuidOf(5), msg)
	if len(got.MediaRefs) != 0 {
		t.Fatal("produced refs with nowhere to store them")
	}
	if got.Text != copyFor(DefaultLocale).MediaImage {
		t.Fatalf("Text = %q, want the placeholder intact", got.Text)
	}
}
