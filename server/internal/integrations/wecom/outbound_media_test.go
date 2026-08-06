package wecom

// outbound_media_test.go — the agent produced a file, and the person who asked
// for it is in WeCom.
//
// Every test here is written against what lands in the chat: the answer in
// words, then the file underneath it. The two are separate messages because
// the long connection has no way to put a picture inside a streaming bubble,
// and the order is not negotiable — an upload takes seconds and the answer
// must never wait behind one, nor be lost to one that fails.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// ---- scaffolding ----

// fakeObjectStore stands in for the deployment's object storage: the agent's
// upload already went in through `multica attachment upload`, and this is the
// read back out.
type fakeObjectStore struct {
	byKey  map[string][]byte
	err    error
	opened []string
}

func newFakeObjectStore() *fakeObjectStore {
	return &fakeObjectStore{byKey: map[string][]byte{}}
}

// put stores bytes under a key and returns the url an attachment row carries.
func (f *fakeObjectStore) put(key string, data []byte) string {
	f.byKey[key] = data
	return "https://objects.example/" + key
}

func (f *fakeObjectStore) KeyFromURL(rawURL string) string {
	return strings.TrimPrefix(rawURL, "https://objects.example/")
}

func (f *fakeObjectStore) GetReader(_ context.Context, key string) (io.ReadCloser, error) {
	f.opened = append(f.opened, key)
	if f.err != nil {
		return nil, f.err
	}
	data, ok := f.byKey[key]
	if !ok {
		return nil, fmt.Errorf("no object %q", key)
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

// mediaRig is one installation whose socket is the fake WeCom server, wired
// exactly as boot wires it: a senders registry, a stream store, the typing
// indicator that opens bubbles, and the chat-done subscriber that answers.
type mediaRig struct {
	srv     *fakeAibotServer
	senders *sendersRegistry
	streams *streamStore
	typing  *TypingIndicatorManager
	objects *fakeObjectStore
	q       *fakeOutboundQueries

	inst      pgtype.UUID
	session   pgtype.UUID
	workspace pgtype.UUID
	message   pgtype.UUID
}

func newMediaRig(t *testing.T) *mediaRig {
	t.Helper()
	srv := newFakeAibotServer()
	sender := wireUpload(t, srv)

	inst := uuidOf(31)
	senders := NewSendersRegistry()
	senders.log = testLogger()
	senders.set(inst, sender)

	streams := newStreamStore()
	typing := NewTypingIndicator(TypingIndicatorConfig{
		Senders:    senders,
		Streams:    streams,
		Identities: &fakeIdentities{byChannelUser: map[string]pgtype.UUID{"T-alex": uuidOf(33)}},
		Logger:     testLogger(),
	})

	return &mediaRig{
		srv:     srv,
		senders: senders,
		streams: streams,
		typing:  typing,
		objects: newFakeObjectStore(),
		q: &fakeOutboundQueries{
			binding: db.ChannelChatSessionBinding{
				InstallationID: inst,
				ChannelChatID:  "T-alex",
				ChatType:       "p2p",
			},
			install: db.ChannelInstallation{ID: inst, Status: string(InstallationActive)},
		},
		inst:      inst,
		session:   uuidOf(34),
		workspace: uuidOf(35),
		message:   uuidOf(36),
	}
}

// attach files an attachment row against the answer message, with bytes in the
// object store to back it.
func (r *mediaRig) attach(filename, contentType string, data []byte) {
	url := r.objects.put("obj-"+filename, data)
	r.q.attachments = append(r.q.attachments, db.Attachment{
		ID:            uuidOf(byte(40 + len(r.q.attachments))),
		WorkspaceID:   r.workspace,
		ChatMessageID: r.message,
		Filename:      filename,
		Url:           url,
		ContentType:   contentType,
		SizeBytes:     int64(len(data)),
	})
}

// outbound builds the subscriber under test. Attachment delivery runs on the
// caller's goroutine so a test sees the whole thing without waiting for one.
func (r *mediaRig) outbound() *Outbound {
	o := NewOutbound(r.q, r.senders, r.streams, testLogger(), WithAttachments(r.objects))
	o.spawn = func(f func()) { f() }
	return o
}

// answered publishes the chat:done the agent's finished turn publishes,
// carrying the assistant message the attachments are bound to.
func (r *mediaRig) answered(content string) events.Event {
	return events.Event{
		Type:          protocol.EventChatDone,
		WorkspaceID:   uuidText(r.workspace),
		ChatSessionID: uuidText(r.session),
		Payload: protocol.ChatDonePayload{
			ChatSessionID: uuidText(r.session),
			MessageID:     uuidText(r.message),
			Content:       content,
		},
	}
}

// openBubble puts a streaming bubble on screen for this session, the way an
// inbound message does.
func (r *mediaRig) openBubble(t *testing.T, reqID string) {
	t.Helper()
	mc := aibotMsgCallback{MsgID: "MSGID-M", ChatID: "T-alex", ChatType: "single", MsgType: "text"}
	mc.From.UserID = "T-alex"
	mc.Text.Content = "帮我做份复盘"
	msg := channelMessageFromCallback("BOT-1", mc, copyFor(DefaultLocale), mc.Text.Content, reqID)
	r.typing.OnIngested(context.Background(), engine.ResolvedInstallation{
		ID:              r.inst,
		InstallerUserID: uuidOf(33),
		Platform:        Installation{},
	}, msg, r.session)
}

// mediaPosts returns the frames that carried a file, in the order they went
// out; textPosts returns the ones that carried words.
func (r *mediaRig) mediaPosts() []map[string]any {
	var out []map[string]any
	for _, f := range r.srv.postFrames() {
		body, _ := f["body"].(map[string]any)
		switch body["msgtype"] {
		case "file", "image", "voice", "video":
			out = append(out, body)
		}
	}
	return out
}

func (r *mediaRig) textPosts() []string {
	var out []string
	for _, f := range r.srv.postFrames() {
		body, _ := f["body"].(map[string]any)
		switch body["msgtype"] {
		case "markdown":
			md, _ := body["markdown"].(map[string]any)
			if s, ok := md["content"].(string); ok {
				out = append(out, s)
			}
		case "stream":
			st, _ := body["stream"].(map[string]any)
			if finish, _ := st["finish"].(bool); finish {
				if s, ok := st["content"].(string); ok {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

// ---- the answer and the file ----

// TestAnAnswerWithAFileSendsTheWordsThenTheFile is the whole feature. The
// answer arrives as words and the deck arrives as a file, in that order,
// through one socket.
func TestAnAnswerWithAFileSendsTheWordsThenTheFile(t *testing.T) {
	rig := newMediaRig(t)
	deck := payload(mediaChunkBytes + 2048)
	rig.attach("季度复盘.pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation", deck)

	if err := rig.outbound().processEvent(context.Background(), rig.answered("做好了，见附件。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	if got := rig.textPosts(); len(got) != 1 || got[0] != "做好了，见附件。" {
		t.Fatalf("words delivered = %v, want the answer", got)
	}
	files := rig.mediaPosts()
	if len(files) != 1 {
		t.Fatalf("delivered %d files, want 1", len(files))
	}
	if files[0]["msgtype"] != "file" {
		t.Errorf("a .pptx went out as %v, want a file", files[0]["msgtype"])
	}
	nested := files[0]["file"].(map[string]any)
	if nested["media_id"] != rig.srv.mediaID {
		t.Errorf("the message carries media_id %v, want the one the upload returned (%v)",
			nested["media_id"], rig.srv.mediaID)
	}
	if !bytesEqual(rig.srv.assembled(), deck) {
		t.Error("the bytes WeCom reassembled are not the ones the agent produced")
	}
	if len(rig.objects.opened) != 1 {
		t.Errorf("read the object %d times, want once", len(rig.objects.opened))
	}
}

// TestTheFileFollowsAnAnswerThatLandedInTheBubble — the answer replaced the
// streaming bubble in place, which is where it belongs, and the file comes
// after it as its own message. A stream frame cannot carry an attachment.
func TestTheFileFollowsAnAnswerThatLandedInTheBubble(t *testing.T) {
	rig := newMediaRig(t)
	rig.openBubble(t, "REQ-42")
	rig.attach("chart.png", "image/png", payload(4096))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("图在下面。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	if got := rig.textPosts(); len(got) != 1 || got[0] != "图在下面。" {
		t.Fatalf("words delivered = %v, want the answer sealing the bubble", got)
	}
	files := rig.mediaPosts()
	if len(files) != 1 || files[0]["msgtype"] != "image" {
		t.Fatalf("files delivered = %v, want one image", files)
	}
}

// TestAnEmptyAnswerWithAFileStillDeliversTheFile — an agent that uploads a deck
// and says nothing has still answered. The platform makes an assistant message
// for exactly this case; dropping the turn on empty content would throw the
// work away.
func TestAnEmptyAnswerWithAFileStillDeliversTheFile(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("report.pdf", "application/pdf", payload(2048))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	if got := rig.textPosts(); len(got) != 0 {
		t.Errorf("sent %v with no answer to give", got)
	}
	if files := rig.mediaPosts(); len(files) != 1 {
		t.Fatalf("delivered %d files, want the one the agent produced", len(files))
	}
}

// TestSeveralFilesAllArrive — one per message, in the order they were
// produced.
func TestSeveralFilesAllArrive(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("one.pdf", "application/pdf", payload(1024))
	rig.attach("two.png", "image/png", payload(1024))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("两份都在这儿。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	files := rig.mediaPosts()
	if len(files) != 2 {
		t.Fatalf("delivered %d files, want 2", len(files))
	}
	if files[0]["msgtype"] != "file" || files[1]["msgtype"] != "image" {
		t.Errorf("files went out as %v then %v, want file then image", files[0]["msgtype"], files[1]["msgtype"])
	}
}

// ---- when the file does not make it ----

// TestAnUnreadableFileNeverCostsTheAnswer is the rule the ordering exists for.
// Object storage is down, so the deck cannot be read at all — and the sentence
// the agent wrote still has to reach the person who asked.
func TestAnUnreadableFileNeverCostsTheAnswer(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("季度复盘.pptx", "application/pdf", payload(1024))
	rig.objects.err = errors.New("object storage unreachable")

	if err := rig.outbound().processEvent(context.Background(), rig.answered("做好了，见附件。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	got := rig.textPosts()
	if len(got) != 2 {
		t.Fatalf("the chat received %v, want the answer and a word about the missing file", got)
	}
	if got[0] != "做好了，见附件。" {
		t.Errorf("first message = %q, want the answer", got[0])
	}
	if got[1] != copyPacks[LocaleZhHans].MediaSendFailed {
		t.Errorf("second message = %q, want the localized notice", got[1])
	}
	if files := rig.mediaPosts(); len(files) != 0 {
		t.Errorf("sent %d files after failing to read any", len(files))
	}
}

// TestAServerThatRefusesTheUploadStillLeavesTheAnswer — same rule, the other
// failure. The bytes were read and the upload was refused mid-handshake.
func TestAServerThatRefusesTheUploadStillLeavesTheAnswer(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("chart.png", "image/png", payload(4096))
	rig.srv.initErr = 40058

	if err := rig.outbound().processEvent(context.Background(), rig.answered("图在下面。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	got := rig.textPosts()
	if len(got) != 2 || got[0] != "图在下面。" {
		t.Fatalf("the chat received %v, want the answer first", got)
	}
	if got[1] != copyPacks[LocaleZhHans].MediaSendFailed {
		t.Errorf("second message = %q, want the localized notice", got[1])
	}
}

// TestOneNoticeCoversEveryFileThatFailed — two attachments that both fail is
// one thing that went wrong. Saying it twice helps nobody.
func TestOneNoticeCoversEveryFileThatFailed(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("one.pdf", "application/pdf", payload(1024))
	rig.attach("two.pdf", "application/pdf", payload(1024))
	rig.objects.err = errors.New("object storage unreachable")

	if err := rig.outbound().processEvent(context.Background(), rig.answered("在附件里。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	notices := 0
	for _, s := range rig.textPosts() {
		if s == copyPacks[LocaleZhHans].MediaSendFailed {
			notices++
		}
	}
	if notices != 1 {
		t.Fatalf("the user was told %d times, want once", notices)
	}
}

// TestAFileTooBigForTheWireIsNotUploaded — 50MB is what a hundred 512KB chunks
// can express and there is no second transport. Starting an upload that cannot
// finish would spend minutes to arrive at the same notice.
func TestAFileTooBigForTheWireIsNotUploaded(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("huge.bin", "application/octet-stream", make([]byte, maxMediaUploadBytes+1))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("给你了。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if n := len(rig.srv.initFrames()); n != 0 {
		t.Errorf("started %d uploads for a file the wire cannot carry", n)
	}
	got := rig.textPosts()
	if len(got) != 2 || got[1] != copyPacks[LocaleZhHans].MediaSendFailed {
		t.Fatalf("the chat received %v, want the answer and the notice", got)
	}
}

// TestNoAttachmentsMeansNoExtraWork — the ordinary turn, which is most of them.
func TestNoAttachmentsMeansNoExtraWork(t *testing.T) {
	rig := newMediaRig(t)

	if err := rig.outbound().processEvent(context.Background(), rig.answered("42。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if got := rig.textPosts(); len(got) != 1 || got[0] != "42。" {
		t.Fatalf("the chat received %v, want just the answer", got)
	}
	if n := len(rig.objects.opened); n != 0 {
		t.Errorf("read object storage %d times for a turn with no attachments", n)
	}
}

// TestAnAnswerWithNoAttachmentSinkIsUnchanged — a deployment with no storage
// backend keeps the behaviour it had, rather than logging a failure per turn.
func TestAnAnswerWithNoAttachmentSinkIsUnchanged(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("chart.png", "image/png", payload(1024))

	o := NewOutbound(rig.q, rig.senders, rig.streams, testLogger())
	o.spawn = func(f func()) { f() }
	if err := o.processEvent(context.Background(), rig.answered("图在下面。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if got := rig.textPosts(); len(got) != 1 || got[0] != "图在下面。" {
		t.Fatalf("the chat received %v, want just the answer", got)
	}
	if n := len(rig.mediaPosts()); n != 0 {
		t.Errorf("delivered %d files with nowhere to read them from", n)
	}
}

// ---- what WeCom is told the file is ----

// TestAttachmentKindsMapToWhatWecomWillAccept — WeCom validates the bytes
// against the msgtype, and it applies a size ceiling per kind. A 30MB photo
// declared as an image is refused; the same bytes as a file are not.
func TestAttachmentKindsMapToWhatWecomWillAccept(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		contentType string
		filename    string
		size        int
		want        mediaMsgType
	}{
		{"png", "image/png", "chart.png", 4096, mediaTypeImage},
		{"jpeg over the image ceiling", "image/jpeg", "big.jpg", maxOutboundImageBytes + 1, mediaTypeFile},
		{"mp4", "video/mp4", "clip.mp4", 4096, mediaTypeVideo},
		{"mp4 over the video ceiling", "video/mp4", "long.mp4", maxOutboundVideoBytes + 1, mediaTypeFile},
		{"amr", "audio/amr", "note.amr", 4096, mediaTypeVoice},
		{"mp3 is not a WeCom voice note", "audio/mpeg", "song.mp3", 4096, mediaTypeFile},
		{"pptx", "application/vnd.ms-powerpoint", "deck.pptx", 4096, mediaTypeFile},
		{"unnamed type falls back to the extension", "", "chart.png", 4096, mediaTypeImage},
		{"octet-stream falls back to the extension", "application/octet-stream", "clip.mp4", 4096, mediaTypeVideo},
		{"nothing to go on", "", "mystery", 4096, mediaTypeFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wecomMediaKind(tc.contentType, tc.filename, tc.size); got != tc.want {
				t.Errorf("wecomMediaKind(%q, %q, %d) = %q, want %q",
					tc.contentType, tc.filename, tc.size, got, tc.want)
			}
		})
	}
}

// TestAVideoCarriesTheTitleWeCommRequires — video is the one kind with fields
// beyond the media_id, and a body missing them is refused.
func TestAVideoCarriesTheTitleWecomRequires(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("产品演示.mp4", "video/mp4", payload(4096))

	if err := rig.outbound().processEvent(context.Background(), rig.answered("片子在这儿。")); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	files := rig.mediaPosts()
	if len(files) != 1 || files[0]["msgtype"] != "video" {
		t.Fatalf("files = %v, want one video", files)
	}
	video := files[0]["video"].(map[string]any)
	if title, _ := video["title"].(string); title == "" {
		t.Error("the video went out with no title")
	}
	if _, ok := video["description"].(string); !ok {
		t.Error("the video went out with no description field")
	}
}

// bytesEqual keeps the assertions above readable.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
