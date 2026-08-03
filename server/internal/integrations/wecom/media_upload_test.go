package wecom

// media_upload_test.go — putting a file INTO a chat.
//
// The upload is three cmds over the same socket everything else uses, and the
// only way to know it works is to play the server's half: take the frames off
// the wire, reassemble what they carry, and answer them the way WeCom answers.
// That is what fakeAibotServer is — a server, not a stub. Every test here
// asserts on what it reassembled, so a client that sends the right-looking
// frames but the wrong bytes fails.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- the server ----

// fakeAibotServer implements wsConn and plays WeCom. Frames written to it are
// handled on the caller's goroutine (that is where the socket write happens)
// and the responses queue up for ReadMessage, which the test's pump drains —
// so the client really does wait for a verdict that arrives on another
// goroutine, as it does in production.
type fakeAibotServer struct {
	mu sync.Mutex

	inits    []map[string]any
	finishes []map[string]any
	posts    []map[string]any // aibot_send_msg / aibot_respond_msg frames

	arrivals map[int]int    // chunk_index → how many times it turned up
	parts    map[int][]byte // chunk_index → the bytes it carried
	order    []int          // arrival order, duplicates included

	uploadID string
	mediaID  string

	initErr    int
	chunkErr   int
	finishErr  int
	sendErr    int
	respondErr int

	// silent is one chunk index the server takes and deliberately does not
	// answer, once. It is how a lost ack is staged.
	silent     int
	silentDone bool

	// hold is one chunk index whose answer is withheld until every other
	// chunk has arrived. A client that uploads strictly in order deadlocks on
	// it; that is the point.
	hold     int
	held     []byte
	expected int // total_chunks, learned from the init frame

	out    chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFakeAibotServer() *fakeAibotServer {
	return &fakeAibotServer{
		arrivals: map[int]int{},
		parts:    map[int][]byte{},
		uploadID: "UP-1",
		mediaID:  "MEDIA-1",
		silent:   -1,
		hold:     -1,
		out:      make(chan []byte, 512),
		closed:   make(chan struct{}),
	}
}

func (f *fakeAibotServer) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}
func (f *fakeAibotServer) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeAibotServer) SetWriteDeadline(time.Time) error { return nil }

func (f *fakeAibotServer) ReadMessage() (int, []byte, error) {
	select {
	case b := <-f.out:
		return 1, b, nil
	case <-f.closed:
		return 0, nil, errConnDropped
	}
}

func (f *fakeAibotServer) WriteMessage(_ int, data []byte) error {
	var env struct {
		Cmd     string         `json:"cmd"`
		Headers frameHeaders   `json:"headers"`
		Body    map[string]any `json:"body"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	switch env.Cmd {
	case cmdUploadMediaInit:
		f.mu.Lock()
		f.inits = append(f.inits, env.Body)
		if n, ok := env.Body["total_chunks"].(float64); ok {
			f.expected = int(n)
		}
		f.mu.Unlock()
		f.reply(env.Headers.ReqID, f.initErr, map[string]any{"upload_id": f.uploadID})
	case cmdUploadMediaChunk:
		f.takeChunk(env.Headers.ReqID, env.Body)
	case cmdUploadMediaFinish:
		f.mu.Lock()
		f.finishes = append(f.finishes, env.Body)
		f.mu.Unlock()
		f.reply(env.Headers.ReqID, f.finishErr, map[string]any{"media_id": f.mediaID})
	case cmdSendMsg:
		f.mu.Lock()
		f.posts = append(f.posts, map[string]any{"cmd": env.Cmd, "body": env.Body})
		f.mu.Unlock()
		f.reply(env.Headers.ReqID, f.sendErr, nil)
	case cmdRespondMsg:
		f.mu.Lock()
		f.posts = append(f.posts, map[string]any{"cmd": env.Cmd, "body": env.Body})
		f.mu.Unlock()
		f.reply(env.Headers.ReqID, f.respondErr, nil)
	default:
		f.reply(env.Headers.ReqID, 0, nil)
	}
	return nil
}

// takeChunk records one chunk and decides when its answer goes out.
func (f *fakeAibotServer) takeChunk(reqID string, body map[string]any) {
	idx := int(body["chunk_index"].(float64))
	raw, _ := body["base64_data"].(string)
	bs, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		f.reply(reqID, 40001, nil)
		return
	}

	f.mu.Lock()
	f.arrivals[idx]++
	f.parts[idx] = bs
	f.order = append(f.order, idx)
	swallow := idx == f.silent && !f.silentDone
	if swallow {
		f.silentDone = true
	}
	holdThis := idx == f.hold
	complete := len(f.arrivals) == f.expected && f.expected > 0
	f.mu.Unlock()

	if swallow {
		return // the ack the client will wait for and never get
	}
	if holdThis {
		f.mu.Lock()
		f.held = f.response(reqID, f.chunkErr, nil)
		f.mu.Unlock()
		if complete {
			f.release()
		}
		return
	}
	f.reply(reqID, f.chunkErr, nil)
	if complete {
		f.release()
	}
}

func (f *fakeAibotServer) release() {
	f.mu.Lock()
	held := f.held
	f.held = nil
	f.mu.Unlock()
	if held != nil {
		f.out <- held
	}
}

func (f *fakeAibotServer) response(reqID string, code int, body map[string]any) []byte {
	frame := map[string]any{
		"headers": frameHeaders{ReqID: reqID},
		"errcode": code,
	}
	if code != 0 {
		frame["errmsg"] = "refused"
	}
	if body != nil {
		frame["body"] = body
	}
	b, _ := json.Marshal(frame)
	return b
}

func (f *fakeAibotServer) reply(reqID string, code int, body map[string]any) {
	f.out <- f.response(reqID, code, body)
}

// assembled joins the chunks the server received, in index order.
func (f *fakeAibotServer) assembled() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []byte
	for i := 0; i < len(f.parts); i++ {
		out = append(out, f.parts[i]...)
	}
	return out
}

func (f *fakeAibotServer) arrivalCount(idx int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.arrivals[idx]
}

func (f *fakeAibotServer) initFrames() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.inits...)
}

func (f *fakeAibotServer) postFrames() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.posts...)
}

// wireUpload starts a sender against the fake server plus the read pump that
// routes the server's responses back — the same routing dispatchFrame does.
func wireUpload(t *testing.T, srv *fakeAibotServer) *wsSender {
	t.Helper()
	sender := newWSSender(srv, testLogger())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, data, err := srv.ReadMessage()
			if err != nil {
				return
			}
			var env frameEnvelope
			if err := json.Unmarshal(data, &env); err != nil {
				continue
			}
			sender.routeResponse(env)
		}
	}()
	t.Cleanup(func() {
		srv.Close()
		<-done
	})
	return sender
}

// payload builds n bytes of non-repeating content, so a client that reorders
// or drops a chunk cannot accidentally reassemble the right file.
func payload(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(int64(n)))
	r.Read(b)
	return b
}

// ---- splitting ----

// TestChunksSplitAtTheProtocolLimit — 512KB before base64 and no more than 100
// of them. Every boundary here is one WeCom enforces and none of them is
// visible in a happy-path test.
func TestChunksSplitAtTheProtocolLimit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		size    int
		want    int
		wantErr bool
	}{
		{name: "one byte", size: 1, want: 1},
		{name: "exactly one chunk", size: mediaChunkBytes, want: 1},
		{name: "one byte past a chunk", size: mediaChunkBytes + 1, want: 2},
		{name: "empty", size: 0, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := splitMediaChunks(payload(tc.size))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("split %d bytes into %d chunks, want an error", tc.size, len(chunks))
				}
				return
			}
			if err != nil {
				t.Fatalf("split %d bytes: %v", tc.size, err)
			}
			if len(chunks) != tc.want {
				t.Fatalf("split %d bytes into %d chunks, want %d", tc.size, len(chunks), tc.want)
			}
			var joined []byte
			for _, c := range chunks {
				if len(c) > mediaChunkBytes {
					t.Fatalf("a chunk is %d bytes, past the %d limit", len(c), mediaChunkBytes)
				}
				joined = append(joined, c...)
			}
			if len(joined) != tc.size {
				t.Fatalf("chunks add up to %d bytes, want %d", len(joined), tc.size)
			}
		})
	}
}

// TestAFileTooBigForTheProtocolIsRefusedUpFront — 100 chunks of 512KB is the
// ceiling the wire imposes, and a file past it has to be turned away before a
// single byte goes out rather than half-uploaded and then rejected.
func TestAFileTooBigForTheProtocolIsRefusedUpFront(t *testing.T) {
	t.Parallel()

	if chunks, err := splitMediaChunks(make([]byte, maxMediaUploadBytes)); err != nil {
		t.Fatalf("the largest allowed file was refused: %v", err)
	} else if len(chunks) != maxMediaChunks {
		t.Fatalf("the largest allowed file split into %d chunks, want %d", len(chunks), maxMediaChunks)
	}
	_, err := splitMediaChunks(make([]byte, maxMediaUploadBytes+1))
	if !errors.Is(err, errMediaUploadTooLarge) {
		t.Fatalf("one byte past the ceiling gave %v, want errMediaUploadTooLarge", err)
	}
}

// ---- the handshake ----

// TestUploadRunsTheThreeStepHandshake is the whole feature at wire level: init
// declares the file, the chunks carry it, finish returns the media_id — and
// what the server put back together is the file we started with.
func TestUploadRunsTheThreeStepHandshake(t *testing.T) {
	srv := newFakeAibotServer()
	sender := wireUpload(t, srv)

	data := payload(mediaChunkBytes*2 + 7)
	mediaID, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind:     mediaTypeFile,
		Filename: "季度复盘.pptx",
		Data:     data,
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if mediaID != srv.mediaID {
		t.Errorf("media_id = %q, want the one finish returned (%q)", mediaID, srv.mediaID)
	}

	inits := srv.initFrames()
	if len(inits) != 1 {
		t.Fatalf("sent %d init frames, want 1", len(inits))
	}
	init := inits[0]
	if init["type"] != string(mediaTypeFile) {
		t.Errorf("init type = %v, want %q", init["type"], mediaTypeFile)
	}
	if init["filename"] != "季度复盘.pptx" {
		t.Errorf("init filename = %v, want the file's own name", init["filename"])
	}
	if got := init["total_size"]; got != float64(len(data)) {
		t.Errorf("init total_size = %v, want %d", got, len(data))
	}
	if got := init["total_chunks"]; got != float64(3) {
		t.Errorf("init total_chunks = %v, want 3", got)
	}
	if _, present := init["md5"]; present {
		t.Errorf("init carries an md5 we cannot verify our own definition of: %v", init["md5"])
	}

	for i := 0; i < 3; i++ {
		if n := srv.arrivalCount(i); n != 1 {
			t.Errorf("chunk %d arrived %d times, want exactly once", i, n)
		}
	}
	if !bytes.Equal(srv.assembled(), data) {
		t.Errorf("the server reassembled %d bytes, want the %d we sent", len(srv.assembled()), len(data))
	}
	if len(srv.finishes) != 1 {
		t.Fatalf("sent %d finish frames, want 1", len(srv.finishes))
	}
	if srv.finishes[0]["upload_id"] != srv.uploadID {
		t.Errorf("finish upload_id = %v, want the one init handed out", srv.finishes[0]["upload_id"])
	}
}

// TestChunkIndexStartsAtZeroAndCarriesANumber — the field table calls
// chunk_index a string and the worked example in the same document passes a
// number. The official SDK sends a number, so we do; a mismatch here is an
// upload the server rejects for a reason no log line explains.
func TestChunkIndexStartsAtZeroAndCarriesANumber(t *testing.T) {
	srv := newFakeAibotServer()
	sender := wireUpload(t, srv)

	if _, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind: mediaTypeImage, Filename: "chart.png", Data: payload(mediaChunkBytes + 1),
	}); err != nil {
		t.Fatalf("upload: %v", err)
	}
	srv.mu.Lock()
	order := append([]int(nil), srv.order...)
	srv.mu.Unlock()
	if len(order) != 2 {
		t.Fatalf("server saw %d chunks, want 2", len(order))
	}
	seen := map[int]bool{}
	for _, idx := range order {
		seen[idx] = true
	}
	if !seen[0] || !seen[1] {
		t.Errorf("chunk indexes were %v, want 0 and 1", order)
	}
}

// TestAChunkWhoseAckIsHeldDoesNotStallTheRest — the protocol says chunks may
// go out in any order and concurrently, and this is the test that makes us
// mean it. The server withholds chunk 0's answer until every other chunk has
// landed, so a client that uploads strictly in step never finishes.
func TestAChunkWhoseAckIsHeldDoesNotStallTheRest(t *testing.T) {
	srv := newFakeAibotServer()
	srv.hold = 0
	sender := wireUpload(t, srv)
	sender.ackTimeout = 5 * time.Second

	data := payload(mediaChunkBytes*3 + 11)
	done := make(chan error, 1)
	go func() {
		_, err := sender.uploadMedia(context.Background(), outboundMedia{
			Kind: mediaTypeFile, Filename: "big.bin", Data: data,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the upload never finished: chunk 0's ack was held and the rest waited behind it")
	}
	if !bytes.Equal(srv.assembled(), data) {
		t.Error("the server reassembled the wrong bytes")
	}
}

// TestARetriedChunkIsHarmless — chunks are idempotent by contract, which is
// what makes a retry the right answer to a lost ack. Losing one chunk of a
// hundred must not cost the whole file.
func TestARetriedChunkIsHarmless(t *testing.T) {
	srv := newFakeAibotServer()
	srv.silent = 1
	sender := wireUpload(t, srv)
	sender.ackTimeout = 150 * time.Millisecond

	data := payload(mediaChunkBytes*2 + 3)
	if _, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind: mediaTypeFile, Filename: "report.pdf", Data: data,
	}); err != nil {
		t.Fatalf("a single lost ack cost the whole upload: %v", err)
	}
	if n := srv.arrivalCount(1); n != 2 {
		t.Errorf("chunk 1 arrived %d times, want 2 (the lost one and the retry)", n)
	}
	if !bytes.Equal(srv.assembled(), data) {
		t.Error("the duplicate chunk corrupted the reassembled file")
	}
}

// TestUploadStopsWhenTheServerRefusesInit — no upload_id means there is
// nothing to send chunks against, so not one goes out.
func TestUploadStopsWhenTheServerRefusesInit(t *testing.T) {
	srv := newFakeAibotServer()
	srv.initErr = 40058
	sender := wireUpload(t, srv)

	_, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind: mediaTypeFile, Filename: "x.bin", Data: payload(1024),
	})
	if err == nil {
		t.Fatal("a refused init reported success")
	}
	if n := srv.arrivalCount(0); n != 0 {
		t.Errorf("sent %d chunks against an upload id we never got", n)
	}
}

// TestUploadFailsWhenTheServerRefusesFinish — the bytes are up but no media_id
// came back, so there is nothing to put in a message.
func TestUploadFailsWhenTheServerRefusesFinish(t *testing.T) {
	srv := newFakeAibotServer()
	srv.finishErr = 40058
	sender := wireUpload(t, srv)

	mediaID, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind: mediaTypeFile, Filename: "x.bin", Data: payload(1024),
	})
	if err == nil {
		t.Fatalf("a refused finish returned media_id %q as if it worked", mediaID)
	}
}

// TestUploadGivesUpOnAChunkTheServerKeepsRefusing — a rejection is not a lost
// ack: retrying it forever would hold the delivery open for nothing.
func TestUploadGivesUpOnAChunkTheServerKeepsRefusing(t *testing.T) {
	srv := newFakeAibotServer()
	srv.chunkErr = 40058
	sender := wireUpload(t, srv)

	if _, err := sender.uploadMedia(context.Background(), outboundMedia{
		Kind: mediaTypeFile, Filename: "x.bin", Data: payload(1024),
	}); err == nil {
		t.Fatal("a refused chunk reported success")
	}
	if n := srv.arrivalCount(0); n != 1 {
		t.Errorf("chunk 0 was sent %d times; a server that says no means no", n)
	}
}

// ---- the message that carries it ----

// TestMediaMessageFrames pins the four bodies. Each kind nests its media_id
// under its own name, and video additionally carries a title and description
// whose byte lengths WeCom caps.
func TestMediaMessageFrames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind mediaMsgType
		key  string
	}{
		{mediaTypeFile, "file"},
		{mediaTypeImage, "image"},
		{mediaTypeVoice, "voice"},
		{mediaTypeVideo, "video"},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			body, err := sendMsgMediaBody("T-alex", chatTypeSingleInt, mediaSend{
				Kind: tc.kind, MediaID: "MEDIA-1", Title: "季度复盘", Description: "本季度的复盘材料",
			})
			if err != nil {
				t.Fatalf("body: %v", err)
			}
			if body["msgtype"] != string(tc.kind) {
				t.Errorf("msgtype = %v, want %q", body["msgtype"], tc.kind)
			}
			nested, ok := body[tc.key].(map[string]any)
			if !ok {
				t.Fatalf("body has no %q object: %v", tc.key, body)
			}
			if nested["media_id"] != "MEDIA-1" {
				t.Errorf("%s.media_id = %v, want MEDIA-1", tc.key, nested["media_id"])
			}
			if tc.kind == mediaTypeVideo {
				if nested["title"] != "季度复盘" || nested["description"] != "本季度的复盘材料" {
					t.Errorf("video body lost its title/description: %v", nested)
				}
			} else if _, present := nested["title"]; present {
				t.Errorf("%s carries a title it has no field for: %v", tc.key, nested)
			}
		})
	}
}

// TestVideoTitleAndDescriptionAreCutToTheirByteCaps — 64 and 512 bytes, and a
// Chinese title runs into the first one at 21 characters. Cutting mid-rune
// would send WeCom broken UTF-8.
func TestVideoTitleAndDescriptionAreCutToTheirByteCaps(t *testing.T) {
	t.Parallel()

	body, err := sendMsgMediaBody("T-alex", chatTypeSingleInt, mediaSend{
		Kind:        mediaTypeVideo,
		MediaID:     "MEDIA-1",
		Title:       strings.Repeat("复", 40),
		Description: strings.Repeat("盘", 300),
	})
	if err != nil {
		t.Fatalf("body: %v", err)
	}
	video := body["video"].(map[string]any)
	title, _ := video["title"].(string)
	desc, _ := video["description"].(string)
	if len(title) > videoTitleBytes || len(desc) > videoDescriptionBytes {
		t.Fatalf("title %d bytes / description %d bytes, want at most %d / %d",
			len(title), len(desc), videoTitleBytes, videoDescriptionBytes)
	}
	if !strings.HasPrefix(strings.Repeat("复", 40), title) || title == "" {
		t.Errorf("title %q is not a prefix of what was asked for", title)
	}
	for _, s := range []string{title, desc} {
		if strings.ContainsRune(s, '�') {
			t.Errorf("%q was cut mid-rune", s)
		}
	}
}

// TestMediaGoesOutAsAPushByDefault — aibot_send_msg carries the chat id, so it
// reaches the right conversation whatever state the turn is in, and WeCom's own
// SDK is the one piece of working evidence that it takes media at all.
func TestMediaGoesOutAsAPushByDefault(t *testing.T) {
	srv := newFakeAibotServer()
	sender := wireUpload(t, srv)

	if err := sender.sendMedia(context.Background(), "T-alex", chatTypeSingleInt, "REQ-42", mediaSend{
		Kind: mediaTypeFile, MediaID: "MEDIA-1",
	}); err != nil {
		t.Fatalf("sendMedia: %v", err)
	}
	posts := srv.postFrames()
	if len(posts) != 1 {
		t.Fatalf("sent %d frames, want 1: %v", len(posts), posts)
	}
	if posts[0]["cmd"] != cmdSendMsg {
		t.Errorf("cmd = %v, want %q", posts[0]["cmd"], cmdSendMsg)
	}
	body := posts[0]["body"].(map[string]any)
	if body["chatid"] != "T-alex" || body["chat_type"] != float64(chatTypeSingleInt) {
		t.Errorf("the push was addressed %v, want the chat that asked", body)
	}
}

// TestARefusedPushFallsBackToAnsweringTheTurn — this is the half of the
// contradiction the field table argues for: if aibot_send_msg turns out not to
// take media, aibot_respond_msg's own msgtype table says it does, and the turn's
// req_id is what addresses it.
func TestARefusedPushFallsBackToAnsweringTheTurn(t *testing.T) {
	srv := newFakeAibotServer()
	srv.sendErr = 40058
	sender := wireUpload(t, srv)

	if err := sender.sendMedia(context.Background(), "T-alex", chatTypeSingleInt, "REQ-42", mediaSend{
		Kind: mediaTypeImage, MediaID: "MEDIA-1",
	}); err != nil {
		t.Fatalf("sendMedia: %v", err)
	}
	posts := srv.postFrames()
	if len(posts) != 2 {
		t.Fatalf("sent %d frames, want the refused push and the passive reply: %v", len(posts), posts)
	}
	if posts[0]["cmd"] != cmdSendMsg || posts[1]["cmd"] != cmdRespondMsg {
		t.Fatalf("frames went out as %v then %v, want %q then %q",
			posts[0]["cmd"], posts[1]["cmd"], cmdSendMsg, cmdRespondMsg)
	}
	body := posts[1]["body"].(map[string]any)
	if body["msgtype"] != "image" {
		t.Errorf("the passive reply carried msgtype %v, want image", body["msgtype"])
	}
	if _, addressed := body["chatid"]; addressed {
		t.Errorf("a passive reply is addressed by req_id, not chat id: %v", body)
	}
}

// TestARefusedPushWithNoTurnLeftIsAnError — a round whose req_id is gone has
// nothing to fall back to, and the caller has to hear that rather than assume
// the file arrived.
func TestARefusedPushWithNoTurnLeftIsAnError(t *testing.T) {
	srv := newFakeAibotServer()
	srv.sendErr = 40058
	sender := wireUpload(t, srv)

	err := sender.sendMedia(context.Background(), "T-alex", chatTypeGroupInt, "", mediaSend{
		Kind: mediaTypeFile, MediaID: "MEDIA-1",
	})
	if err == nil {
		t.Fatal("a refused push with no turn left reported success")
	}
	if n := len(srv.postFrames()); n != 1 {
		t.Fatalf("sent %d frames, want only the push that was refused", n)
	}
}

// TestMediaThatBothPathsRefuseIsAnError — the caller has to hear about it, so
// it can tell the user in words rather than leave them waiting for a file.
func TestMediaThatBothPathsRefuseIsAnError(t *testing.T) {
	srv := newFakeAibotServer()
	srv.respondErr = 40058
	srv.sendErr = 40058
	sender := wireUpload(t, srv)

	err := sender.sendMedia(context.Background(), "T-alex", chatTypeSingleInt, "REQ-42", mediaSend{
		Kind: mediaTypeFile, MediaID: "MEDIA-1",
	})
	if err == nil {
		t.Fatal("both paths refused the message and sendMedia reported success")
	}
	if !strings.Contains(fmt.Sprint(err), "40058") {
		t.Errorf("error %v does not name the server's errcode", err)
	}
}
