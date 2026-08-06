package wecom

// channel_lifecycle_test.go — what Connect does on each way out, and what it
// leaves behind. The Supervisor holds the WS lease for as long as Connect is
// running, so the difference between "returned an error" and "returned nil"
// decides whether this bot reconnects, and a sender left in the registry after
// a drop is a live-looking handle onto a dead socket.
//
// The ping-goroutine deadlock regression lives next door in
// wecom_channel_test.go; this file covers the other exits.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// errSocketClosed is what a real gorilla conn returns once something has
// closed it underneath the read — the watchdog's whole purpose.
var errSocketClosed = errors.New("use of closed network connection")

// scriptConn is a programmable socket: writes are recorded, reads are served
// from a queue, and Close (from the ctx watchdog or the deferred teardown)
// unblocks a pending read the way a real one does. The subscribe handshake is
// auto-acked so tests only script what comes after it.
type scriptConn struct {
	mu     sync.Mutex
	writes []map[string]any

	frames chan []byte

	closeOnce sync.Once
	closed    chan struct{}

	// subscribeErrCode is what the auto-ack reports. 0 accepts the
	// connection; anything else is a rejected bot_id / secret.
	subscribeErrCode int
}

func newScriptConn() *scriptConn {
	return &scriptConn{frames: make(chan []byte, 16), closed: make(chan struct{})}
}

func (c *scriptConn) WriteMessage(_ int, data []byte) error {
	select {
	case <-c.closed:
		return errSocketClosed
	default:
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	c.mu.Lock()
	c.writes = append(c.writes, f)
	c.mu.Unlock()

	if f["cmd"] == cmdSubscribe {
		hdr, _ := f["headers"].(map[string]any)
		reqID, _ := hdr["req_id"].(string)
		ack, _ := json.Marshal(map[string]any{
			"headers": frameHeaders{ReqID: reqID},
			"errcode": c.subscribeErrCode,
			"errmsg":  "scripted",
		})
		c.push(ack)
	}
	return nil
}

func (c *scriptConn) ReadMessage() (int, []byte, error) {
	select {
	case <-c.closed:
		return 0, nil, errSocketClosed
	case b := <-c.frames:
		return websocket.TextMessage, b, nil
	}
}

func (c *scriptConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptConn) SetWriteDeadline(time.Time) error { return nil }

func (c *scriptConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// push queues a frame for the read loop.
func (c *scriptConn) push(b []byte) {
	select {
	case c.frames <- b:
	case <-c.closed:
	}
}

func (c *scriptConn) pushJSON(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	c.push(b)
}

func (c *scriptConn) cmds() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, f := range c.writes {
		s, _ := f["cmd"].(string)
		out = append(out, s)
	}
	return out
}

type scriptDialer struct {
	conn    *scriptConn
	err     error
	dialled string
}

func (d *scriptDialer) DialContext(_ context.Context, url string, _ http.Header) (wsConn, *http.Response, error) {
	d.dialled = url
	if d.err != nil {
		return nil, nil, d.err
	}
	return d.conn, nil, nil
}

// connectUnder starts Connect on its own goroutine and hands back the exit.
func connectUnder(t *testing.T, c *wecomChannel, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()
	return done
}

// waitExit fails the test rather than hanging the suite.
func waitExit(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return")
		return nil
	}
}

// waitFor polls until cond holds, so tests do not race the goroutines Connect
// starts (the flush, the ping loop, the watchdog).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func liveChannel(conn *scriptConn) *wecomChannel {
	return &wecomChannel{
		installationID: uuidOf(1),
		botID:          "wb-1",
		secret:         "s3cret",
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
		dialer:         &scriptDialer{conn: conn},
		wsURL:          "wss://example.invalid/ws",
		logger:         testLogger(),
	}
}

// TestConnectReturnsNilOnAGracefulShutdown — the Supervisor reads a nil as
// "you asked for this", an error as "retry me". A clean stop must not look
// like a failure.
func TestConnectReturnsNilOnAGracefulShutdown(t *testing.T) {
	conn := newScriptConn()
	c := liveChannel(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := connectUnder(t, c, ctx)

	waitFor(t, "the subscribe handshake", func() bool {
		return len(conn.cmds()) > 0
	})
	cancel()

	if err := waitExit(t, done); err != nil {
		t.Fatalf("a cancelled run must return nil, got %v", err)
	}
}

// TestConnectSurfacesASubscribeRejection — a wrong secret is not a transient
// failure, but it still has to come back as an error the operator can see in
// the log rather than a silent nil.
func TestConnectSurfacesASubscribeRejection(t *testing.T) {
	conn := newScriptConn()
	conn.subscribeErrCode = 40001
	c := liveChannel(conn)
	reg := NewSendersRegistry()
	c.senders = reg

	err := waitExit(t, connectUnder(t, c, context.Background()))
	if err == nil {
		t.Fatal("a rejected subscribe must return an error")
	}
	if !strings.Contains(err.Error(), "40001") {
		t.Errorf("error %q should carry the errcode the platform sent", err)
	}
	if reg.get(uuidOf(1)) != nil {
		t.Error("a connection that never authenticated must not be published as a live sender")
	}
}

// TestConnectPublishesThenRetiresTheSender — the OutboundReplier finds the
// live socket through the registry, so an entry outliving its connection is a
// handle onto nothing.
func TestConnectPublishesThenRetiresTheSender(t *testing.T) {
	conn := newScriptConn()
	c := liveChannel(conn)
	reg := NewSendersRegistry()
	c.senders = reg

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUnder(t, c, ctx)

	waitFor(t, "the sender to be published", func() bool { return reg.get(uuidOf(1)) != nil })

	cancel()
	if err := waitExit(t, done); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if reg.get(uuidOf(1)) != nil {
		t.Fatal("the sender must be cleared when the connection ends")
	}
}

// TestConnectDeliversWhatWasHeldDuringTheOutage — the reconnect is the moment
// the queued replies are supposed to arrive.
func TestConnectDeliversWhatWasHeldDuringTheOutage(t *testing.T) {
	reg := NewSendersRegistry()
	if err := reg.send(uuidOf(1), pendingSend{ChatID: "T-alex", ChatType: chatTypeSingleInt, Content: "答案是 42"}); err != nil {
		t.Fatalf("send while down: %v", err)
	}

	conn := newScriptConn()
	c := liveChannel(conn)
	c.senders = reg

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUnder(t, c, ctx)

	waitFor(t, "the held reply to go out", func() bool {
		for _, cmd := range conn.cmds() {
			if cmd == cmdSendMsg {
				return true
			}
		}
		return false
	})

	cancel()
	_ = waitExit(t, done)
}

// TestDisconnectedEventEndsTheRun — WeCom allows one connection per bot, so
// this frame says another replica took over. Returning an error is what hands
// the run back to the Supervisor for backoff instead of sitting on a socket
// the platform has already abandoned.
func TestDisconnectedEventEndsTheRun(t *testing.T) {
	conn := newScriptConn()
	c := liveChannel(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUnder(t, c, ctx)

	waitFor(t, "the subscribe handshake", func() bool { return len(conn.cmds()) > 0 })
	conn.pushJSON(map[string]any{
		"cmd":     cmdEventCallback,
		"headers": frameHeaders{ReqID: "r"},
		"body":    map[string]any{"event": map[string]any{"eventtype": eventDisconnected}},
	})

	err := waitExit(t, done)
	if err == nil {
		t.Fatal("being displaced must return an error so the Supervisor reconnects")
	}
	if !strings.Contains(err.Error(), "superseded") {
		t.Errorf("error = %q, want it to name the displacement", err)
	}
}

// TestReadLoopSurvivesAMalformedFrame — one bad frame is not a reason to drop
// a socket that is otherwise fine.
func TestReadLoopSurvivesAMalformedFrame(t *testing.T) {
	conn := newScriptConn()
	got := make(chan channel.InboundMessage, 1)
	c := liveChannel(conn)
	c.handler = func(_ context.Context, m channel.InboundMessage) error {
		got <- m
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUnder(t, c, ctx)

	waitFor(t, "the subscribe handshake", func() bool { return len(conn.cmds()) > 0 })
	conn.push([]byte("{not json"))
	conn.push([]byte(`{"cmd":"aibot_msg_callback","headers":{"req_id":"r"},"body":{"bad_body":`))
	conn.pushJSON(map[string]any{
		"cmd":     cmdMsgCallback,
		"headers": frameHeaders{ReqID: "r2"},
		"body": map[string]any{
			"msgid": "MSGID-009", "chattype": "single",
			"from":    map[string]any{"userid": "T-alex"},
			"msgtype": "text", "text": map[string]any{"content": "还在吗"},
		},
	})

	select {
	case m := <-got:
		if m.Text != "还在吗" {
			t.Fatalf("handler got %q", m.Text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the read loop died on a malformed frame")
	}

	cancel()
	if err := waitExit(t, done); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}

// TestHandlerFailureEndsTheRun — a failing handler is an infra problem
// (database down, bus wedged). Escalating drops the socket so the Supervisor
// retries the whole thing rather than reading messages into a broken pipeline.
func TestHandlerFailureEndsTheRun(t *testing.T) {
	boom := errors.New("ingest failed")
	conn := newScriptConn()
	c := liveChannel(conn)
	c.handler = func(context.Context, channel.InboundMessage) error { return boom }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectUnder(t, c, ctx)

	waitFor(t, "the subscribe handshake", func() bool { return len(conn.cmds()) > 0 })
	conn.pushJSON(map[string]any{
		"cmd":     cmdMsgCallback,
		"headers": frameHeaders{ReqID: "r"},
		"body": map[string]any{
			"msgid": "MSGID-010", "chattype": "single",
			"from":    map[string]any{"userid": "T-alex"},
			"msgtype": "text", "text": map[string]any{"content": "在"},
		},
	})

	if err := waitExit(t, done); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the handler error handed back to the Supervisor", err)
	}
}

// TestConnectRefusesAnUnusableConfiguration — these are boot mistakes, and a
// Channel that dials with no credentials just burns the reconnect budget.
func TestConnectRefusesAnUnusableConfiguration(t *testing.T) {
	cases := []struct {
		name string
		c    *wecomChannel
	}{
		{"no handler", &wecomChannel{botID: "wb-1", secret: "s"}},
		{"no bot id", &wecomChannel{secret: "s", handler: func(context.Context, channel.InboundMessage) error { return nil }}},
		{"no secret", &wecomChannel{botID: "wb-1", handler: func(context.Context, channel.InboundMessage) error { return nil }}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialer := &scriptDialer{conn: newScriptConn()}
			tc.c.dialer = dialer
			tc.c.logger = testLogger()
			if err := tc.c.Connect(context.Background()); err == nil {
				t.Fatal("want a configuration error")
			}
			if dialer.dialled != "" {
				t.Error("a misconfigured channel must not reach the network")
			}
		})
	}
}

// TestConnectReportsADialFailure — the ordinary transient case; the Supervisor
// backs off on it.
func TestConnectReportsADialFailure(t *testing.T) {
	refused := errors.New("connection refused")
	c := liveChannel(newScriptConn())
	c.dialer = &scriptDialer{err: refused}

	err := c.Connect(context.Background())
	if !errors.Is(err, refused) {
		t.Fatalf("err = %v, want the dial error", err)
	}
}

// TestConnectDialsTheDefaultEndpointWhenUnset — the URL override is test-only
// plumbing; production has to land on WeCom's published endpoint.
func TestConnectDialsTheDefaultEndpointWhenUnset(t *testing.T) {
	dialer := &scriptDialer{err: errors.New("stop here")}
	c := liveChannel(newScriptConn())
	c.dialer, c.wsURL = dialer, ""

	_ = c.Connect(context.Background())
	if dialer.dialled != DefaultWSURL {
		t.Fatalf("dialled %q, want %q", dialer.dialled, DefaultWSURL)
	}
}

// ---- frame dispatch, without the socket ----

// TestServerPingIsAnswered — WeCom pings rarely, but an unanswered ping is a
// socket the platform stops trusting.
func TestServerPingIsAnswered(t *testing.T) {
	c := &wecomChannel{botID: "wb-1"}
	conn := &recordingConn{}
	sender := newWSSender(conn, testLogger())

	err := c.dispatchFrame(context.Background(),
		frameEnvelope{Cmd: cmdServerPing, Headers: frameHeaders{ReqID: "r-9"}}, sender, testLogger())
	if err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.frames) != 1 {
		t.Fatalf("want one reply frame, got %d", len(conn.frames))
	}
	f := conn.frames[0]
	if f["cmd"] != cmdPong {
		t.Errorf("replied with %v, want a pong", f["cmd"])
	}
	hdr, _ := f["headers"].(map[string]any)
	if hdr["req_id"] != "r-9" {
		t.Errorf("pong req_id = %v, want the ping's", hdr["req_id"])
	}
}

// TestNonFatalFramesDoNotEndTheRun — an ack carrying an errcode (rate limit,
// unwritable chat) is worth a log line, not a reconnect.
func TestNonFatalFramesDoNotEndTheRun(t *testing.T) {
	c := &wecomChannel{botID: "wb-1"}
	sender := newWSSender(&recordingConn{}, testLogger())
	ctx := context.Background()

	frames := []frameEnvelope{
		{Cmd: cmdPong},
		{Cmd: "", ErrCode: 45009, ErrMsg: "api freq out of limit"},
		{Cmd: "", ErrCode: 0},
		{Cmd: cmdEventCallback, Body: json.RawMessage(`{"event":{"eventtype":"enter_chat"}}`)},
		{Cmd: cmdEventCallback, Body: json.RawMessage(`{"event":{"eventtype":"feedback_event"}}`)},
		{Cmd: cmdEventCallback, Body: json.RawMessage(`not json`)},
		{Cmd: cmdMsgCallback, Body: json.RawMessage(`not json`)},
		{Cmd: "some_future_cmd"},
	}
	for i, f := range frames {
		if err := c.dispatchFrame(ctx, f, sender, testLogger()); err != nil {
			t.Fatalf("frame %d (%q) ended the run: %v", i, f.Cmd, err)
		}
	}
}

// TestUnsupportedTypeWithoutADedupKeyStaysSilent — no msgid means no way to
// promise "at most once", and a receipt storm on redelivery is worse than
// silence.
func TestUnsupportedTypeWithoutADedupKeyStaysSilent(t *testing.T) {
	cases := []struct {
		name  string
		msgID string
		setup func(c *wecomChannel)
	}{
		{"no msgid on the frame", "", func(*wecomChannel) {}},
		{"no deduper wired", "MSGID-011", func(c *wecomChannel) { c.dedup = nil }},
		{"no installation id", "MSGID-012", func(c *wecomChannel) { c.installationID = pgtype.UUID{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error { return nil })
			tc.setup(c)
			sender := newWSSender(conn, testLogger())

			if err := c.dispatchFrame(context.Background(), mediaFrame("location", tc.msgID), sender, testLogger()); err != nil {
				t.Fatalf("dispatchFrame: %v", err)
			}
			if n := len(conn.sends()); n != 0 {
				t.Fatalf("sent %d receipts without an at-most-once guarantee", n)
			}
		})
	}
}

// TestCapabilitiesAndTypeStayHonest — the mask is a promise about what the
// adapter can actually do. CapVoice because WeCom transcribes voice notes
// itself, CapAttachment because photos and files are downloaded, decrypted
// and bound to the message. CapRichCard is the one still missing: nothing
// here builds a template_card.
func TestCapabilitiesAndTypeStayHonest(t *testing.T) {
	c := &wecomChannel{}
	if c.Type() != TypeWecom {
		t.Errorf("Type = %q", c.Type())
	}
	want := channel.CapText | channel.CapVoice | channel.CapAttachment |
		channel.CapTypingIndicator | channel.CapMessageEdit
	if c.Capabilities() != want {
		t.Errorf("Capabilities = %v, want %v", c.Capabilities(), want)
	}
	if c.Capabilities().Has(channel.CapRichCard) {
		t.Error("no card is ever built; declaring rich cards would make callers degrade the wrong way")
	}
	if err := c.Disconnect(context.Background()); err != nil {
		t.Errorf("Disconnect: %v", err)
	}
}
