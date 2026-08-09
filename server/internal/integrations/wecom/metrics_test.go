package wecom

// metrics_test.go — the counters an operator reads.
//
// These assert the signal fires on the path it claims to describe. A counter
// wired to the wrong branch is worse than no counter: it reads as evidence the
// thing is being watched.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// countingMetrics records every call so a test can assert on the shape of what
// was reported rather than on a number in a registry.
type countingMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountingMetrics() *countingMetrics {
	return &countingMetrics{counts: map[string]int{}}
}

func (m *countingMetrics) bump(k string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[k]++
}

func (m *countingMetrics) get(k string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[k]
}

func (m *countingMetrics) RecordConnectFailure()       { m.bump("connect_failure") }
func (m *countingMetrics) RecordAuthFailure()          { m.bump("auth_failure") }
func (m *countingMetrics) RecordCallbackQueued()       { m.bump("callback_queued") }
func (m *countingMetrics) RecordCallbackQueueBlocked() { m.bump("callback_blocked") }
func (m *countingMetrics) RecordStreamFinished()       { m.bump("stream_finished") }
func (m *countingMetrics) RecordStreamFellBack()       { m.bump("stream_fell_back") }
func (m *countingMetrics) RecordWelcomeSent()          { m.bump("welcome_sent") }
func (m *countingMetrics) RecordWelcomeSkipped()       { m.bump("welcome_skipped") }
func (m *countingMetrics) RecordWelcomeFailed()        { m.bump("welcome_failed") }

// The reason is part of what is asserted, not a detail: "an attachment
// failed" and "the guard refused the address it resolved to" send an operator
// to two different places.
func (m *countingMetrics) RecordMediaFailure(reason string) { m.bump("media_failure:" + reason) }

var _ Metrics = (*countingMetrics)(nil)

// waitFor polls until cond holds or the budget runs out. The read loop runs on
// its own goroutine, so the counter it bumps is observed from here a moment
// after the event that caused it.
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

// A handshake the server refused and a socket that never came up are the two
// causes an operator has to tell apart: the first needs somebody to fix the
// installation and will repeat identically on every backoff until they do, the
// second usually clears on its own. One "cannot connect" number cannot say
// which, so they are counted separately.
func TestARefusedHandshakeIsNotCountedAsAConnectFailure(t *testing.T) {
	t.Parallel()

	mx := newCountingMetrics()
	c := &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
		dialer:         scriptedDialer{conn: &rejectingSubscribeConn{}},
		wsURL:          "wss://example.test/ws",
		metrics:        mx,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err == nil {
		t.Fatal("Connect succeeded against a server that refused the handshake")
	}
	if got := mx.get("auth_failure"); got != 1 {
		t.Fatalf("auth_failure = %d, want the rejection reported as a credential problem", got)
	}
	if got := mx.get("connect_failure"); got != 0 {
		t.Fatalf("connect_failure = %d for a socket that dialled and answered fine", got)
	}
}

// The other side of the same split: a dial that never lands is infrastructure,
// and must not show up as a credential problem — an operator who rotates a
// perfectly good secret because of it has been sent the wrong way.
func TestADialThatNeverLandsIsAConnectFailure(t *testing.T) {
	t.Parallel()

	mx := newCountingMetrics()
	c := &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
		dialer:         failingDialer{err: errors.New("dial tcp: connection refused")},
		wsURL:          "wss://example.test/ws",
		metrics:        mx,
	}

	if err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded with a dialer that always fails")
	}
	if got := mx.get("connect_failure"); got != 1 {
		t.Fatalf("connect_failure = %d, want the failed dial reported", got)
	}
	if got := mx.get("auth_failure"); got != 0 {
		t.Fatalf("auth_failure = %d for a socket that never opened; nobody's credentials are wrong", got)
	}
}

// A handshake that is written and then never answered is the same class as a
// failed dial. It is the one the subscribeTimeout produces, and it is the one
// most likely to be misread as "the secret is wrong".
func TestAHandshakeThatIsNeverAnsweredIsAConnectFailure(t *testing.T) {
	t.Parallel()

	mx := newCountingMetrics()
	c := &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
		dialer:         scriptedDialer{conn: &silentSubscribeConn{}},
		wsURL:          "wss://example.test/ws",
		metrics:        mx,
	}

	if err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against a server that never acked the handshake")
	}
	if got := mx.get("connect_failure"); got != 1 {
		t.Fatalf("connect_failure = %d, want the unanswered handshake reported", got)
	}
	if got := mx.get("auth_failure"); got != 0 {
		t.Fatalf("auth_failure = %d for a handshake the server never answered either way", got)
	}
}

// Backpressure is invisible from the outside: the read loop parks, the socket
// stops being drained, and the only outward sign is a connection WeCom
// eventually replaces. It has to be a number before it is an incident.
func TestAFullIngestQueueIsReportedAsBackpressure(t *testing.T) {
	t.Parallel()

	// One frame the worker holds, callbackQueueDepth more to fill the buffer,
	// and one that finds it full.
	total := callbackQueueDepth + 2
	frames := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		payload, err := json.Marshal(msgCallbackFrame(t, "text", "hello"))
		if err != nil {
			t.Fatalf("marshal frame: %v", err)
		}
		frames = append(frames, payload)
	}
	conn := &floodConn{
		frames:    frames,
		delivered: make(chan struct{}),
		unblock:   make(chan struct{}),
	}

	// The first callback's handler parks until the queue behind it is full.
	// Every later one returns immediately.
	var calls atomic.Int32
	release := make(chan struct{})

	mx := newCountingMetrics()
	c := &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler: func(context.Context, channel.InboundMessage) error {
			if calls.Add(1) == 1 {
				<-release
			}
			return nil
		},
		dialer:  scriptedDialer{conn: conn},
		wsURL:   "wss://example.test/ws",
		metrics: mx,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()

	waitFor(t, "the read loop to report a full ingest queue", func() bool {
		return mx.get("callback_blocked") >= 1
	})
	// The buffer really did fill, and the frames still outstanding are held by
	// a parked read loop rather than dropped. (Whether the worker had already
	// taken the first frame when the buffer filled is a race, so the count at
	// this instant is one of two values, not one.)
	queuedWhileBlocked := mx.get("callback_queued")
	if queuedWhileBlocked < callbackQueueDepth {
		t.Fatalf("callback_queued = %d when the queue reported full, want at least %d", queuedWhileBlocked, callbackQueueDepth)
	}
	if queuedWhileBlocked >= total {
		t.Fatalf("callback_queued = %d while the read loop is parked; it cannot have handed over all %d", queuedWhileBlocked, total)
	}

	close(release)
	waitFor(t, "the queue to drain", func() bool {
		return mx.get("callback_queued") == total
	})

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Connect did not return after ctx cancel")
	}
}

// A channel built without a sink must not be a nil-pointer dereference on the
// read loop. This is the deployment with /metrics turned off, which is the one
// least likely to be exercised anywhere else.
func TestAChannelWithNoSinkStillRuns(t *testing.T) {
	t.Parallel()

	c := &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
		dialer:         scriptedDialer{conn: &rejectingSubscribeConn{}},
		wsURL:          "wss://example.test/ws",
	}
	if err := c.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against a server that refused the handshake")
	}
}

// The factory is the other way a nil sink reaches the read loop, and it has to
// substitute the no-op rather than store the nil.
func TestTheFactorySubstitutesTheNoOpSink(t *testing.T) {
	t.Parallel()

	factory := newWecomFactory(ChannelDeps{Credentials: fixedCredentials{}})
	ch, err := factory(channel.Config{
		ID:      mustTestUUID(t),
		Raw:     []byte(`{"bot_id":"bot-1"}`),
		Handler: func(context.Context, channel.InboundMessage) error { return nil },
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	wc, ok := ch.(*wecomChannel)
	if !ok {
		t.Fatalf("factory returned %T, want *wecomChannel", ch)
	}
	if wc.metrics == nil {
		t.Fatal("factory stored a nil sink; the read loop would dereference it")
	}
	wc.metrics.RecordConnectFailure() // must not panic
}

// fixedCredentials is a CredentialsResolver that hands back whatever bot id the
// installation config carried, with no decryption.
type fixedCredentials struct{}

func (fixedCredentials) Credentials(inst Installation) (InstallationCredentials, error) {
	return InstallationCredentials{BotID: inst.BotID, Secret: "secret-1"}, nil
}

// rejectingSubscribeConn completes the handshake exchange and answers it with a
// non-zero errcode — a bot whose secret was rotated, or one that was deleted.
type rejectingSubscribeConn struct {
	mu       sync.Mutex
	ack      []byte
	unblock  chan struct{}
	closeOne sync.Once
}

func (c *rejectingSubscribeConn) WriteMessage(_ int, data []byte) error {
	var env frameEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Cmd == cmdSubscribe {
		ack, _ := json.Marshal(frameEnvelope{
			Headers: frameHeaders{ReqID: env.Headers.ReqID},
			ErrCode: 40001,
			ErrMsg:  "invalid credential",
		})
		c.mu.Lock()
		c.ack = ack
		c.mu.Unlock()
	}
	return nil
}

func (c *rejectingSubscribeConn) ReadMessage() (int, []byte, error) {
	c.mu.Lock()
	if c.ack != nil {
		ack := c.ack
		c.ack = nil
		c.mu.Unlock()
		return websocket.TextMessage, ack, nil
	}
	c.mu.Unlock()
	return 0, nil, errors.New("closed")
}

func (c *rejectingSubscribeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *rejectingSubscribeConn) SetWriteDeadline(time.Time) error { return nil }
func (c *rejectingSubscribeConn) Close() error {
	c.closeOne.Do(func() {
		if c.unblock != nil {
			close(c.unblock)
		}
	})
	return nil
}

// silentSubscribeConn accepts the handshake write and then never answers —
// the socket the subscribeTimeout exists for.
type silentSubscribeConn struct{}

func (silentSubscribeConn) WriteMessage(int, []byte) error { return nil }
func (silentSubscribeConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("i/o timeout")
}
func (silentSubscribeConn) SetReadDeadline(time.Time) error  { return nil }
func (silentSubscribeConn) SetWriteDeadline(time.Time) error { return nil }
func (silentSubscribeConn) Close() error                     { return nil }

// failingDialer never opens a socket.
type failingDialer struct{ err error }

func (d failingDialer) DialContext(context.Context, string, http.Header) (wsConn, *http.Response, error) {
	return nil, nil, d.err
}

// ---- the bubble ----
//
// The bubble is the one feature whose failure is invisible from the outside:
// the answer arrives either way, just as a separate message instead of in the
// bubble the question opened. Nobody files a ticket about that, so the ratio
// between these two counters is the only thing that can say the bubble has
// stopped working — after a WeCom-side change to the stream frame, say.

func TestAnAnswerThatLandsInTheBubbleIsCountedAsFinished(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	rig := newBubbleRig(t)
	rig.senders.WithMetrics(mx)

	rig.ran(t, "REQ-M1", 1, "task-1")
	rig.answer(t, "the agent reply", "task-1")

	if got := mx.get("stream_finished"); got != 1 {
		t.Fatalf("stream_finished = %d, want 1 — an answer sealed the bubble and nothing counted it, so a dashboard has no denominator to read fall-backs against", got)
	}
	if got := mx.get("stream_fell_back"); got != 0 {
		t.Fatalf("stream_fell_back = %d, want 0 — a bubble that worked was counted as a failure", got)
	}
}

func TestAnAnswerSentAsANewMessageIsCountedAsFallenBack(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	rig := newBubbleRig(t)
	rig.senders.WithMetrics(mx)
	rig.conn.refuseClosingCode = errcodeStreamExpired

	rig.ran(t, "REQ-M2", 1, "task-1")
	rig.answer(t, "the agent reply", "task-1")

	// The answer still reaches the user, which is why nobody reports this.
	if pushes := rig.conn.pushes(t); len(pushes) != 1 {
		t.Fatalf("the refused bubble delivered %d messages, want 1 — this test's premise is gone", len(pushes))
	}
	if got := mx.get("stream_fell_back"); got != 1 {
		t.Fatalf("stream_fell_back = %d, want 1 — every bubble on this deployment could be refusing its closing frame and nothing would say so", got)
	}
	if got := mx.get("stream_finished"); got != 0 {
		t.Fatalf("stream_finished = %d, want 0 — a refused closing frame was counted as a bubble that worked", got)
	}
}

// ---- the greeting ----
//
// A greeting is never retried and its failure is never told to anybody: the
// req_id it answers is spent, and the person is left looking at an empty
// window. The counter is the only trace.

func TestAGreetingThatWentOutIsCountedAsSent(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	lookup := &fakeWelcomeLookup{binding: db.ChannelUserBinding{MulticaUserID: mustTestUUID(t)}}
	c, conn, sender := welcomeRig(t, lookup, nil)
	c.metrics = mx

	c.handleEnterChat(context.Background(), enterChatFrame(t, "req-m1", "single", "T-alex"), sender, slog.Default())

	if welcomeSaid(t, conn) == "" {
		t.Fatal("no greeting was written; this test's premise is gone")
	}
	if got := mx.get("welcome_sent"); got != 1 {
		t.Fatalf("welcome_sent = %d, want 1 — with nothing counted here, 'is the bot greeting people?' has no answer at all", got)
	}
}

// A group is skipped on purpose, and it must not read as a failure — the
// number an operator alerts on is failed, and a busy group would bury it.
func TestAGroupIsCountedAsSkippedAndNotAsFailed(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	c, conn, sender := welcomeRig(t, &fakeWelcomeLookup{}, nil)
	c.metrics = mx

	c.handleEnterChat(context.Background(), enterChatFrame(t, "req-m2", "group", "T-alex"), sender, slog.Default())

	if said := welcomeSaid(t, conn); said != "" {
		t.Fatalf("the bot greeted a group: %q", said)
	}
	if got := mx.get("welcome_skipped"); got != 1 {
		t.Fatalf("welcome_skipped = %d, want 1 — a deliberate skip that counts nothing is indistinguishable from the handler never running", got)
	}
	if got := mx.get("welcome_failed"); got != 0 {
		t.Fatalf("welcome_failed = %d, want 0 — a deliberate skip counted as a failure makes the alert fire on group traffic", got)
	}
}

// The window closed: the write was refused, the req_id is spent, and the
// person who opened the chat is looking at nothing.
func TestAGreetingThatCouldNotBeWrittenIsCountedAsFailed(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	lookup := &fakeWelcomeLookup{binding: db.ChannelUserBinding{MulticaUserID: mustTestUUID(t)}}
	c, conn, _ := welcomeRig(t, lookup, nil)
	c.metrics = mx
	// A socket that takes the frame and never acks it. respondWelcome waits
	// for the reply that will not come and gives up with the handler's
	// deadline — the shape of a window that closed.
	dead := &recordingConn{}
	sender := newWSSender(dead, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.handleEnterChat(ctx, enterChatFrame(t, "req-m3", "single", "T-alex"), sender, slog.Default())

	if said := welcomeSaid(t, conn); said != "" {
		t.Fatalf("the greeting reached the wrong socket: %q", said)
	}
	if got := mx.get("welcome_failed"); got != 1 {
		t.Fatalf("welcome_failed = %d, want 1 — a greeting that never landed leaves no other trace: no retry, no user-visible error, one WARN line", got)
	}
	if got := mx.get("welcome_sent"); got != 0 {
		t.Fatalf("welcome_sent = %d, want 0 — a greeting that was never delivered was counted as delivered", got)
	}
}

// ---- attachments ----
//
// A failed attachment leaves the message body saying "[Image]", so the agent
// answers as though it had been shown a picture it never received. The sender
// gets one apology; the operator gets this, and the reason is the whole point
// of it — a blocked address is a configuration fault on this side, and it is
// the one an apology cannot describe.

func TestABlockedMediaAddressIsCountedUnderItsOwnReason(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	senders := newSendersRegistry().WithMetrics(mx)
	storage := &fakeMediaStorage{}
	r := newTestResolver(storage, newFakeMediaLedger(storage), senders)
	// The production client, which refuses a host that resolves to a
	// non-public address — the guard this counter is about.
	r.http = newMediaHTTPClient(mediaGuard{})

	srv := cosServer(t, []byte("never fetched"), "")
	defer srv.Close()

	r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, pgtype.UUID{}, mustTestUUID(t),
		mediaMessage(t, "image", map[string]any{
			"image": map[string]any{"url": srv.URL + "/a.enc", "aeskey": testAESKey},
		}))

	if got := mx.get("media_failure:blocked_address"); got != 1 {
		t.Fatalf("media_failure{reason=blocked_address} = %d, want 1 — the SSRF guard refusing every attachment on this deployment looks exactly like nobody sending any", got)
	}
	if got := mx.get("media_failure:unreadable"); got != 0 {
		t.Fatalf("a blocked address was counted as unreadable (%d) — that sends the operator to WeCom when the fix is MULTICA_WECOM_MEDIA_ALLOW_CIDRS", got)
	}
}

func TestAnOversizeAttachmentIsCountedApartFromAnUnreadableOne(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	senders := newSendersRegistry().WithMetrics(mx)
	storage := &fakeMediaStorage{}
	r := newTestResolver(storage, newFakeMediaLedger(storage), senders)

	big := cosServer(t, make([]byte, maxMediaBytes+1), "")
	defer big.Close()
	r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, pgtype.UUID{}, mustTestUUID(t),
		mediaMessage(t, "file", map[string]any{
			"file": map[string]any{"url": big.URL + "/big.enc", "aeskey": testAESKey},
		}))

	// A url that answers 404 — the ordinary five-minute link that lapsed.
	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer gone.Close()
	r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, pgtype.UUID{}, mustTestUUID(t),
		mediaMessage(t, "file", map[string]any{
			"file": map[string]any{"url": gone.URL + "/gone.enc", "aeskey": testAESKey},
		}))

	if got := mx.get("media_failure:too_large"); got != 1 {
		t.Fatalf("media_failure{reason=too_large} = %d, want 1", got)
	}
	if got := mx.get("media_failure:unreadable"); got != 1 {
		t.Fatalf("media_failure{reason=unreadable} = %d, want 1 — an expired link and a file over the ceiling need different answers, and one number cannot give either", got)
	}
}

// One message, four attachments, all of them dead: the sender is told once,
// deliberately. The operator's number is not the apology's number — "how much
// media is this deployment losing" is four, and collapsing it would understate
// an outage by however many files people happen to attach at a time.
func TestEveryLostAttachmentIsCountedEvenThoughTheSenderIsToldOnce(t *testing.T) {
	t.Parallel()
	mx := newCountingMetrics()
	senders := newSendersRegistry().WithMetrics(mx)
	storage := &fakeMediaStorage{}
	r := newTestResolver(storage, newFakeMediaLedger(storage), senders)

	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer gone.Close()

	items := make([]any, 0, 4)
	for i := 0; i < 4; i++ {
		items = append(items, map[string]any{
			"msgtype": "image",
			"image":   map[string]any{"url": gone.URL + "/" + strconv.Itoa(i) + ".enc", "aeskey": testAESKey},
		})
	}
	r.ResolveMedia(context.Background(), mediaInstallation(), engine.ResolvedIdentity{}, pgtype.UUID{}, mustTestUUID(t),
		mediaMessage(t, "mixed", map[string]any{"mixed": map[string]any{"msg_item": items}}))

	if got := mx.get("media_failure:unreadable"); got != 4 {
		t.Fatalf("media_failure{reason=unreadable} = %d, want 4 — counting per message instead of per attachment understates an outage by however many files people attach at once", got)
	}
}
