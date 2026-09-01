package wecom

// connect_registers_sender_test.go — that a live connection is reachable from
// the outbound side.
//
// This exists because losing it costs nothing that any other check can see.
// sendersRegistry is package-private and set() has exactly one production
// caller — the three lines in Connect. Delete them and the package still
// compiles, go vet stays quiet, every existing test stays green, and the
// process still logs "wecom integration enabled" and "subscribe ok" on boot.
// What actually happens is that every outbound push resolves to nil: the
// bubble never opens, the answer never leaves, failure notices never leave,
// attachments never leave. The bot receives messages and never speaks.
//
// It happened. A merge that removed the outbound-queue consumer took these
// lines with it, because they sat between the consumer's call site and the
// heartbeat block. Nothing failed. The whole suite was green, including
// -race and the CI entry point, and a boot smoke test printed the two
// reassuring log lines above.
//
// So the assertion is deliberately about the registry rather than about any
// message: it is the narrowest thing that is false the moment the wiring is
// gone, and it does not depend on which caller happens to reach for a sender
// first.

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// parkedConn answers the subscribe frame and then parks on read, which holds
// Connect inside its live-connection body for as long as the test needs.
type parkedConn struct {
	mu      sync.Mutex
	pending [][]byte
	release chan struct{}
	closed  bool
}

func (c *parkedConn) WriteMessage(_ int, data []byte) error {
	var env struct {
		Cmd     string `json:"cmd"`
		Headers struct {
			ReqID string `json:"req_id"`
		} `json:"headers"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	// Only the subscribe frame gets an ack; a ping needs none for this test.
	if env.Cmd == cmdSubscribe {
		ack, _ := json.Marshal(map[string]any{
			"headers": map[string]string{"req_id": env.Headers.ReqID},
			"errcode": 0,
			"errmsg":  "ok",
		})
		c.mu.Lock()
		c.pending = append(c.pending, ack)
		c.mu.Unlock()
	}
	return nil
}

func (c *parkedConn) ReadMessage() (int, []byte, error) {
	for {
		c.mu.Lock()
		if len(c.pending) > 0 {
			msg := c.pending[0]
			c.pending = c.pending[1:]
			c.mu.Unlock()
			return websocketTextMessage, msg, nil
		}
		c.mu.Unlock()
		select {
		case <-c.release:
			return 0, nil, http.ErrServerClosed
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (c *parkedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *parkedConn) SetWriteDeadline(time.Time) error { return nil }
func (c *parkedConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.release)
	}
	return nil
}

type parkedDialer struct{ conn *parkedConn }

func (d *parkedDialer) DialContext(context.Context, string, http.Header) (wsConn, *http.Response, error) {
	return d.conn, nil, nil
}

// A connection that has authenticated must be findable by installation id
// from the outbound side, and must be gone once it ends.
func TestConnectRegistersTheSenderWhileTheConnectionIsLive(t *testing.T) {
	t.Parallel()
	reg := newSendersRegistry()
	instID := mustTestUUID(t)
	conn := &parkedConn{release: make(chan struct{})}

	c := &wecomChannel{
		botID:          "bot-1",
		secret:         "secret-1",
		installationID: instID,
		senders:        reg,
		dialer:         &parkedDialer{conn: conn},
		handler:        func(context.Context, channel.InboundMessage) error { return nil },
	}

	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- c.Connect(ctx) }()

	// Wait for the registration rather than for a fixed delay: Connect has to
	// dial, subscribe and read one ack before it gets there.
	deadline := time.Now().After
	for start := time.Now(); ; {
		if reg.get(instID) != nil {
			break
		}
		if deadline(start.Add(5 * time.Second)) {
			cancel()
			<-done
			t.Fatal("the registry is empty while the connection is live — every " +
				"outbound push would resolve to nil, so the bot would receive " +
				"messages and never reply")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	conn.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after the connection ended")
	}

	// And the other half: a sender for a socket that is gone must not be left
	// behind for the next delivery to find.
	if reg.get(instID) != nil {
		t.Error("the sender outlived its connection — a later push would be " +
			"written to a dead socket")
	}
}
