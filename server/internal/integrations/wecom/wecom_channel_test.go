package wecom

// wecom_channel_test.go — regression coverage for Connect's teardown
// ordering. The read loop dropping while the parent context is still live is
// the ordinary transient failure the Supervisor exists to retry, so Connect
// must hand that error back promptly rather than block on its own goroutines.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// errConnDropped stands in for the socket dying underneath the read loop.
var errConnDropped = errors.New("connection reset by peer")

// scriptedConn is a wsConn that completes the aibot_subscribe handshake and
// then fails every subsequent read, without ever touching the network.
type scriptedConn struct {
	acked  bool
	ackBuf []byte
}

func (c *scriptedConn) WriteMessage(_ int, data []byte) error {
	// Echo the subscribe frame's req_id back so subscribe() matches its ack.
	var env frameEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	if env.Cmd == cmdSubscribe {
		ack, err := json.Marshal(map[string]any{
			"headers": frameHeaders{ReqID: env.Headers.ReqID},
			"errcode": 0,
		})
		if err != nil {
			return err
		}
		c.ackBuf = ack
	}
	return nil
}

func (c *scriptedConn) ReadMessage() (int, []byte, error) {
	if !c.acked && c.ackBuf != nil {
		c.acked = true
		return 1, c.ackBuf, nil // websocket.TextMessage
	}
	return 0, nil, errConnDropped
}

func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }
func (c *scriptedConn) Close() error                     { return nil }

type scriptedDialer struct{ conn *scriptedConn }

func (d scriptedDialer) DialContext(context.Context, string, http.Header) (wsConn, *http.Response, error) {
	return d.conn, nil, nil
}

// TestConnectReturnsWhenReadFailsWithLiveContext pins the teardown ordering.
// Before the fix, Connect registered `defer pingCancel()` ahead of
// `defer func(){ <-pingDone }()`; LIFO ran the wait first, and pingLoop only
// returns on pingCtx.Done, so with the parent ctx still live nothing ever
// cancelled it and Connect blocked forever instead of returning the read
// error to the Supervisor. This test deadlocks on the old code.
func TestConnectReturnsWhenReadFailsWithLiveContext(t *testing.T) {
	c := &wecomChannel{
		botID:   "bot",
		secret:  "secret",
		handler: func(context.Context, channel.InboundMessage) error { return nil },
		dialer:  scriptedDialer{conn: &scriptedConn{}},
		wsURL:   "wss://example.invalid/ws",
	}

	// Parent context stays live for the whole test — that is the point.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()

	select {
	case err := <-done:
		if !errors.Is(err, errConnDropped) {
			t.Fatalf("want the read error wrapped, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after the read loop failed: " +
			"the ping goroutine was never cancelled, so teardown deadlocked")
	}
}
