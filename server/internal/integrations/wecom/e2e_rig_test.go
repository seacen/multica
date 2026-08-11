package wecom

// e2e_rig_test.go — the socket double the end-to-end tests drive a real
// wecomChannel through, plus the two helpers that build one.
//
// It lived beside the enter_chat greeting until that was withdrawn upstream;
// the greeting was the first thing that needed a socket which ANSWERS, and
// these tests are what is left that still does.

import (
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// connectedChannel is a wecomChannel wired to a scripted socket and nothing
// else: enough to run Connect end to end, with no database behind it.
func connectedChannel(t *testing.T, conn wsConn, handler channel.InboundHandler) *wecomChannel {
	t.Helper()
	return &wecomChannel{
		installationID: mustTestUUID(t),
		botID:          "bot-1",
		secret:         "secret-1",
		handler:        handler,
		dialer:         scriptedDialer{conn: conn},
		wsURL:          "wss://example.test/ws",
		logger:         slog.Default(),
	}
}

// serverFrame renders one frame the way the server would push it.
func serverFrame(t *testing.T, cmd, reqID string, body json.RawMessage) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(frameEnvelope{Cmd: cmd, Headers: frameHeaders{ReqID: reqID}, Body: body})
	if err != nil {
		t.Fatalf("marshal server frame: %v", err)
	}
	return b
}

// scriptedAckConn is a socket double for the whole read loop. It acks the
// subscribe handshake, then delivers a scripted sequence of server frames, and
// answers every frame we write the way WeCom does — with a separate ack frame
// carrying the same req_id, delivered back over the socket. That last part is
// what makes this a real exercise of the path: the greeting waits for a server
// verdict, and the read loop is the only thing that delivers one.
type scriptedAckConn struct {
	mu     sync.Mutex
	frames []frameEnvelope

	script []json.RawMessage
	sent   bool

	reads  chan []byte
	closed chan struct{}
	once   sync.Once
}

func newScriptedAckConn(script ...json.RawMessage) *scriptedAckConn {
	return &scriptedAckConn{
		script: script,
		reads:  make(chan []byte, 256),
		closed: make(chan struct{}),
	}
}

func (c *scriptedAckConn) WriteMessage(_ int, data []byte) error {
	var env frameEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	c.mu.Lock()
	c.frames = append(c.frames, env)
	c.mu.Unlock()
	if env.Headers.ReqID == "" {
		return nil
	}
	ack, err := json.Marshal(frameEnvelope{Headers: frameHeaders{ReqID: env.Headers.ReqID}})
	if err != nil {
		return err
	}
	select {
	case c.reads <- ack:
	case <-c.closed:
	}
	return nil
}

func (c *scriptedAckConn) ReadMessage() (int, []byte, error) {
	select {
	case b := <-c.reads:
		c.mu.Lock()
		first := !c.sent
		c.sent = true
		script := c.script
		c.mu.Unlock()
		if first {
			// That was the subscribe ack. The main read loop takes over from
			// here, so the scripted frames can go out now.
			go func() {
				for _, f := range script {
					select {
					case c.reads <- f:
					case <-c.closed:
						return
					}
				}
			}()
		}
		return websocket.TextMessage, b, nil
	case <-c.closed:
		return 0, nil, errors.New("wecom test: socket closed")
	}
}

func (c *scriptedAckConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedAckConn) SetWriteDeadline(time.Time) error { return nil }
func (c *scriptedAckConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// waitForCmd polls the recorded writes for a frame with the given cmd.
func (c *scriptedAckConn) waitForCmd(cmd string, d time.Duration) (frameEnvelope, bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, f := range c.frames {
			if f.Cmd == cmd {
				c.mu.Unlock()
				return f, true
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	return frameEnvelope{}, false
}
