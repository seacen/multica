package wecom

// ws_sender.go — a serialized writer for one WebSocket connection. gorilla
// forbids concurrent writes so every outbound frame goes through the same
// mutex; the ping loop, subscribe handshake, and Send() calls all share
// this writer.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn is the subset of gorilla's Conn the wecom package uses. Kept
// minimal so tests can inject a fake without embedding all of gorilla's
// surface.
type wsConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Close() error
}

// Dialer opens a WebSocket connection to the aibot endpoint. Production
// uses gorilla's default dialer; tests wire a fake pointing at an
// httptest.Server.
type Dialer interface {
	DialContext(ctx context.Context, url string, header http.Header) (wsConn, *http.Response, error)
}

// defaultDialer is the production Dialer. Proxy is set explicitly because a
// zero-valued websocket.Dialer has a nil Proxy and ignores the environment,
// unlike websocket.DefaultDialer — and self-hosted deployments behind a
// corporate egress proxy reach qyapi.weixin.qq.com only through HTTPS_PROXY.
var defaultDialer Dialer = gorillaDialer{d: &websocket.Dialer{
	HandshakeTimeout: handshakeTimeout,
	Proxy:            http.ProxyFromEnvironment,
}}

type gorillaDialer struct {
	d *websocket.Dialer
}

func (g gorillaDialer) DialContext(ctx context.Context, u string, header http.Header) (wsConn, *http.Response, error) {
	conn, resp, err := g.d.DialContext(ctx, u, header)
	if err != nil {
		return nil, resp, err
	}
	return &gorillaWSConn{Conn: conn}, resp, nil
}

// gorillaWSConn wraps *websocket.Conn so it satisfies wsConn without leaking
// the concrete type into wsConn's method signatures.
type gorillaWSConn struct {
	*websocket.Conn
}

// wsSender serializes writes to one WebSocket connection. Instantiated per
// Connect() call and dropped when the connection ends.
type wsSender struct {
	conn wsConn
	mu   sync.Mutex
	log  *slog.Logger

	// waiters holds the frames whose verdict somebody is standing by for,
	// keyed by req_id. Most writes are fire-and-forget — a ping, a push — but
	// a stream frame's errcode decides whether the answer goes in the bubble
	// or in a new message, so that one write has to hear back. The read loop
	// routes acks in through deliverAck.
	ackMu   sync.Mutex
	waiters map[string]chan ackResult
}

func newWSSender(conn wsConn, log *slog.Logger) *wsSender {
	if log == nil {
		log = slog.Default()
	}
	return &wsSender{conn: conn, log: log, waiters: make(map[string]chan ackResult)}
}

// ackTimeout caps the wait for a verdict. WeCom answers in a few hundred
// milliseconds; past this we assume the ack was lost rather than the frame
// refused, which matters because the two call for opposite responses.
const ackTimeout = 5 * time.Second

// ackSuperseded is the pseudo-code handed to a waiter whose frame was
// displaced by a closing frame on the same stream.
const ackSuperseded = -1

var (
	// errStreamBusy — a mid-stream refresh was skipped because the previous
	// frame on this req_id has not been acked. This is the backpressure the
	// official SDK calls replyStreamNonBlocking: progress updates yield,
	// closing frames never do.
	errStreamBusy = errors.New("wecom: previous stream frame still unacked")

	// errStreamAckTimeout — the frame went out and no verdict came back. The
	// frame may well have landed, so callers weigh a possible duplicate
	// against a possible silence rather than assuming failure.
	errStreamAckTimeout = errors.New("wecom: stream frame ack timed out")

	// errStreamSuperseded — a refresh was overtaken by the closing frame.
	errStreamSuperseded = errors.New("wecom: stream frame superseded by the closing frame")

	// errNoLiveConnection — the installation has no socket right now.
	errNoLiveConnection = errors.New("wecom: no live connection for installation")
)

// ackResult is one server verdict.
type ackResult struct {
	code int
	msg  string
}

// deliverAck hands a server ack to whoever is waiting on that req_id. The read
// loop calls it for every anonymous ack frame; acks nobody is waiting on (the
// heartbeat, an ordinary push) fall straight through.
func (s *wsSender) deliverAck(reqID string, code int, msg string) {
	if reqID == "" {
		return
	}
	s.ackMu.Lock()
	ch, ok := s.waiters[reqID]
	if ok {
		delete(s.waiters, reqID)
	}
	s.ackMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- ackResult{code: code, msg: msg}:
	default:
	}
}

// awaitAck registers interest in the next ack for reqID. force is what makes a
// closing frame able to jump an in-flight refresh: without it the answer would
// be held hostage by a progress update whose ack is late.
func (s *wsSender) awaitAck(reqID string, force bool) (chan ackResult, bool) {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if prev, taken := s.waiters[reqID]; taken {
		if !force {
			return nil, false
		}
		select {
		case prev <- ackResult{code: ackSuperseded}:
		default:
		}
	}
	ch := make(chan ackResult, 1)
	s.waiters[reqID] = ch
	return ch, true
}

// cancelAck retires a waiter that will never be answered.
func (s *wsSender) cancelAck(reqID string, ch chan ackResult) {
	s.ackMu.Lock()
	defer s.ackMu.Unlock()
	if cur, ok := s.waiters[reqID]; ok && cur == ch {
		delete(s.waiters, reqID)
	}
}

// respondStream writes one frame of a streaming reply and waits for the
// server's verdict.
//
// reqID is not ours to choose: every frame of one stream must echo the req_id
// of the aibot_msg_callback that opened the turn, or the server refuses it
// (846605). streamID is ours — reuse it to replace the bubble's body, and set
// finish once the content is final.
func (s *wsSender) respondStream(ctx context.Context, reqID, streamID, content string, finish bool) error {
	if reqID == "" {
		return errors.New("wecom: stream frame requires the callback req_id")
	}
	body, err := respondStreamBody(streamID, content, finish)
	if err != nil {
		return err
	}

	ch, ok := s.awaitAck(reqID, finish)
	if !ok {
		return errStreamBusy
	}
	if err := s.write(map[string]any{
		"cmd":     cmdRespondMsg,
		"headers": frameHeaders{ReqID: reqID},
		"body":    body,
	}); err != nil {
		s.cancelAck(reqID, ch)
		return err
	}

	timer := time.NewTimer(ackTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		switch {
		case res.code == ackSuperseded:
			return errStreamSuperseded
		case res.code != 0:
			return &streamError{Code: res.code, Msg: res.msg}
		}
		return nil
	case <-timer.C:
		s.cancelAck(reqID, ch)
		return errStreamAckTimeout
	case <-ctx.Done():
		s.cancelAck(reqID, ch)
		return errStreamAckTimeout
	}
}

// write marshals frame to JSON and pushes it under the writer mutex. The
// caller must not hold sendMu on wecomChannel — nothing here reaches back
// into the Channel.
func (s *wsSender) write(frame map[string]any) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("wecom: marshal frame: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.TextMessage, payload)
}

// sendText pushes an aibot_send_msg (proactive push) with plain text to a
// specific chat. Callers pass channel.ChatType so the aibot chat_type int
// (1=single, 2=group) is decided at the wecom-side boundary, not the
// engine's. Used by wecomChannel.Send and OutboundReplier.
func (s *wsSender) sendText(chatID string, chatTypeInt int, content string) error {
	body, err := sendMsgTextBody(chatID, chatTypeInt, content)
	if err != nil {
		return err
	}
	return s.write(map[string]any{
		"cmd":     cmdSendMsg,
		"headers": frameHeaders{ReqID: newReqID()},
		"body":    body,
	})
}
