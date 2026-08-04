package wecom

// regression_backlog_order_mid_drain_test.go — guards the order two answers
// reach a person's phone across a reconnect.
//
// The holding queue exists so that an answer produced while the socket was
// down still arrives, and arrives in the order it was written. Connect
// registers the sender and drains on its own goroutine, so there is a stretch
// where a connection is live and a backlog is still going out — and the drain
// takes its message OUT of the queue before it writes it. A reply for the next
// turn that arrives inside that stretch sees a live socket and an empty queue,
// so nothing holds it back and it can reach the wire ahead of the answer that
// had been waiting. The user then reads the answer to their second question
// above the answer to their first: the conversation runs backwards, which is
// exactly what the queue was built to prevent.
//
// outbound_queue_test.go's TestANewReplyDoesNotOvertakeTheBacklog covers the
// easy half — a reply arriving before the drain starts. This file covers the
// half that needs two goroutines: a reply arriving while the drain is in
// flight and its message is in neither the queue nor the socket.
//
// The stall point is the trace call every outbound frame passes through on its
// way to the writer. It is a stand-in for anything that can delay a drained
// message on that stretch in production — the marshal, a preemption, a GC
// pause — not a claim about tracing.

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// stallingTraceHandler parks the goroutine writing one particular message
// after it has left the queue and before it has taken the writer, and says so
// on reached. Anything else it is handed goes straight through, so the second
// reply is never slowed down by it.
type stallingTraceHandler struct {
	stall   string
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

func (h *stallingTraceHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *stallingTraceHandler) Handle(_ context.Context, r slog.Record) error {
	hit := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "text" && a.Value.String() == h.stall {
			hit = true
			return false
		}
		return true
	})
	if !hit {
		return nil
	}
	h.once.Do(func() { close(h.reached) })
	<-h.release
	return nil
}

func (h *stallingTraceHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *stallingTraceHandler) WithGroup(string) slog.Handler      { return h }

// TestAFreshReplyDoesNotOvertakeAnAnswerMidDrain — the person asked two
// questions across a short reconnect. Whatever the sockets did in between,
// they must read the first answer first.
func TestAFreshReplyDoesNotOvertakeAnAnswerMidDrain(t *testing.T) {
	const held = "答案是 42"
	const fresh = "还有一件事"

	inst := uuidOf(31)
	q := &fakeOutboundQueries{
		binding: db.ChannelChatSessionBinding{
			InstallationID: inst,
			ChannelChatID:  "T-alex",
			ChatType:       "p2p",
		},
		install: db.ChannelInstallation{ID: inst, Status: string(InstallationActive)},
	}
	reg := NewSendersRegistry()
	reg.log = testLogger()
	o := NewOutbound(q, reg, nil, testLogger())

	// The first turn finishes while the socket is down, so its answer waits.
	if err := o.processEvent(context.Background(), events.Event{
		ChatSessionID: uuidText(uuidOf(32)),
		Payload:       protocol.ChatDonePayload{Content: held},
	}); err != nil {
		t.Fatalf("a reply produced while the socket is down must not error: %v", err)
	}

	// The socket is back and Connect starts the drain on its own goroutine.
	gate := &stallingTraceHandler{
		stall:   held,
		reached: make(chan struct{}),
		release: make(chan struct{}),
	}
	conn := &recordingConn{}
	reg.set(inst, newWSSender(conn, slog.New(gate)))
	SetTrace(true)
	t.Cleanup(func() { SetTrace(false) })

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		reg.flushPending(inst)
	}()

	select {
	case <-gate.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain never reached the socket with the held answer")
	}

	// The second turn finishes right now — the ordinary state of things a few
	// seconds after a reconnect, with one answer still on its way out.
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- o.processEvent(context.Background(), events.Event{
			ChatSessionID: uuidText(uuidOf(33)),
			Payload:       protocol.ChatDonePayload{Content: fresh},
		})
	}()

	// Long enough for the new answer to reach the socket if nothing stops it.
	// A delivery that holds it back instead is the behaviour we want, and it
	// gets its turn as soon as the drain is let go below.
	var deliveredEarly bool
	select {
	case err := <-sendErr:
		deliveredEarly = true
		if err != nil {
			t.Fatalf("the second reply must not error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
	}

	close(gate.release)
	<-drained
	if !deliveredEarly {
		select {
		case err := <-sendErr:
			if err != nil {
				t.Fatalf("the second reply must not error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the second reply never completed")
		}
	}

	got := contentsOf(conn)
	want := []string{held, fresh}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("the user read %v, want %v — the answer that was already waiting for the socket must land before the one produced after it, or the conversation reads backwards", got, want)
	}
}
