package wecom

// senders_registry.go — a small process-wide map from installation_id to
// live wsSender. wecomChannel.Connect adds an entry on entry and clears it
// on exit; OutboundReplier and wecomChannel.Send look up by installation
// id to push aibot_send_msg over the same socket the inbound loop owns
// (aibot has no REST outbound path; every write goes over the WebSocket).
//
// Why a registry rather than storing the sender on wecomChannel:
// OutboundReplier is created once at boot with the shared engine.Router
// and does not have per-installation Channel handles. When the engine
// invokes Replier.Reply, it passes engine.ResolvedInstallation carrying
// the installation id, not the Channel. The registry is the seam that
// lets the boot-time Replier reach the per-installation live connection
// without threading the Channel through the engine.

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

// sendersRegistry is a goroutine-safe installation_id → wsSender map, plus
// the holding queue for messages that arrived while the map had no entry (see
// outbound_queue.go — the aibot socket is the only transport, so an
// undeliverable message either waits or is lost).
type sendersRegistry struct {
	mu    sync.RWMutex
	byKey map[string]*wsSender

	pending *outboundQueue
	log     *slog.Logger
}

// newSendersRegistry constructs an empty registry.
func newSendersRegistry() *sendersRegistry {
	log := slog.Default()
	return &sendersRegistry{
		byKey:   make(map[string]*wsSender),
		pending: newOutboundQueue(log),
		log:     log,
	}
}

// NewSendersRegistry is the public constructor boot uses to inject the
// same registry into both the wecom ChannelDeps (writer side) and the
// OutboundReplier (reader side). Kept exported so router.go can wire it
// without importing an unexported type.
func NewSendersRegistry() *sendersRegistry { return newSendersRegistry() }

func (r *sendersRegistry) set(id pgtype.UUID, s *wsSender) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byKey[util.UUIDToString(id)] = s
}

func (r *sendersRegistry) clear(id pgtype.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byKey, util.UUIDToString(id))
}

// get returns the live wsSender for an installation, or nil when no
// connection is currently held. Callers MUST treat nil as "connection not
// ready" — Supervisor may be mid-reconnect after a lease flip.
func (r *sendersRegistry) get(id pgtype.UUID) *wsSender {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byKey[util.UUIDToString(id)]
}

// send is the one outbound entry point for messages the engine produces
// (agent replies, inbox cards). It writes to the live connection when there
// is one and holds the message for the next one when there is not, so a
// reconnect window costs latency rather than the message.
//
// It returns an error only for a message the wire will never accept — an
// empty chat id, a bad chat type — so a malformed body cannot sit in the
// queue being retried forever. A transport failure is not an error here: the
// message is re-queued and the caller has nothing useful to do about it.
func (r *sendersRegistry) send(id pgtype.UUID, msg pendingSend) error {
	if _, err := sendMsgTextBody(msg.ChatID, msg.ChatType, msg.Content); err != nil {
		return err
	}
	sender := r.get(id)
	if sender == nil {
		r.pending.enqueue(id, msg)
		r.log.Debug("wecom outbound: no live connection, message held for reconnect",
			"installation_id", util.UUIDToString(id), "depth", r.pending.depth(id))
		return nil
	}
	if err := sender.sendText(msg.ChatID, msg.ChatType, msg.Content); err != nil {
		r.pending.enqueue(id, msg)
		r.log.Warn("wecom outbound: send failed, message held for reconnect",
			"installation_id", util.UUIDToString(id), "error", err)
		return nil
	}
	return nil
}

// stream writes one frame of a streaming reply to the bubble h describes.
//
// Unlike send it never queues. A stream frame is only meaningful while the
// req_id it echoes is still fresh, so a frame that cannot go out now is
// worthless on the next connection — replaying it after a reconnect would
// spend a write on a req_id the server has already forgotten. Callers that
// need the words delivered regardless fall back to send(), which does queue.
func (r *sendersRegistry) stream(ctx context.Context, h streamHandle, content string, finish bool) error {
	sender := r.get(h.InstallationID)
	if sender == nil {
		return errNoLiveConnection
	}
	return sender.respondStream(ctx, h.ReqID, h.StreamID, content, finish)
}

// flushPending drains an installation's holding queue over the live
// connection, oldest first. Connect calls it once the subscribe ack lands.
// A write that fails puts its message back at the head and stops the drain —
// the socket is going down again, and the next Connect will pick up where
// this one left off.
func (r *sendersRegistry) flushPending(id pgtype.UUID) {
	for {
		sender := r.get(id)
		if sender == nil {
			return
		}
		msg, ok := r.pending.pop(id)
		if !ok {
			return
		}
		if err := sender.sendText(msg.ChatID, msg.ChatType, msg.Content); err != nil {
			r.pending.pushFront(id, msg)
			r.log.Warn("wecom outbound: resend failed, keeping the rest queued",
				"installation_id", util.UUIDToString(id), "error", err,
				"depth", r.pending.depth(id))
			return
		}
	}
}
