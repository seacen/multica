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
	return r.sendCtx(context.Background(), id, msg)
}

// sendCtx is send for a caller with a budget. The write is bounded by ctx
// instead of only by the socket's own ten-second deadline; a write cut short
// that way is held for the reconnect exactly like any other write that did not
// land, so the caller is released without the message being lost.
func (r *sendersRegistry) sendCtx(ctx context.Context, id pgtype.UUID, msg pendingSend) error {
	if _, err := sendMsgTextBody(msg.ChatID, msg.ChatType, msg.Content); err != nil {
		return err
	}
	// A backlog outranks a live connection. Connect registers the sender and
	// drains on a separate goroutine, so for a moment both are true — and a
	// message written straight out in that moment lands ahead of the answers
	// that have been waiting for the socket, which reads as the conversation
	// running backwards. Joining the queue keeps one order for everything.
	sender := r.get(id)
	if sender == nil || r.pending.depth(id) > 0 {
		r.pending.enqueue(id, msg)
		r.log.Debug("wecom outbound: message queued behind the backlog",
			"installation_id", util.UUIDToString(id),
			"connected", sender != nil, "depth", r.pending.depth(id))
		return nil
	}
	if faultFires(FaultDropNextSend) {
		// Accepted and then dropped, which is what an overflowing holding
		// queue or a write the platform discarded looks like from here: the
		// caller is told nothing went wrong.
		logFault(r.log, FaultDropNextSend, "sendersRegistry.sendCtx")
		return nil
	}
	if err := sender.sendTextCtx(ctx, msg.ChatID, msg.ChatType, msg.Content); err != nil {
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

// uploadMedia carries one file up the installation's live connection and
// returns the media_id a message can be built around.
//
// Like stream and unlike send it never queues. An upload is a conversation with
// one socket — the upload_id it hands back means nothing on the next one — so a
// file that cannot go up now has to be attempted again from the start, and the
// caller says so in words rather than holding megabytes for a reconnect that
// may be hours away.
func (r *sendersRegistry) uploadMedia(ctx context.Context, id pgtype.UUID, m outboundMedia) (string, error) {
	sender := r.get(id)
	if sender == nil {
		return "", errNoLiveConnection
	}
	return sender.uploadMedia(ctx, m)
}

// sendMedia delivers an uploaded file as a message. reqID is the turn's
// callback id when there is still one; it is only used if WeCom refuses the
// push (media_upload.go).
func (r *sendersRegistry) sendMedia(ctx context.Context, id pgtype.UUID, chatID string, chatType int, reqID string, m mediaSend) error {
	sender := r.get(id)
	if sender == nil {
		return errNoLiveConnection
	}
	return sender.sendMedia(ctx, chatID, chatType, reqID, m)
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
		// Held from the pop until the write is done, so a reply produced
		// meanwhile still sees somebody ahead of it and joins the queue
		// instead of overtaking.
		r.pending.beginDrain(id)
		err := sender.sendText(msg.ChatID, msg.ChatType, msg.Content)
		if err != nil {
			r.pending.pushFront(id, msg)
		}
		r.pending.endDrain(id)
		if err != nil {
			r.log.Warn("wecom outbound: resend failed, keeping the rest queued",
				"installation_id", util.UUIDToString(id), "error", err,
				"depth", r.pending.depth(id))
			return
		}
	}
}
