package outbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Request is one row to enqueue. It exists so callers describe a delivery in
// domain terms instead of assembling generated query params, and so the
// business key stays visible at every call site.
type Request struct {
	InstallationID pgtype.UUID
	WorkspaceID    pgtype.UUID

	// ChatSessionID is optional. When set, the row is fenced on the session
	// still being bound to this installation and still active — both at claim
	// time and again immediately before send.
	ChatSessionID pgtype.UUID

	// SourceKind and SourceID form the business key that makes enqueue
	// idempotent. SourceID must identify the business result itself (a task
	// id, say), never the attempt: a per-attempt id would let the realtime
	// producer and the reconciler both insert and deliver the same reply.
	SourceKind string
	SourceID   string

	TargetChatID   string
	TargetChatType int16

	// MsgType and Payload are opaque to this package; the channel's Sender
	// renders them at send time.
	MsgType string
	Payload []byte
}

// ProducerStore is the generated-query surface the Producer needs.
type ProducerStore interface {
	EnqueueChannelOutbound(ctx context.Context, arg db.EnqueueChannelOutboundParams) (db.ChannelOutboundQueue, error)
	RecordChannelOutboundDelivered(ctx context.Context, arg db.RecordChannelOutboundDeliveredParams) (db.ChannelOutboundQueue, error)
}

// Producer enqueues rows and nudges a local consumer. Any replica may hold
// one; the queue is what decouples producing a reply from being the replica
// that can write it.
type Producer struct {
	channelType string
	q           ProducerStore
	wake        *WakeRegistry
	metrics     Metrics
}

// NewProducer builds a producer for one channel type. wake is optional.
func NewProducer(channelType string, q ProducerStore, wake *WakeRegistry, metrics Metrics) (*Producer, error) {
	if strings.TrimSpace(channelType) == "" {
		return nil, errors.New("outbox: channel type is required")
	}
	if q == nil {
		return nil, errors.New("outbox: queries are required")
	}
	if metrics == nil {
		metrics = NoopMetrics()
	}
	return &Producer{channelType: channelType, q: q, wake: wake, metrics: metrics}, nil
}

// Enqueue inserts one row and wakes the installation's consumer.
//
// It reports whether the row was freshly inserted. false means the business
// key already existed and this call delivered nothing new — the expected
// outcome when the realtime path and the reconciler race, and the reason
// neither needs to coordinate with the other.
//
// path is the Metrics attribution bucket (EnqueuePathRealtime or
// EnqueuePathReconcile).
func (p *Producer) Enqueue(ctx context.Context, req Request, path string) (bool, error) {
	if !req.InstallationID.Valid || !req.WorkspaceID.Valid {
		return false, errors.New("outbox: installation and workspace ids are required")
	}
	if strings.TrimSpace(req.SourceKind) == "" || strings.TrimSpace(req.SourceID) == "" {
		return false, errors.New("outbox: source kind and id are required")
	}
	if strings.TrimSpace(req.TargetChatID) == "" {
		return false, errors.New("outbox: target chat id is required")
	}

	_, err := p.q.EnqueueChannelOutbound(ctx, db.EnqueueChannelOutboundParams{
		InstallationID: req.InstallationID,
		WorkspaceID:    req.WorkspaceID,
		ChannelType:    p.channelType,
		ChatSessionID:  req.ChatSessionID,
		SourceKind:     req.SourceKind,
		SourceID:       req.SourceID,
		TargetChatID:   req.TargetChatID,
		TargetChatType: req.TargetChatType,
		MsgType:        req.MsgType,
		Payload:        req.Payload,
	})
	switch {
	case err == nil:
		p.metrics.RecordEnqueued(p.channelType, path, req.SourceKind)
	case errors.Is(err, pgx.ErrNoRows):
		// ON CONFLICT DO NOTHING returned no row: this business key is already
		// queued or delivered. Still wake — the row that won the race may be
		// sitting unclaimed on this replica.
		p.wakeInstallation(req.InstallationID)
		return false, nil
	default:
		return false, err
	}

	p.wakeInstallation(req.InstallationID)
	return true, nil
}

// RecordDelivered records a delivery that bypassed the queue, as a row that is
// already 'sent'. It reports whether the row was freshly inserted; false means
// the business key was already there, which is what a race with Enqueue looks
// like and needs no handling.
//
// A channel needs this the moment it grows a send that cannot be expressed as
// a row. [Reconciler] infers a lost reply from the absence of one, and that
// inference holds only while every outbound send is an [Producer.Enqueue] —
// so a platform with an in-window reply, addressed by the req_id of the
// callback that opened the turn, breaks it. Only the connection that received
// that callback can echo the frame, so a worker claiming a row a moment later
// has nothing to echo and the reply has to leave over the socket. Left
// unrecorded, the reconciler reads that delivery as a loss and sends it a
// second time.
//
// So the rule a channel adopting this package has to keep is: a delivery path
// that does not go through the queue records the delivery here. The business
// key must be the one the Enqueue it stands in for would have used — a
// different SourceKind or SourceID records nothing the reconciler looks for.
//
// req.Payload is ignored. The row is the record that something went out, not a
// copy of it: the body was rendered and written by the path that delivered it,
// and the reconciler only ever reads the key.
//
// No wake: there is nothing for a consumer to claim.
func (p *Producer) RecordDelivered(ctx context.Context, req Request) (bool, error) {
	if !req.InstallationID.Valid || !req.WorkspaceID.Valid {
		return false, errors.New("outbox: installation and workspace ids are required")
	}
	if strings.TrimSpace(req.SourceKind) == "" || strings.TrimSpace(req.SourceID) == "" {
		return false, errors.New("outbox: source kind and id are required")
	}
	if strings.TrimSpace(req.TargetChatID) == "" {
		return false, errors.New("outbox: target chat id is required")
	}

	_, err := p.q.RecordChannelOutboundDelivered(ctx, db.RecordChannelOutboundDeliveredParams{
		InstallationID: req.InstallationID,
		WorkspaceID:    req.WorkspaceID,
		ChannelType:    p.channelType,
		ChatSessionID:  req.ChatSessionID,
		SourceKind:     req.SourceKind,
		SourceID:       req.SourceID,
		TargetChatID:   req.TargetChatID,
		TargetChatType: req.TargetChatType,
		MsgType:        req.MsgType,
	})
	switch {
	case err == nil:
		p.metrics.RecordEnqueued(p.channelType, EnqueuePathDirect, req.SourceKind)
		return true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// ON CONFLICT DO NOTHING returned no row: this business key is already
		// queued, delivered or recorded. Nothing to do — the reconciler is
		// already suppressed either way.
		return false, nil
	default:
		return false, err
	}
}

func (p *Producer) wakeInstallation(id pgtype.UUID) {
	if p.wake == nil {
		return
	}
	p.wake.Wake(util.UUIDToString(id))
}

// ChannelType reports the channel this producer enqueues for.
func (p *Producer) ChannelType() string { return p.channelType }

// LogValue keeps a producer cheap to log without leaking its store.
func (p *Producer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("channel_type", p.channelType))
}
