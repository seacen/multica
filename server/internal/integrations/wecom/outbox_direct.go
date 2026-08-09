package wecom

// outbox_direct.go — the record a socket delivery owes the outbound queue.
//
// The queue's reconciler reads a terminal task with no queue row as a reply
// the realtime path dropped, and enqueues it. That reading is right for every
// message this adapter enqueues and wrong for the ones it writes straight to
// the socket, which are the two it cannot express as a row at all: the
// streaming bubble is addressed by the req_id of the callback that opened the
// turn, so only the connection that received that callback can write it, and
// the failure notice closes a bubble the same way. Nothing was dropped there,
// so the rescue is a second copy of words the user has already read.
//
// So every ending this adapter delivers over the socket ends with one of these
// calls, under the business key the enqueue it replaced would have used. The
// row is the same 'sent' tombstone a drained row leaves behind, which is why
// nothing downstream has to know the difference — see
// RecordChannelOutboundDelivered in channel_outbound_queue.sql.

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/outbox"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// socketDeliveryQueries is what resolving a session to a queue row's address
// takes. Both readers already hold it — outbound.go for the answer and the
// typing indicator for the failure notice — so *db.Queries satisfies this
// without a new query.
type socketDeliveryQueries interface {
	GetChannelChatSessionBindingBySession(ctx context.Context, arg db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error)
	GetChannelInstallation(ctx context.Context, arg db.GetChannelInstallationParams) (db.ChannelInstallation, error)
}

// recordSocketDelivery tells the outbound queue that this task's ending has
// already reached the user, so the reconciler leaves it alone.
//
// Best effort on purpose. The message is on screen either way, and failing the
// turn over a bookkeeping row would trade a duplicate for a lost reply. The
// failure is logged at WARN because it has a visible consequence: about a
// minute later the reconciler delivers the same words again.
//
// The installation's status is deliberately not checked. A revoke between the
// send and this call does not un-send the message, and the row's only job is
// to stop a second one.
func recordSocketDelivery(
	ctx context.Context,
	producer *outbox.Producer,
	log *slog.Logger,
	inst db.ChannelInstallation,
	binding db.ChannelChatSessionBinding,
	taskID pgtype.UUID,
	sourceKind string,
) {
	if producer == nil || !taskID.Valid || !inst.ID.Valid {
		return
	}
	if _, err := producer.RecordDelivered(ctx, outbox.Request{
		InstallationID: inst.ID,
		WorkspaceID:    inst.WorkspaceID,
		ChatSessionID:  binding.ChatSessionID,
		SourceKind:     sourceKind,
		SourceID:       util.UUIDToString(taskID),
		TargetChatID:   binding.ChannelChatID,
		TargetChatType: int16(aibotChatTypeFromChannel(channel.ChatType(binding.ChatType))),
		MsgType:        msgTypeMarkdown,
	}); err != nil {
		log.WarnContext(ctx, "wecom outbound: could not record a reply delivered over the socket; the reconciler will send it again",
			"installation_id", util.UUIDToString(inst.ID),
			"task_id", util.UUIDToString(taskID),
			"source_kind", sourceKind,
			"error", err)
	}
}

// resolveSocketDeliveryTarget loads the binding and installation a record
// needs from the session alone, for the callers that do not already hold them.
func resolveSocketDeliveryTarget(ctx context.Context, q socketDeliveryQueries, sessionID pgtype.UUID) (db.ChannelInstallation, db.ChannelChatSessionBinding, bool) {
	if q == nil {
		return db.ChannelInstallation{}, db.ChannelChatSessionBinding{}, false
	}
	binding, err := q.GetChannelChatSessionBindingBySession(ctx, db.GetChannelChatSessionBindingBySessionParams{
		ChatSessionID: sessionID,
		ChannelType:   channelTypeWecom,
	})
	if err != nil {
		return db.ChannelInstallation{}, db.ChannelChatSessionBinding{}, false
	}
	inst, err := q.GetChannelInstallation(ctx, db.GetChannelInstallationParams{
		ID:          binding.InstallationID,
		ChannelType: channelTypeWecom,
	})
	if err != nil {
		return db.ChannelInstallation{}, db.ChannelChatSessionBinding{}, false
	}
	return inst, binding, true
}
