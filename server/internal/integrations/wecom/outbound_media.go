package wecom

// outbound_media.go — delivering the files an agent produced.
//
// The agent's side of this already exists and is platform-agnostic: it runs
// `multica attachment upload <path>`, the file lands in object storage, and
// CompleteTask binds the row to the assistant message it just wrote. What was
// missing was the last hop. Everything downstream of that bind assumed a chat
// window in a browser, so a WeCom conversation was told it could not take
// files at all.
//
// Three things decide the shape here.
//
// The answer goes first, always. An upload is megabytes and round trips and it
// can fail; the sentence the agent wrote cannot be made to wait behind one, and
// must not be lost to one. So this runs after the reply is out, on its own
// goroutine and its own budget, and its worst outcome is one extra line saying
// a file did not make it.
//
// The file is its own message. The long connection has no msg_item, so nothing
// can be embedded in a streaming bubble — "answer with an attachment" is
// necessarily two messages, and the bubble carrying the words is sealed before
// the first byte goes up.
//
// And WeCom validates bytes against the msgtype. A .pptx declared as an image
// is refused rather than converted, and each kind has its own size ceiling, so
// what to call a file is a decision and not a lookup.

import (
	"context"
	"fmt"
	"io"
	"mime"
	"path"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// mediaObjectStore is the slice of storage.Storage this path needs: the
// attachment row carries the object's URL, and these two turn it back into
// bytes.
type mediaObjectStore interface {
	KeyFromURL(rawURL string) string
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
}

// attachmentBudget bounds one answer's whole attachment delivery — reading
// every object, uploading it, and sending it. Generous because a 50MB file over
// a hundred acked chunks is not fast, and nothing is waiting on it.
const attachmentBudget = 5 * time.Minute

// The per-kind ceilings WeCom applies to uploaded material: 10MB for a photo,
// 10MB for a video, 2MB for a voice note. Bytes past a kind's ceiling still
// travel — as a file, which has the widest limit of the four — because a file
// card the user can open beats a photo the server refused.
const (
	maxOutboundImageBytes = 10 << 20
	maxOutboundVoiceBytes = 2 << 20
	maxOutboundVideoBytes = 10 << 20
)

// OutboundOption configures the chat-done subscriber at construction.
type OutboundOption func(*Outbound)

// WithAttachments turns on file delivery. Without it — a deployment with no
// object storage — an answer is delivered exactly as it was before, and the
// agent is told as much in its brief.
func WithAttachments(objects mediaObjectStore) OutboundOption {
	return func(o *Outbound) { o.objects = objects }
}

// deliverAttachments hands the answer's files to a goroutine of their own, if
// there are any to hand over. It is called after the words are out, from both
// delivery paths, and returns immediately.
//
// reqID is the turn's callback id when the answer went into a bubble, and empty
// otherwise. It is only ever used as the fallback address if WeCom refuses the
// push (media_upload.go).
func (o *Outbound) deliverAttachments(e events.Event, addr roundAddress, reqID string) {
	if o.objects == nil || o.senders == nil {
		return
	}
	messageID, err := util.ParseUUID(chatDoneMessageID(e.Payload))
	if err != nil || !messageID.Valid {
		return // a turn with no assistant message has nothing bound to it
	}
	workspaceID, err := util.ParseUUID(e.WorkspaceID)
	if err != nil || !workspaceID.Valid {
		return
	}
	if !addr.InstallationID.Valid || addr.ChatID == "" {
		return
	}
	o.spawn(func() {
		ctx, cancel := context.WithTimeout(context.Background(), attachmentBudget)
		defer cancel()
		o.sendAttachments(ctx, messageID, workspaceID, addr, reqID)
	})
}

// sendAttachments delivers every file bound to one answer. Files are
// independent: one that fails does not stop the rest, and whatever did not make
// it is said once at the end rather than once each.
func (o *Outbound) sendAttachments(ctx context.Context, messageID, workspaceID pgtype.UUID, addr roundAddress, reqID string) {
	rows, err := o.q.ListAttachmentsByChatMessage(ctx, db.ListAttachmentsByChatMessageParams{
		ChatMessageID: messageID,
		WorkspaceID:   workspaceID,
	})
	if err != nil {
		o.logger.WarnContext(ctx, "wecom outbound: attachment lookup failed",
			"error", err, "chat_message_id", uuidStringPub(messageID))
		return
	}
	if len(rows) == 0 {
		return
	}
	failed := 0
	for _, row := range rows {
		if err := o.sendAttachment(ctx, row, addr, reqID); err != nil {
			failed++
			// The object's URL stays out of the log: it is an address that
			// serves the file to whoever holds it.
			o.logger.WarnContext(ctx, "wecom outbound: attachment not delivered",
				"error", err,
				"installation_id", uuidStringPub(addr.InstallationID),
				"attachment_id", uuidStringPub(row.ID),
				"content_type", row.ContentType,
				"size_bytes", row.SizeBytes)
		}
	}
	if failed == 0 {
		return
	}
	// The answer is already on the user's screen and it may well refer to a
	// file. Saying nothing would leave them looking for one that never comes.
	if err := o.senders.send(addr.InstallationID, pendingSend{
		ChatID:   addr.ChatID,
		ChatType: addr.ChatType,
		Content:  copyFor(addr.Locale).MediaSendFailed,
	}); err != nil {
		o.logger.WarnContext(ctx, "wecom outbound: could not say the file failed",
			"error", err, "installation_id", uuidStringPub(addr.InstallationID))
	}
}

// sendAttachment carries one file from object storage into the chat.
func (o *Outbound) sendAttachment(ctx context.Context, row db.Attachment, addr roundAddress, reqID string) error {
	data, err := o.readObject(ctx, row.Url)
	if err != nil {
		return err
	}
	kind := wecomMediaKind(row.ContentType, row.Filename, len(data))
	name := outboundMediaName(row.Filename, row.ContentType)

	mediaID, err := o.senders.uploadMedia(ctx, addr.InstallationID, outboundMedia{
		Kind:     kind,
		Filename: name,
		Data:     data,
	})
	if err != nil {
		return fmt.Errorf("upload %s: %w", kind, err)
	}
	// Video is the only kind with fields beyond the media_id, and both are
	// required. The file's own name is what there is to say about it — the
	// attachment row carries no caption and the agent's words are already in
	// the message above.
	return o.senders.sendMedia(ctx, addr.InstallationID, addr.ChatID, addr.ChatType, reqID, mediaSend{
		Kind:        kind,
		MediaID:     mediaID,
		Title:       strings.TrimSuffix(name, path.Ext(name)),
		Description: name,
	})
}

// readObject pulls the whole file into memory. It has to be whole: the upload
// declares total_size and total_chunks before the first chunk goes out, so
// there is no streaming this one.
func (o *Outbound) readObject(ctx context.Context, rawURL string) ([]byte, error) {
	key := o.objects.KeyFromURL(rawURL)
	if key == "" {
		return nil, fmt.Errorf("wecom: attachment is not an object this deployment stores")
	}
	rc, err := o.objects.GetReader(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("read attachment: %w", err)
	}
	defer rc.Close()
	// One byte of headroom, so reading exactly the cap can be told from a file
	// that has more to come.
	data, err := io.ReadAll(io.LimitReader(rc, maxMediaUploadBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read attachment: %w", err)
	}
	if len(data) > maxMediaUploadBytes {
		return nil, errMediaUploadTooLarge
	}
	return data, nil
}

// wecomMediaKind decides what WeCom is told this file is.
//
// The content type leads, because it is what the uploader declared. When it
// says nothing useful — empty, or the octet-stream that means "bytes" — the
// filename's extension is the better guess. A kind whose ceiling the file
// exceeds is demoted to a file rather than sent and refused.
func wecomMediaKind(contentType, filename string, size int) mediaMsgType {
	ct := baseContentType(contentType)
	if ct == "" || ct == "application/octet-stream" {
		ct = baseContentType(mime.TypeByExtension(path.Ext(filename)))
	}
	switch {
	case strings.HasPrefix(ct, "image/") && size <= maxOutboundImageBytes:
		return mediaTypeImage
	case strings.HasPrefix(ct, "video/") && size <= maxOutboundVideoBytes:
		return mediaTypeVideo
	// Voice is AMR only. An mp3 sent as a voice note is refused, and as a file
	// it is at least playable after a tap.
	case ct == "audio/amr" && size <= maxOutboundVoiceBytes:
		return mediaTypeVoice
	default:
		return mediaTypeFile
	}
}

// baseContentType drops the parameters a content type may carry, so
// "text/csv; charset=utf-8" compares as "text/csv".
func baseContentType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if semi := strings.IndexByte(s, ';'); semi >= 0 {
		s = strings.TrimSpace(s[:semi])
	}
	return s
}

// outboundMediaName is what the recipient sees on the file card. It is reduced
// to a single path segment — the name reaches the wire and a stored filename is
// not guaranteed to be one — and given an extension when it has none, since
// that is the only hint WeCom gets about the format.
func outboundMediaName(filename, contentType string) string {
	name := cleanMediaFilename(filename)
	if name == "" {
		name = "attachment"
	}
	if path.Ext(name) == "" {
		if ext := mediaExtension(baseContentType(contentType)); ext != "" {
			name += ext
		}
	}
	return name
}

// chatDoneMessageID pulls the assistant message id out of a chat:done payload
// (the typed payload, or its map form after a serialization round trip). It is
// the key every attachment on this turn is bound to.
func chatDoneMessageID(payload any) string {
	switch p := payload.(type) {
	case protocol.ChatDonePayload:
		return p.MessageID
	case map[string]any:
		if s, ok := p["message_id"].(string); ok {
			return s
		}
	}
	return ""
}
