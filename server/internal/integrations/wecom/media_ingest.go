package wecom

// media_ingest.go — the engine.MediaResolver for WeChat Work.
//
// The shape is the one lark/media_ingest.go established and the Router
// depends on: HasMedia is a pure in-memory look at the payload we already
// have, ResolveMedia runs detached from the connector ACK path, every upload
// is covered by an intent-ledger row written BEFORE the PUT, and nothing is
// ever deleted inline — a failure anywhere leaves the row for the reconciler
// and leaves the message's placeholder text intact.
//
// What is wecom-specific is the middle: WeCom hands over a pre-signed COS url
// and a per-url key instead of a resource id to be fetched with a tenant
// token, so there is no API client and no credential here — just an HTTP GET
// and a decrypt (media_download.go, media_crypt.go). The callback body says
// nothing about the file besides those two strings, so the name comes out of
// the download's Content-Disposition and the type is worked out from the name
// or sniffed from the decrypted bytes.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
)

// mediaStorage is the slice of storage.Storage this resolver drives.
// ObjectURL is a pure function of configuration, which is what lets the
// intent ledger persist the object's URL before the object exists.
type mediaStorage interface {
	Upload(ctx context.Context, key string, data []byte, contentType string, filename string) (string, error)
	ObjectURL(key string) string
}

// mediaNotifier is the outbound seam for telling the sender an attachment did
// not make it. *sendersRegistry is the production value; nil disables the
// notice and leaves only the log.
type mediaNotifier interface {
	send(id pgtype.UUID, msg pendingSend) error
}

type wecomMediaResolver struct {
	storage mediaStorage
	ledger  engine.MediaIntentLedger
	http    *http.Client
	notify  mediaNotifier
	logger  *slog.Logger
}

// NewMediaResolver builds the wecom MediaResolver. storage and ledger are
// required — without either there is nothing durable to point an attachment
// at, and the resolver degrades to leaving the placeholder in place. senders
// is optional and is taken as the concrete type so a nil argument leaves the
// field nil rather than a typed-nil interface.
func NewMediaResolver(storage mediaStorage, ledger engine.MediaIntentLedger, senders *sendersRegistry, logger *slog.Logger) engine.MediaResolver {
	if logger == nil {
		logger = slog.Default()
	}
	r := &wecomMediaResolver{
		storage: storage,
		ledger:  ledger,
		http:    &http.Client{},
		logger:  logger,
	}
	if senders != nil {
		r.notify = senders
	}
	return r
}

// HasMedia reports whether this callback carried anything to download. It
// runs synchronously on the connector ACK path and decides whether the
// message pays for a media deadline, a deferred run and a semaphore slot at
// all, so it stays a decode of bytes already in hand.
func (r *wecomMediaResolver) HasMedia(msg channel.InboundMessage) bool {
	wm, err := wecomMsgFromRaw(msg)
	if err != nil {
		return false
	}
	return len(wm.Media) > 0
}

// ResolveMedia downloads, decrypts and stores every attachment on the
// message, returning it with a MediaRef per object that landed. Attachments
// are independent: one that fails does not stop the rest, and the sender is
// told once at the end about whatever did not arrive.
func (r *wecomMediaResolver) ResolveMedia(ctx context.Context, inst engine.ResolvedInstallation, _ engine.ResolvedIdentity, _ pgtype.UUID, chatMessageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
	wm, err := wecomMsgFromRaw(msg)
	if err != nil {
		r.logger.Warn("wecom media ingest skipped: raw decode failed", "message_id", msg.MessageID, "err", err)
		return msg
	}
	if len(wm.Media) == 0 {
		return msg
	}
	if r.storage == nil || r.ledger == nil {
		r.logger.Warn("wecom media ingest skipped: no storage configured",
			"msg_id", wm.MsgID, "attachments", len(wm.Media))
		return msg
	}

	var failures []mediaFailure
	for i, m := range wm.Media {
		ref, err := r.ingestOne(ctx, inst, chatMessageID, wm, i, m)
		if err != nil {
			failures = appendFailure(failures, classifyMediaFailure(err))
			// The url and the key never reach the log: one is a signed
			// address anyone could then fetch, the other unlocks it.
			r.logger.Warn("wecom media ingest failed",
				"installation_id", util.UUIDToString(inst.ID),
				"msg_id", wm.MsgID,
				"attachment", i,
				"kind", string(m.Kind),
				"err", err)
			continue
		}
		msg.MediaRefs = append(msg.MediaRefs, ref)
	}
	r.tellTheSender(inst, wm, failures)
	return msg
}

// ingestOne carries a single attachment from url to MediaRef. The ledger row
// goes first: from that point on every failure — download, decrypt, upload,
// a crash — leaves an intent the reconciler settles, and nothing here deletes
// anything.
func (r *wecomMediaResolver) ingestOne(ctx context.Context, inst engine.ResolvedInstallation, chatMessageID pgtype.UUID, wm InboundMessage, index int, m InboundMedia) (channel.MediaRef, error) {
	key := mediaObjectKey(inst, chatMessageID, wm.MsgID, index, m.Kind)
	link := r.storage.ObjectURL(key)

	ok, err := r.ledger.RecordPendingMediaObject(ctx, engine.RecordPendingMediaObjectParams{
		StorageKey:     key,
		WorkspaceID:    inst.WorkspaceID,
		ChatMessageID:  chatMessageID,
		StorageURL:     link,
		InstallationID: inst.ID,
	})
	if err != nil {
		// No durable intent, no upload — the fail-safe direction.
		return channel.MediaRef{}, fmt.Errorf("record media intent: %w", err)
	}
	if !ok {
		// The reconciler owns this key; never resurrect it.
		return channel.MediaRef{}, fmt.Errorf("media key %s is owned by the reconciler", key)
	}

	got, err := downloadMedia(ctx, r.http, m.URL)
	if err != nil {
		return channel.MediaRef{}, err
	}
	plain, err := decryptMedia(m.AESKey, got.Body)
	if err != nil {
		return channel.MediaRef{}, err
	}

	filename, contentType := describeMedia(wm, index, m, got.Filename, plain)
	if _, err := r.storage.Upload(ctx, key, plain, contentType, filename); err != nil {
		// The store may still be processing the PUT; deleting here could
		// reorder with it. The intent row covers the object either way.
		return channel.MediaRef{}, fmt.Errorf("upload media: %w", err)
	}
	return channel.MediaRef{
		Type:       m.Kind,
		StorageKey: key,
		StorageURL: link,
		Filename:   filename,
		MimeType:   contentType,
		SizeBytes:  int64(len(plain)),
	}, nil
}

// mediaObjectKey names the object. It is derived from the CHAT message rather
// than the WeCom message for the reason lark documents: one platform message
// can be ingested twice (the inbound dedup claim is reclaimable once stale),
// and a shared key would run the second ingest into the first one's ledger
// row — possibly a tombstone the intent upsert refuses, silently dropping the
// media. The attachment's position in the message is part of the key too,
// since a 图文混排 can carry several and the url is not stable enough to key on.
func mediaObjectKey(inst engine.ResolvedInstallation, chatMessageID pgtype.UUID, msgID string, index int, kind channel.MsgType) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d",
		util.UUIDToString(chatMessageID), msgID, string(kind), index)))
	return path.Join(
		"workspaces",
		util.UUIDToString(inst.WorkspaceID),
		"wecom",
		util.UUIDToString(inst.ID),
		hex.EncodeToString(sum[:]),
	)
}

// describeMedia works out what to call the file and what to say it is. The
// callback body carries neither, so the name comes from the download's
// Content-Disposition and the type from the name's extension, falling back to
// sniffing the decrypted bytes — the extension is the better signal for the
// formats that are really zip containers (.docx, .xlsx), sniffing is the
// better one when there is no name at all.
func describeMedia(wm InboundMessage, index int, m InboundMedia, headerName string, plain []byte) (filename, contentType string) {
	filename = cleanMediaFilename(headerName)
	if ext := path.Ext(filename); ext != "" {
		contentType = mime.TypeByExtension(ext)
	}
	if contentType == "" {
		contentType = http.DetectContentType(plain)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if filename == "" {
		filename = fallbackMediaName(wm.MsgID, index, m.Kind, contentType)
	}
	return filename, contentType
}

// fallbackMediaName builds a name for an attachment the server did not name.
// It has to be unique within the message, so the attachment's position is in
// it — two photos in one 图文混排 otherwise land as one name twice.
func fallbackMediaName(msgID string, index int, kind channel.MsgType, contentType string) string {
	prefix := "wecom-file"
	switch kind {
	case channel.MsgTypeImage:
		prefix = "wecom-image"
	case channel.MsgTypeVideo:
		prefix = "wecom-video"
	}
	return fmt.Sprintf("%s-%s-%d%s", prefix, safeMediaSegment(msgID), index, mediaExtension(contentType))
}

// mediaExtension picks a file extension for a content type, preferring the
// familiar spelling over whatever the mime database happens to list first
// (image/jpeg resolves to ".jfif" on some systems).
func mediaExtension(contentType string) string {
	if semi := strings.IndexByte(contentType, ';'); semi >= 0 {
		contentType = strings.TrimSpace(contentType[:semi])
	}
	switch contentType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "application/pdf":
		return ".pdf"
	}
	if exts, err := mime.ExtensionsByType(contentType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	return ""
}

// safeMediaSegment reduces an id to characters that are safe in a filename.
func safeMediaSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "unknown"
	}
	return out
}

// ---- telling the sender ----

// mediaFailure is the kind of bad news, not the error itself: the sender gets
// told what to do differently, and "too big" and "did not arrive" have
// different answers.
type mediaFailure int

const (
	mediaFailureUnreadable mediaFailure = iota
	mediaFailureTooLarge
)

func classifyMediaFailure(err error) mediaFailure {
	if errors.Is(err, errMediaTooLarge) {
		return mediaFailureTooLarge
	}
	return mediaFailureUnreadable
}

// appendFailure keeps one entry per kind. A message with four attachments
// that all expired is one thing that went wrong, and saying it four times
// helps nobody.
func appendFailure(list []mediaFailure, f mediaFailure) []mediaFailure {
	for _, existing := range list {
		if existing == f {
			return list
		}
	}
	return append(list, f)
}

// tellTheSender writes one short notice into the chat the attachments came
// from. It goes through the same outbound registry every other wecom message
// uses, so a socket that is mid-reconnect holds it rather than dropping it.
//
// The agent run is a separate matter: it is already deferred behind the media
// deadline and will proceed with the placeholder text. That is deliberate —
// the person can see for themselves that the picture did not get through, and
// the answer to whatever they typed beside it is still worth having.
func (r *wecomMediaResolver) tellTheSender(inst engine.ResolvedInstallation, wm InboundMessage, failures []mediaFailure) {
	if len(failures) == 0 || r.notify == nil || !inst.ID.Valid {
		return
	}
	chatID := wm.ChatID
	if chatID == "" {
		chatID = wm.SenderUserID
	}
	if chatID == "" {
		return
	}
	c := copyFor(installationLocale(inst))
	lines := make([]string, 0, len(failures))
	for _, f := range failures {
		switch f {
		case mediaFailureTooLarge:
			lines = append(lines, c.MediaTooLarge)
		default:
			lines = append(lines, c.MediaUnreadable)
		}
	}
	chatType := chatTypeSingleInt
	if strings.EqualFold(wm.ChatType, "group") {
		chatType = chatTypeGroupInt
	}
	if err := r.notify.send(inst.ID, pendingSend{
		ChatID:   chatID,
		ChatType: chatType,
		Content:  strings.Join(lines, "\n"),
	}); err != nil {
		r.logger.Warn("wecom media failure notice not delivered",
			"installation_id", util.UUIDToString(inst.ID), "msg_id", wm.MsgID, "err", err)
	}
}

// installationLocale reads the copy language off the resolved installation,
// falling back to the default when the platform payload is not a wecom one.
func installationLocale(inst engine.ResolvedInstallation) Locale {
	if wi, ok := inst.Platform.(Installation); ok {
		return resolveLocale(wi.Locale)
	}
	return DefaultLocale
}
