package wecom

// media_upload.go — putting a file INTO a WeCom chat.
//
// The bot has no REST endpoint for this: media goes up the same WebSocket
// everything else uses, in three cmds and no access_token
// (https://developer.work.weixin.qq.com/document/path/101463).
//
//	aibot_upload_media_init   → declares the file, hands back an upload_id
//	aibot_upload_media_chunk  → one slice of the bytes, base64'd, × N
//	aibot_upload_media_finish → hands back the media_id a message can carry
//
// Two properties of the middle step are the whole design here: chunks may go
// out in any order and concurrently, and re-sending one is idempotent. So the
// chunks are uploaded a few at a time rather than in lockstep, and a chunk
// whose ack never comes back is simply sent again — losing one slice of a
// hundred must not cost the file.
//
// What it does NOT do is put a picture inside the streaming bubble. The long
// connection has no msg_item, so a file is always its own message; an answer
// with an attachment is the answer, then the file.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"
)

// The three upload cmds, and the frame the finished media rides out on.
const (
	cmdUploadMediaInit   = "aibot_upload_media_init"
	cmdUploadMediaChunk  = "aibot_upload_media_chunk"
	cmdUploadMediaFinish = "aibot_upload_media_finish"
)

// mediaMsgType is what WeCom will call this file once it is a message. The
// four values are the protocol's own; nothing else is accepted.
type mediaMsgType string

const (
	mediaTypeFile  mediaMsgType = "file"
	mediaTypeImage mediaMsgType = "image"
	mediaTypeVoice mediaMsgType = "voice"
	mediaTypeVideo mediaMsgType = "video"
)

// mediaChunkBytes is the cap on one chunk BEFORE base64, so a frame on the
// wire is about a third larger again. maxMediaChunks is the cap on how many
// there may be, and the two together are the largest file this transport can
// carry — 50 MB, well under the 100 MB an inbound attachment may be.
const (
	mediaChunkBytes     = 512 * 1024
	maxMediaChunks      = 100
	maxMediaUploadBytes = mediaChunkBytes * maxMediaChunks
)

// The byte caps on a video message's two required fields.
const (
	videoTitleBytes       = 64
	videoDescriptionBytes = 512
)

// mediaChunkParallelism is how many chunks are in flight at once. The protocol
// allows any number; this is a courtesy to the socket, which every other frame
// on this connection shares — a 50 MB file at full tilt would stall the
// progress refreshes of every other chat the bot is in.
const mediaChunkParallelism = 4

// mediaChunkAttempts is how many times one chunk is offered. The second try
// exists for a lost ack and nothing else: a chunk the server refuses is
// refused again, so a rejection ends the upload on the spot.
const mediaChunkAttempts = 2

var (
	// errMediaUploadTooLarge — past what the chunk protocol can express. The
	// caller says so in words rather than starting an upload that cannot end.
	errMediaUploadTooLarge = fmt.Errorf("wecom: media exceeds the %d byte upload limit", maxMediaUploadBytes)

	// errMediaUploadEmpty — a zero-byte file. WeCom has nothing to store and
	// the user has nothing to open.
	errMediaUploadEmpty = errors.New("wecom: media has no bytes to upload")
)

// outboundMedia is one file on its way to a chat.
type outboundMedia struct {
	// Kind decides which msgtype the finished message uses, and WeCom
	// validates the bytes against it — a .pptx declared as an image is
	// refused, not converted.
	Kind mediaMsgType

	// Filename is what the recipient sees on the file card. It is also the
	// only hint the server gets about the format, so it keeps its extension.
	Filename string

	Data []byte
}

func (m outboundMedia) validate() error {
	switch m.Kind {
	case mediaTypeFile, mediaTypeImage, mediaTypeVoice, mediaTypeVideo:
	default:
		return fmt.Errorf("wecom: %q is not an uploadable media type", m.Kind)
	}
	if strings.TrimSpace(m.Filename) == "" {
		return errors.New("wecom: media upload requires a filename")
	}
	return nil
}

// mediaSend is a finished upload addressed as a message.
type mediaSend struct {
	Kind    mediaMsgType
	MediaID string

	// Title and Description are video's alone — the other three kinds have no
	// field for them and reject a body that carries one.
	Title       string
	Description string
}

// splitMediaChunks cuts a file into the slices the protocol will take. The
// slices alias the input; nothing is copied until a chunk is base64'd for its
// own frame.
func splitMediaChunks(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, errMediaUploadEmpty
	}
	if len(data) > maxMediaUploadBytes {
		return nil, errMediaUploadTooLarge
	}
	chunks := make([][]byte, 0, (len(data)+mediaChunkBytes-1)/mediaChunkBytes)
	for start := 0; start < len(data); start += mediaChunkBytes {
		end := start + mediaChunkBytes
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[start:end])
	}
	return chunks, nil
}

// uploadMedia carries one file through the three steps and returns the
// media_id a message can be built around. ctx bounds the whole thing.
func (s *wsSender) uploadMedia(ctx context.Context, m outboundMedia) (string, error) {
	if err := m.validate(); err != nil {
		return "", err
	}
	chunks, err := splitMediaChunks(m.Data)
	if err != nil {
		return "", err
	}
	uploadID, err := s.uploadMediaInit(ctx, m, len(chunks))
	if err != nil {
		return "", err
	}
	if err := s.uploadMediaChunks(ctx, uploadID, chunks); err != nil {
		return "", err
	}
	return s.uploadMediaFinish(ctx, uploadID)
}

// uploadMediaInit declares the file and takes the upload_id back.
//
// The optional md5 field is deliberately not sent. The document lists it
// without saying what it is taken over — the raw file or the base64 of it —
// and a server that checks a value we guessed wrong would refuse every upload
// with an errcode that says nothing about why.
func (s *wsSender) uploadMediaInit(ctx context.Context, m outboundMedia, chunks int) (string, error) {
	body, err := s.request(ctx, cmdUploadMediaInit, map[string]any{
		"type":         string(m.Kind),
		"filename":     m.Filename,
		"total_size":   len(m.Data),
		"total_chunks": chunks,
	})
	if err != nil {
		return "", err
	}
	var res struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return "", fmt.Errorf("wecom: decode upload init response: %w", err)
	}
	if res.UploadID == "" {
		return "", errors.New("wecom: upload init returned no upload_id")
	}
	return res.UploadID, nil
}

// uploadMediaChunks sends every slice, a few at a time. The first failure
// cancels the rest: there is no partial upload worth finishing.
func (s *wsSender) uploadMediaChunks(ctx context.Context, uploadID string, chunks [][]byte) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(mediaChunkParallelism)
	for i, chunk := range chunks {
		g.Go(func() error { return s.uploadMediaChunk(gctx, uploadID, i, chunk) })
	}
	return g.Wait()
}

// uploadMediaChunk sends one slice, once more if its verdict never arrived.
//
// chunk_index goes on the wire as a number. The field table in the document
// calls it a string and the worked example beside it passes a number; WeCom's
// own SDK sends a number, so that is what the server is known to accept.
func (s *wsSender) uploadMediaChunk(ctx context.Context, uploadID string, index int, chunk []byte) error {
	body := map[string]any{
		"upload_id":   uploadID,
		"chunk_index": index,
		"base64_data": base64.StdEncoding.EncodeToString(chunk),
	}
	var lastErr error
	for attempt := 0; attempt < mediaChunkAttempts; attempt++ {
		_, err := s.request(ctx, cmdUploadMediaChunk, body)
		if err == nil {
			return nil
		}
		lastErr = err
		// A refusal is the server's answer and will be its answer again. Only
		// a verdict that never came back is worth a second offer, and the
		// protocol's own idempotence is what makes that safe.
		if !errors.Is(err, errUploadAckTimeout) {
			break
		}
		s.log.Warn("wecom: media chunk got no verdict, sending it again",
			"upload_id", uploadID, "chunk_index", index, "attempt", attempt+1)
	}
	return fmt.Errorf("wecom: upload chunk %d: %w", index, lastErr)
}

// uploadMediaFinish seals the upload and takes the media_id back.
func (s *wsSender) uploadMediaFinish(ctx context.Context, uploadID string) (string, error) {
	body, err := s.request(ctx, cmdUploadMediaFinish, map[string]any{"upload_id": uploadID})
	if err != nil {
		return "", err
	}
	var res struct {
		MediaID string `json:"media_id"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return "", fmt.Errorf("wecom: decode upload finish response: %w", err)
	}
	if res.MediaID == "" {
		return "", errors.New("wecom: upload finish returned no media_id")
	}
	return res.MediaID, nil
}

// sendMedia delivers an uploaded file as a message. The documentation leaves
// two routes open and contradicts itself about them, so both are wired.
//
// aibot_send_msg goes first. Its field table lists only template_card and
// markdown — and WeCom's own Node SDK pushes media on it, which is the one
// piece of evidence either way that comes from working code. It also carries
// the chat id, so it reaches the right conversation whatever state the turn is
// in.
//
// aibot_respond_msg is the fallback, and only when the caller still has the
// turn's req_id. Its msgtype table does name file, image, voice and video, so
// if the push turns out to be refused this is what the document says to use
// instead. Second rather than first for two reasons: on the chat-done path the
// turn's req_id has already carried a streaming bubble to its sealed end, and
// a passive reply on a spent req_id is both undocumented and — while any frame
// on that req_id is still awaiting its verdict — able to be handed an ack that
// was not its own. Running it only after an explicit refusal puts it well clear
// of both.
//
// Which of the two a real bot accepts has not been observed; that is what the
// fallback is for.
func (s *wsSender) sendMedia(ctx context.Context, chatID string, chatType int, reqID string, m mediaSend) error {
	body, err := sendMsgMediaBody(chatID, chatType, m)
	if err != nil {
		return err
	}
	pushErr := func() error { _, err := s.request(ctx, cmdSendMsg, body); return err }()
	if pushErr == nil {
		// Which command a live bot accepts for media is not something the
		// documentation settles, so say which one carried the file. Without
		// this line a successful delivery leaves no trace at all and the only
		// way to tell the two routes apart is to ask the person who received
		// it.
		s.log.Info("wecom: media delivered by push",
			"route", "aibot_send_msg", "media_id", m.MediaID, "msgtype", string(m.Kind))
		return nil
	}
	if reqID == "" {
		return pushErr
	}
	s.log.Warn("wecom: media push refused, answering the turn instead",
		"media_id", m.MediaID, "msgtype", string(m.Kind), "error", pushErr)

	passive, err := respondMediaBody(m)
	if err != nil {
		return err
	}
	if _, err := s.requestWithID(ctx, reqID, cmdRespondMsg, passive); err != nil {
		return fmt.Errorf("wecom: media push refused (%v) and the passive reply failed: %w", pushErr, err)
	}
	s.log.Info("wecom: media delivered by passive reply",
		"route", "aibot_respond_msg", "media_id", m.MediaID, "msgtype", string(m.Kind))
	return nil
}

// ---- request/response over the socket ----

// errUploadAckTimeout — the frame went out and no answer came back inside the
// ack window. Distinct from a refusal, because only this one is worth
// retrying.
var errUploadAckTimeout = errors.New("wecom: no response to the request frame")

// wecomAPIError is a server refusal of one request frame, carrying the errcode
// so a log line and a retry decision have something to go on.
type wecomAPIError struct {
	Cmd  string
	Code int
	Msg  string
}

func (e *wecomAPIError) Error() string {
	return fmt.Sprintf("wecom: %s rejected errcode=%d errmsg=%s", e.Cmd, e.Code, e.Msg)
}

// request writes one frame under a req_id of our own and waits for the whole
// answer. Used by every cmd whose response says something beyond "accepted".
func (s *wsSender) request(ctx context.Context, cmd string, body map[string]any) (json.RawMessage, error) {
	return s.requestWithID(ctx, newReqID(), cmd, body)
}

// requestWithID is request() for a frame whose req_id is not ours to choose —
// a passive reply, which must echo the req_id of the callback that opened the
// turn or the server refuses it.
func (s *wsSender) requestWithID(ctx context.Context, reqID, cmd string, body map[string]any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w, ok := s.awaitReply(reqID)
	if !ok {
		return nil, fmt.Errorf("wecom: %s req_id %s is already awaiting a response", cmd, reqID)
	}
	defer s.cancelReply(reqID, w)

	if err := s.writeWithContext(ctx, map[string]any{
		"cmd":     cmd,
		"headers": frameHeaders{ReqID: reqID},
		"body":    body,
	}); err != nil {
		return nil, err
	}

	timer := time.NewTimer(s.ackTimeout)
	defer timer.Stop()
	select {
	case res := <-w.ch:
		if res.code != 0 {
			return nil, &wecomAPIError{Cmd: cmd, Code: res.code, Msg: res.msg}
		}
		return res.body, nil
	case <-timer.C:
		return nil, errUploadAckTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ---- message bodies ----

// sendMsgMediaBody builds an aibot_send_msg body carrying media.
func sendMsgMediaBody(chatID string, chatType int, m mediaSend) (map[string]any, error) {
	if chatID == "" {
		return nil, errors.New("wecom: send_msg requires chat_id")
	}
	if chatType != chatTypeSingleInt && chatType != chatTypeGroupInt {
		return nil, errors.New("wecom: send_msg chat_type must be 1 (single) or 2 (group)")
	}
	body, err := mediaBodyFields(m)
	if err != nil {
		return nil, err
	}
	body["chatid"] = chatID
	body["chat_type"] = chatType
	return body, nil
}

// respondMediaBody builds an aibot_respond_msg body carrying media. It is
// addressed by the frame's req_id rather than by a chat id, which is why there
// is no chatid here.
func respondMediaBody(m mediaSend) (map[string]any, error) {
	return mediaBodyFields(m)
}

// mediaBodyFields is the {msgtype, <kind>:{...}} pair both frames share.
func mediaBodyFields(m mediaSend) (map[string]any, error) {
	if m.MediaID == "" {
		return nil, errors.New("wecom: media message requires a media_id")
	}
	nested := map[string]any{"media_id": m.MediaID}
	switch m.Kind {
	case mediaTypeFile, mediaTypeImage, mediaTypeVoice:
	case mediaTypeVideo:
		nested["title"] = clipUTF8(m.Title, videoTitleBytes)
		nested["description"] = clipUTF8(m.Description, videoDescriptionBytes)
	default:
		return nil, fmt.Errorf("wecom: %q is not a media msgtype", m.Kind)
	}
	return map[string]any{
		"msgtype":      string(m.Kind),
		string(m.Kind): nested,
	}, nil
}

// clipUTF8 cuts a string to a byte budget on a character boundary. WeCom
// counts bytes, a Chinese title spends three of them per character, and a cut
// through the middle of one sends the server broken UTF-8.
func clipUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
