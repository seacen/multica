package wecom

// ws_frame.go — the aibot WebSocket wire format. Every frame is JSON with a
// {cmd, headers.req_id, body} envelope. We only parse the frames we act on:
//
//   inbound   — aibot_msg_callback (user message), aibot_event_callback (event)
//   outbound  — aibot_subscribe (auth), ping (heartbeat), aibot_send_msg (push),
//               aibot_respond_msg (in-window reply)
//   response  — the ack the server writes for aibot_subscribe / ping / send_msg
//
// The wire is documented at https://developer.work.weixin.qq.com/document/path/101463 .

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// Frame commands the client sends.
const (
	cmdSubscribe  = "aibot_subscribe"
	cmdPing       = "ping"
	cmdSendMsg    = "aibot_send_msg"
	cmdRespondMsg = "aibot_respond_msg"
)

// Frame commands the server sends. These are what the read loop switches on.
const (
	cmdMsgCallback   = "aibot_msg_callback"
	cmdEventCallback = "aibot_event_callback"
	cmdServerPing    = "ping"
	cmdPong          = "pong"
)

// Event types inside aibot_event_callback.body.event.eventtype.
const (
	eventDisconnected = "disconnected_event"
	eventEnterChat    = "enter_chat"
	eventTemplateCard = "template_card_event"
	eventFeedback     = "feedback_event"
)

// aibot receiver kinds for aibot_send_msg. WeCom uses ints, not strings.
const (
	chatTypeSingleInt = 1
	chatTypeGroupInt  = 2
)

// frameHeaders carries a per-frame correlation id. Server acks reflect the
// req_id back so the client can pair requests with responses.
type frameHeaders struct {
	ReqID string `json:"req_id"`
}

// frameEnvelope is the outer shape of every frame the server pushes. Body
// is left raw so downstream code can unmarshal the specific shape without
// re-parsing the outer wrapper.
type frameEnvelope struct {
	Cmd     string          `json:"cmd"`
	Headers frameHeaders    `json:"headers"`
	Body    json.RawMessage `json:"body"`

	// Response fields (present when the server acks one of our writes).
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// aibotMsgCallback is the body of an aibot_msg_callback frame — a user
// message pushed from a chat to the bot.
type aibotMsgCallback struct {
	MsgID    string `json:"msgid"`
	AIBotID  string `json:"aibotid"`
	ChatID   string `json:"chatid"`
	ChatType string `json:"chattype"` // "single" | "group"
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"` // "text" | "image" | "voice" | "file" | "video" | "mixed"
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
	// Image / File / Video are the downloadable kinds. Each carries only a
	// pre-signed COS url and the key its bytes are encrypted with — no name,
	// no size, no MIME type (see media_download.go for where those come from
	// instead).
	Image mediaBody `json:"image"`
	File  mediaBody `json:"file"`
	Video mediaBody `json:"video"`
	// Mixed carries 图文混排 — a message the user composed with text runs
	// and attachments interleaved. Each item is itself typed and carries the
	// same bodies a standalone message of that type would.
	Mixed struct {
		MsgItem []mixedItem `json:"msg_item"`
	} `json:"mixed"`
	// Voice carries the TRANSCRIPT, not audio. WeCom runs the speech
	// recognition on its side and delivers only the result, so a voice note
	// needs no download, no media key and no storage — it is a sentence that
	// happened to be spoken. Not gated on chat type: whatever chat a voice
	// note arrives from, the transcript is read the same way. mixedItem
	// carries the same field, because a voice run inside a 图文混排 would
	// otherwise drop a spoken sentence out of the middle of a message whose
	// other runs are read.
	Voice struct {
		Content string `json:"content"`
	} `json:"voice"`
	// Quote is the message this one is a reply to, present when the sender
	// used 引用. Only a text or 图文混排 message carries one.
	//
	// It is the quoted message's CONTENT and nothing about its provenance.
	// The documented fields are msgtype plus the typed body that goes with it
	// — text.content, voice.content (already transcribed), image/file/video
	// as the same {url, aeskey} pair a standalone attachment carries in
	// long-connection mode, and mixed.msg_item for a quoted 图文混排. There is
	// no from.userid, no msgid, no create_time: who said it and when are not
	// on the wire, and no amount of rendering can put them there.
	//
	// Which is why the media reference matters. It is the only field that can
	// tell a quote of a picture the agent has already read from a quote of one
	// it has never seen — see attachments.
	Quote *quotedMessage `json:"quote"`
}

// quotedMessage is the 引用 payload: any message kind, nested. It reuses
// mixedItem because the shape is the same one — a msgtype and the typed body
// that goes with it — and a quoted 图文混排 nests its own runs inside.
type quotedMessage struct {
	mixedItem
	Mixed struct {
		MsgItem []mixedItem `json:"msg_item"`
	} `json:"mixed"`
}

// render turns the quoted message into the text it contributes. An attachment
// renders as quotedMediaPlaceholder, which is the ordinary placeholder with
// room in it to name the attachment; the bytes behind it arrive through
// media, on the same detached path an inbound attachment takes.
func (q *quotedMessage) render() string {
	if q == nil {
		return ""
	}
	if strings.EqualFold(q.MsgType, "mixed") {
		var runs []string
		for _, item := range q.Mixed.MsgItem {
			if s := item.renderQuoted(); s != "" {
				runs = append(runs, s)
			}
		}
		return strings.Join(runs, "\n")
	}
	return q.mixedItem.renderQuoted()
}

// media lists the quoted message's downloadable attachments, in the order
// render lays their placeholders out, each stamped with the marker it stands
// for.
//
// Fetching them is the whole of the fix for a quoted picture. `> Quoted:
// [Image]` is all a quote of an image can be rendered as — the payload carries
// no sender, no message id and no timestamp to say WHICH image — so an agent
// reading that line cannot tell a screenshot it read a minute ago from one it
// has never seen, and answers "左下角那块看不清" about a picture it does not
// have. The url and key on the quote are the only thing that resolves it, and
// they are handed over in this callback like any other attachment's.
//
// The stamp is what closes the last half of that. Fetching the bytes puts the
// picture in the attachment list; the marker's occurrence number is what says
// WHICH entry of that list the quote's placeholder is, and without it a
// message carrying two attachments — one quoted, one just sent — hands the
// agent two markers and two ids with nothing joining them.
func (q *quotedMessage) media() []InboundMedia {
	if q == nil {
		return nil
	}
	var out []InboundMedia
	if strings.EqualFold(q.MsgType, "mixed") {
		for _, item := range q.Mixed.MsgItem {
			out = append(out, item.media()...)
		}
	} else {
		out = q.mixedItem.media()
	}
	// Counted per marker, not over the whole list: "[Image: unavailable]" and
	// "[File: unavailable]" are different strings, so the second picture in a
	// quote that also carried a document is still that marker's first
	// occurrence. Counting them together would send the binder looking for a
	// second "[Image: unavailable]" that is not there and leave the picture
	// unnamed.
	seen := make(map[string]int, len(out))
	for i := range out {
		marker := quotedMediaPlaceholder(out[i].Kind)
		out[i].InlinePlaceholder = marker
		out[i].InlineIndex = seen[marker]
		seen[marker]++
	}
	return out
}

// mediaBody is the {url, aeskey} pair every downloadable kind carries. In
// long-connection mode the key is minted per url, so it lives on the message
// rather than in configuration.
type mediaBody struct {
	URL    string `json:"url"`
	AESKey string `json:"aeskey"`
}

// mixedItem is one run of a 图文混排 message: a sentence, a spoken sentence,
// or an attachment, in the order the user composed them.
type mixedItem struct {
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
	Voice struct {
		Content string `json:"content"`
	} `json:"voice"`
	Image mediaBody `json:"image"`
	File  mediaBody `json:"file"`
	Video mediaBody `json:"video"`
}

// words is the part of one 图文混排 run the SENDER typed or said. An
// attachment contributes nothing, which is the difference between this and
// render below.
func (item mixedItem) words() string {
	switch strings.ToLower(item.MsgType) {
	case "text":
		return strings.TrimSpace(item.Text.Content)
	case "voice":
		// WeCom runs the speech recognition on its side and delivers only
		// the result, so a voice run is a sentence that happened to be
		// spoken — no download, no key. It is the sender's own words, so a
		// spoken "/issue 登录坏了" is a command like the typed one.
		return strings.TrimSpace(item.Voice.Content)
	default:
		return ""
	}
}

// render turns one 图文混排 run into the line it contributes to the message
// body. An item of a kind this adapter does not know contributes nothing
// rather than a stray placeholder.
func (item mixedItem) render() string {
	return item.renderWith(mediaPlaceholder)
}

// renderQuoted is render for a run inside a 引用 block: same line, but an
// attachment gets the marker that can be joined to the attachment it stands
// for. See quotedMediaPlaceholder.
func (item mixedItem) renderQuoted() string {
	return item.renderWith(quotedMediaPlaceholder)
}

func (item mixedItem) renderWith(placeholder func(channel.MsgType) string) string {
	if s := item.words(); s != "" {
		return s
	}
	body, kind, ok := mediaFor(item.MsgType, item.Image, item.File, item.Video)
	if !ok || strings.TrimSpace(body.URL) == "" {
		return ""
	}
	return placeholder(kind)
}

// media is the downloadable attachment behind this run's placeholder, if it
// has one. Paired with render on purpose: a run that renders a placeholder
// must produce an attachment here and a run that renders nothing must produce
// none, or the positional correspondence the body relies on slips by one.
func (item mixedItem) media() []InboundMedia {
	body, kind, ok := mediaFor(item.MsgType, item.Image, item.File, item.Video)
	if !ok || strings.TrimSpace(body.URL) == "" {
		return nil
	}
	return []InboundMedia{{Kind: kind, URL: body.URL, AESKey: body.AESKey}}
}

// mediaPlaceholder is the marker that stands in for an attachment in the
// stored message body, so the agent can see that something was attached
// before (or instead of) the bytes arriving on the detached media path.
//
// The exact strings are Lark's and DingTalk's, byte for byte:
// lark/content_flatten.go flattenContent returns "[Image]" / "[File]" /
// "[Video]", and dingtalk/inbound.go:95 pins dingtalkImagePlaceholder =
// "[Image]" with "[File]" at inbound.go:205. An agent reads every channel
// through the same prompt; a wecom-only spelling would be one more thing
// for it to learn for no reason.
func mediaPlaceholder(kind channel.MsgType) string {
	switch kind {
	case channel.MsgTypeImage:
		return "[Image]"
	case channel.MsgTypeVideo:
		return "[Video]"
	default:
		return "[File]"
	}
}

// mediaUnavailable is what a quoted attachment's marker says while there is
// no attachment to name. English, like the placeholders it goes inside, and
// for the same reason: an agent reads every channel through one prompt, so
// this vocabulary is the agent's rather than the chat's, and putting it in
// the copy pack would make the same picture read differently to the same
// agent depending on whose chat it came from.
const mediaUnavailable = "unavailable"

// quotedMediaPlaceholder is the marker a QUOTED attachment renders as. It
// carries a word inside the brackets where the sender's own "[Image]" carries
// nothing, because the quote's placeholder is the one that has to be joined
// to an attachment.
//
// Fetching the quoted picture (see quotedMessage.media) put it in the
// attachment list and stopped there. The list is a separate block of text
// from the body, so with one attachment the reader infers the join and with
// two — one quoted, one just sent — there is nothing to infer from: two bare
// "[Image]" markers, two ids, no correspondence beyond an ordering rule
// nobody stated. So the marker names its attachment, "[Image: 019fe1d3-…]",
// and the join stops being an inference.
//
// The word is what the marker says BEFORE there is an id, and it is written
// so that it stays true if no id ever arrives: a download that fails, a host
// the media address guard refuses, no storage configured, an intent the
// reconciler already owns and a bind that never commits all leave it reading
// "unavailable", and only an attachment row the agent can actually fetch
// replaces it. Which is the distinction that matters to a reader — "there was
// a picture here and it did not arrive" is a marker with no id, "there was no
// picture" is no marker at all.
func quotedMediaPlaceholder(kind channel.MsgType) string {
	return namedMediaPlaceholder(kind, mediaUnavailable)
}

// namedMediaPlaceholder writes a name inside the bracketed placeholder:
// "[Image]" becomes "[Image: <name>]". engine.bindMediaRefs rewrites the same
// shape when it swaps the attachment's id in, and keeps the label ahead of
// the colon, so the two spellings cannot drift apart.
func namedMediaPlaceholder(kind channel.MsgType, name string) string {
	return strings.TrimSuffix(mediaPlaceholder(kind), "]") + ": " + name + "]"
}

// mediaFor returns the body and normalized kind for a raw wecom msgtype, and
// whether that type is one we download at all.
func mediaFor(msgType string, image, file, video mediaBody) (mediaBody, channel.MsgType, bool) {
	switch strings.ToLower(msgType) {
	case "image":
		return image, channel.MsgTypeImage, true
	case "file":
		return file, channel.MsgTypeFile, true
	case "video":
		return video, channel.MsgTypeVideo, true
	default:
		return mediaBody{}, channel.MsgTypeUnknown, false
	}
}

// attachments lists the downloadable media on this callback, in the order the
// user sent it. A body with no url is skipped: there is nothing to fetch, and
// carrying it forward would only produce an intent-ledger row for an object
// that can never exist.
func (mc aibotMsgCallback) attachments() []InboundMedia {
	// The quoted message's attachments come FIRST, because that is where its
	// placeholders are: routableText renders the quote block above the
	// sender's own words. The Nth placeholder in the body is the Nth
	// attachment here, and that correspondence is the only thing an agent has
	// to work out which picture is which — 图文混排 already depends on it.
	out := mc.Quote.media()
	if body, kind, ok := mediaFor(mc.MsgType, mc.Image, mc.File, mc.Video); ok {
		if strings.TrimSpace(body.URL) != "" {
			out = append(out, InboundMedia{Kind: kind, URL: body.URL, AESKey: body.AESKey})
		}
		return out
	}
	if !strings.EqualFold(mc.MsgType, "mixed") {
		return out
	}
	for _, item := range mc.Mixed.MsgItem {
		out = append(out, item.media()...)
	}
	return out
}

// needsCopy reports whether routing this callback will read anything off the
// copy pack — a quote prefix, or (for a kind we cannot read) the
// unsupported-type receipt. Plain text with nothing quoted does not, which is
// what lets the read loop skip the per-destination language lookup for the
// overwhelmingly common case.
func (mc aibotMsgCallback) needsCopy() bool {
	if strings.EqualFold(mc.MsgType, "text") {
		return mc.Quote.render() != ""
	}
	return true
}

// routableText returns the text this callback is stored and read as, and
// whether there is any. It is the message's own body with any quoted message
// rendered above it — see ownText for the body and quotedMessage.render for
// the quote.
//
// Quoting is how a person asks about one specific thing in a busy room: they
// long-press the message, hit 引用, and type their question under it. Without
// the quote the agent is handed the question with its subject removed — "这个
// 数对吗" about nothing — and answers into the void.
//
// A quote decorates a message that was readable on its own; it does not
// rescue one that was not. Ingesting the quote off the back of a kind we
// cannot read would put somebody else's words in as this person's message.
func (mc aibotMsgCallback) routableText(c copyPack) (string, bool) {
	own, ok := mc.ownText()
	quoted := mc.Quote.render()
	if quoted == "" {
		return own, ok
	}
	if !ok {
		return "", false
	}
	block := renderQuoteBlock(c, quoted)
	if strings.TrimSpace(own) == "" {
		// Quoting something and saying nothing is "look at this", which is a
		// message worth answering.
		return block, true
	}
	return block + "\n" + own, true
}

// renderQuoteBlock marks every line of the quote, not only the first: an
// unmarked second paragraph reads as the sender's own words.
func renderQuoteBlock(c copyPack, quoted string) string {
	var b strings.Builder
	for i, line := range strings.Split(quoted, "\n") {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString("> ")
		if i == 0 {
			b.WriteString(c.QuotePrefix)
		}
		b.WriteString(line)
	}
	return b.String()
}

// ownText is the agent-readable body of this callback, and whether there is
// one at all.
//
// Plain text answers with its body; a photo, file or video answers with a
// bracketed placeholder, because the bytes arrive later on a detached path
// and the message has to say something in the meantime (the placeholder is
// also what survives if the download never succeeds); 图文混排 answers with
// its runs rendered in the order they were composed, so "look at this" still
// reads above the picture it was written about.
//
// A standalone voice note answers with its transcript. Recognition comes back
// empty on background noise or a half-second press, and an empty body would
// reach the agent as a turn with nothing in it — so an empty transcript
// reports false and takes the receipt path.
//
// Everything else — a location card, a kind WeCom adds next year — answers
// false and takes the receipt path too.
func (mc aibotMsgCallback) ownText() (string, bool) {
	switch strings.ToLower(mc.MsgType) {
	case "text":
		return mc.Text.Content, true
	case "voice":
		transcript := strings.TrimSpace(mc.Voice.Content)
		return transcript, transcript != ""
	case "image", "file", "video":
		body, kind, _ := mediaFor(mc.MsgType, mc.Image, mc.File, mc.Video)
		if strings.TrimSpace(body.URL) == "" {
			return "", false
		}
		return mediaPlaceholder(kind), true
	case "mixed":
		var runs []string
		for _, item := range mc.Mixed.MsgItem {
			if s := item.render(); s != "" {
				runs = append(runs, s)
			}
		}
		if len(runs) == 0 {
			return "", false
		}
		return strings.Join(runs, "\n"), true
	default:
		return "", false
	}
}

// ownCommandSource is what the slash-command parsers read: the sender's own
// words, and nothing this adapter wrote.
//
// It is not ownText. ownText inserts "[Image]" / "[File]" / "[Video]" where
// the attachments were, because the stored body has to show that something
// was attached and where. engine.ParseIssueCommand only ever looks at the
// FIRST non-empty line, so a person who attaches a screenshot and then types
// "/issue 登录坏了" — the natural order, and the one WeCom's composer
// encourages — produces a body opening with "[Image]", the parser sees a
// placeholder instead of the command, no issue is filed, and nothing anywhere
// tells them why. Typing the same two things in the other order works. That
// is not a distinction a user can be expected to know about.
//
// So the command source drops the placeholders and keeps the runs the sender
// authored, in order. The stored body still carries them: cutting them there
// would lose the position the detached media binder materializes into
// (engine/issue_command.go issueDescriptionFromCommandBody says the same
// thing from the other end).
//
// A standalone photo, file or video answers with nothing. Its whole body is a
// placeholder, so there are no words in it to parse, and handing the parser a
// string the sender never typed is the defect this exists to remove.
func (mc aibotMsgCallback) ownCommandSource() string {
	switch strings.ToLower(mc.MsgType) {
	case "text":
		return mc.Text.Content
	case "voice":
		// A transcript is the sender's own words, so a spoken "/issue 登录坏了"
		// is a command exactly like the typed one — the same rule mixedItem
		// applies to a voice run inside a 图文混排.
		return strings.TrimSpace(mc.Voice.Content)
	case "mixed":
		var runs []string
		for _, item := range mc.Mixed.MsgItem {
			if s := item.words(); s != "" {
				runs = append(runs, s)
			}
		}
		return strings.Join(runs, "\n")
	default:
		return ""
	}
}

// aibotEventCallback is the body of an aibot_event_callback frame. We only
// look at the event type; specific event fields (template-card selection,
// feedback vote) are not surfaced yet.
type aibotEventCallback struct {
	Event struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

// ---- normalization ----

// InboundMessage is the wecom-side flattened envelope the WS read loop
// builds from a decoded aibot_msg_callback. It is stashed into
// channel.InboundMessage.Raw as JSON so wecom_resolvers.go can reach the
// platform-specific fields (BotID, ReqID) the cross-platform envelope does
// not carry.
type InboundMessage struct {
	// BotID is the smart-bot identifier this event was delivered to. It
	// is the routing key the installation resolver uses.
	BotID string `json:"bot_id"`

	// MsgID is the WeCom per-message identifier used for two-phase dedup.
	MsgID string `json:"msg_id,omitempty"`

	// MsgType is the raw wecom type ("text", "image", "event", ...). Media
	// / unknown types round-trip via the cross-platform channel.MsgType enum
	// (see channelMsgType); the raw string stays here for auditing.
	MsgType string `json:"msg_type,omitempty"`

	// ChatType is the tencent-internal conversation discriminator
	// ("single" for 1:1, "group" for a group chat).
	ChatType string `json:"chat_type,omitempty"`

	// ChatID is the userid (single) or chatid (group) that the message
	// originated in — the routing identity for outbound + session binding.
	ChatID string `json:"chat_id,omitempty"`

	// SenderUserID is the userid of the person who typed the message.
	SenderUserID string `json:"sender_user_id,omitempty"`

	// Content is the human-readable body: the user's words — typed for
	// MsgType == "text", what WeCom's recognition returned for
	// MsgType == "voice" — and the placeholders standing in for their
	// attachments. The cross-platform envelope's Text field is populated
	// from this.
	Content string `json:"content,omitempty"`

	// ReqID is the frame req_id the server sent this message with. An
	// aibot_respond_msg — the in-window reply — has to echo it; this adapter
	// does not send one, and keeps the id so a caller that does can.
	//
	// There is no short window on it. Replies are allowed for 24 hours after the
	// callback, and the id is not tied to the connection that received it: a
	// stream opened on one connection took a refresh and a closing frame from a
	// second one, measured against a live bot. The bounds that do apply are the
	// stream's own lifetime (see errcodeStreamExpired) and 846605 for an id the
	// server does not recognise.
	ReqID string `json:"req_id,omitempty"`

	// Media lists the attachments to fetch, in the order the user sent them.
	// It is the MediaResolver's input and travels only in
	// channel.InboundMessage.Raw, which the engine passes along in memory and
	// never persists — the urls lapse after five minutes and the keys are
	// single-use, so neither belongs in a table or a log line.
	Media []InboundMedia `json:"media,omitempty"`
}

// InboundMedia is one downloadable attachment on a callback.
type InboundMedia struct {
	// Kind is the normalized media type the attachment row is labelled with.
	Kind channel.MsgType `json:"kind"`
	// URL is the pre-signed COS address, good for five minutes, needing no
	// access token.
	URL string `json:"url"`
	// AESKey unlocks what comes back from URL. Long-connection mode mints one
	// per url; see media_crypt.go.
	AESKey string `json:"aeskey"`
	// InlinePlaceholder is the exact marker in the stored body this attachment
	// stands for, and InlineIndex is which occurrence of that marker it is.
	// They travel onto channel.MediaRef, and the binder rewrites that one
	// occurrence to name the attachment once a row exists (engine/session.go).
	//
	// Set for a QUOTED attachment only. The sender's own "[Image]" keeps the
	// bare marker it has always had: it is already unambiguous, being the
	// attachment on the message the reader is looking at, and rewriting it
	// would change what every existing wecom message body says.
	InlinePlaceholder string `json:"inline_placeholder,omitempty"`
	InlineIndex       int    `json:"inline_index,omitempty"`
}

// inline copies the marker this attachment stands for onto the ref the binder
// will rewrite. A ref with no marker is left exactly as it was, which is what
// keeps the sender's own attachments on the path they were already on.
func (m InboundMedia) inline(ref channel.MediaRef) channel.MediaRef {
	if m.InlinePlaceholder == "" {
		return ref
	}
	ref.InlinePlaceholder = m.InlinePlaceholder
	ref.InlineIndex = m.InlineIndex
	// The marker refers to the attachment, it does not carry it: the picture
	// belongs to a message somebody else sent, and replacing the marker with
	// an inline image would state this sender attached it again.
	ref.InlineIDOnly = true
	return ref
}

// channelMessageFromCallback converts a wecom-side aibot_msg_callback into
// the cross-platform channel.InboundMessage the engine.Router consumes.
// The wecom-side InboundMessage is stashed in Raw so wecom_resolvers.go can
// access platform-specific fields.
//
// Routing identity:
//   - single → ChatType=p2p,  ChatID=userid,  SenderID=userid
//   - group  → ChatType=group, ChatID=chatid,  SenderID=from.userid
//
// A user @-mentioning the bot in a group is not distinguishable from a raw
// group message on the wire — WeCom only forwards to the bot when it was
// addressed, so any received group message counts as addressed.
//
// c is the destination's copy pack, needed again here to recompose the body
// when a directive is stripped off it — see forceFresh below.
//
// text is the agent-readable body the caller already resolved via routableText.
// It is passed in rather than recomputed because the caller has to know
// whether the message is routable at all before it gets here. The command
// source is a different string and is derived here from mc — see
// ownCommandSource for why they must not be the same one.
//
// botDisplayName is the bot's name in a chat, from the installation config. It
// is used for one thing: recognising where the sender's @-mention ends. Empty
// is fine and falls back to a whitespace heuristic; see stripLeadingMentions.
func channelMessageFromCallback(botID, botDisplayName string, mc aibotMsgCallback, c copyPack, text, reqID string) channel.InboundMessage {
	chatType := channel.ChatTypeP2P
	if strings.EqualFold(mc.ChatType, "group") {
		chatType = channel.ChatTypeGroup
	}
	senderID := mc.From.UserID
	chatID := mc.ChatID
	if chatType == channel.ChatTypeP2P && chatID == "" {
		// Some flavors set ChatID only for groups; fall back to the sender.
		chatID = senderID
	}

	// The command source is the sender's own words — ownCommandSource, not the
	// resolved body, so a 图文混排 whose first run is a screenshot still has
	// its "/issue …" on the first line the parser reads.
	//
	// In a group the @-mention IS how you reach the bot, so it arrives glued to
	// whatever was typed after it — "@Andrew /new" is a person asking for a
	// fresh session, not prose that happens to contain a word — and the
	// addressing comes off the front.
	//
	// Groups only. In a 1:1 nobody has to address the bot, so a leading "@" is
	// the sender naming a colleague they are talking ABOUT: "@李雷 /issue 帮我
	// 问问他" is a question, and stripping the name would turn it into a filed
	// issue nobody asked for plus, via SkipAgentRun below, no answer at all.
	//
	// For a plain text message this is mc.Text.Content, which is what p2p was
	// passing through before CommandText was set here. It is read off
	// ownCommandSource rather than Text.Content because a spoken "/issue …"
	// carries the words in voice.content: sourcing the command from the typed
	// field would leave it empty, and the engine would then fall back to Text,
	// file the issue and — with SkipAgentRun false — run the agent over it as
	// well.
	command := mc.ownCommandSource()
	if chatType == channel.ChatTypeGroup {
		command = stripLeadingMentions(command, botDisplayName)
	}

	// A fresh-session directive behind a quote loses its subject.
	// The shape is ordinary: somebody quotes a colleague's line — "Q3 毛利率
	// 42.1%" — and types "/new 重新分析这个数" under it. The router strips the
	// directive off CommandText and puts what is left into Text, and what is
	// left is the sender's own words WITHOUT the quote block routableText had
	// rendered above them. So a fresh session opened and the agent was asked
	// to re-analyse a number it was never shown, in a session that by
	// construction holds no earlier context to find it in.
	//
	// Recomposing here — quote block, then the stripped body — and declaring
	// ForceFresh tells the router the adapter has already stripped, so it
	// leaves Text alone. Same arrangement Feishu uses for its enriched
	// bodies. CommandText stays unstripped so the shared parser still
	// classifies the command the same way on every platform.
	//
	// A bare "/new" behind a quote is deliberately left alone: the router
	// returns before storing anything, so recomposing would be inert, and
	// claiming ForceFresh for a message that is never written is a lie about
	// state.
	forceFresh := false
	if body, ok := engine.ParseFreshSessionCommand(command); ok && strings.TrimSpace(body) != "" {
		if quoted := mc.Quote.render(); quoted != "" {
			text = renderQuoteBlock(c, quoted) + "\n" + body
			forceFresh = true
		}
	}

	wm := InboundMessage{
		BotID:        botID,
		MsgID:        mc.MsgID,
		MsgType:      mc.MsgType,
		ChatType:     mc.ChatType,
		ChatID:       chatID,
		SenderUserID: senderID,
		Content:      text,
		ReqID:        reqID,
		Media:        mc.attachments(),
	}
	raw, _ := json.Marshal(wm)

	return channel.InboundMessage{
		EventID:        mc.MsgID,
		MessageID:      mc.MsgID,
		Type:           channelMsgType(mc.MsgType),
		Text:           text,
		AddressedToBot: true,
		// The sender's own words, with a group's addressing removed and the
		// media placeholders left out. Command classification is shared
		// (channel/message.go) and falls back to Text when this is empty — and
		// Text starts with the mention in a group, and with "[Image]" whenever
		// a screenshot came first, so on that fallback every such slash command
		// read as ordinary prose. Lark sets this from its command body
		// (feishu_channel.go:139) and Slack from its cleaned text
		// (slack/inbound.go:131); WeCom was the one adapter leaving it empty.
		CommandText: command,
		// Set only when we recomposed Text above; see the comment there.
		ForceFresh: forceFresh,
		// A pure /issue command in WeCom should NOT trigger the
		// agent — the engine already creates the issue and the
		// OutboundReplier already sends "✅ 已创建 #N". Letting the agent
		// see "/issue foo" then produces a "I don't recognize this slash
		// command" reply that just clutters the conversation. wecom is
		// alone on this — Slack/Lark keep the historical "let the agent
		// see /issue and respond too" behaviour.
		//
		// Read off the same source the engine will parse, so a group /issue
		// behaves like the p2p one instead of filing the issue and then also
		// asking the agent about it. It has to be the same source: read off the
		// raw text instead and a p2p "@李雷 /issue …" would file an issue and
		// stay silent, which is the whole reason the strip above is gated; read
		// off the resolved body instead and a screenshot-then-"/issue" message
		// would skip the agent while the parser declined the placeholder line,
		// leaving the sender with neither an issue nor an answer.
		SkipAgentRun: isIssueCommand(command),
		Source: channel.Source{
			ChannelType: TypeWecom,
			ChatID:      chatID,
			ChatType:    chatType,
			SenderID:    senderID,
		},
		Raw: raw,
	}
}

// stripLeadingMentions removes the @-mentions a message opens with, which in a
// group chat is how the sender addresses the bot. WeCom puts them in the text
// and sends no mention list alongside it, so there is nothing to match against
// but the shape: an "@" at the very front, up to the next space.
//
// Group messages only — the caller gates it on chatType. Nobody addresses the
// bot in a 1:1, so the same "@" at the front there is a colleague's name in the
// sender's own sentence, and removing it would rewrite what they said.
//
// Only the front. A name further into the sentence is the sender talking ABOUT
// somebody — "@Andrew ask @李雷 about yesterday" is one instruction naming one
// colleague — and stripping that would quietly rewrite what they said.
//
// This feeds command classification only. The stored message keeps the text
// exactly as it arrived, so the transcript still shows who was addressed.
//
// Slack does the same thing with a regex over its mention token
// (slack/inbound.go cleanText); Feishu is handed an already-clean command body
// by the platform. WeCom was the one adapter passing the raw text through.
func stripLeadingMentions(s, botName string) string {
	for {
		trimmed := strings.TrimLeftFunc(s, unicode.IsSpace)
		if !strings.HasPrefix(trimmed, "@") {
			return trimmed
		}
		// Our own name first, matched whole. A display name may contain
		// spaces — "Multica Bot" is the obvious one — and cutting at the
		// first space would leave "Bot /new 重新分析", which is not a command,
		// so every slash command in that group would still be dropped.
		//
		// The name is not guessed. It comes from the installation config, set
		// when the bot was connected, because the callback carries no
		// structured mention list to read it from. Absent, the heuristic below
		// is what runs — correct for a one-word name, and what every
		// installation has until somebody fills the field in.
		if botName != "" && strings.HasPrefix(trimmed[1:], botName) {
			s = trimmed[1+len(botName):]
			continue
		}
		i := strings.IndexFunc(trimmed, unicode.IsSpace)
		if i < 0 {
			// The whole message is one mention and nothing else. There is no
			// command and no words — leave it, so an empty body is decided by
			// the caller rather than manufactured here.
			return trimmed
		}
		s = trimmed[i:]
	}
}

// isIssueCommand asks the engine's own parser instead of mirroring it. The
// mirror had drifted: it trimmed with strings.TrimSpace, which strips every
// Unicode space including U+3000 — the ideographic space a Chinese IME emits
// in full-width mode — while engine.ParseIssueCommand trims only " \t".
//
// So a p2p line opening with U+3000 read as a command here and as prose there:
// SkipAgentRun was set so no agent ran, and the parser declined so no issue was
// filed. The sender got nothing back and no error anywhere said why. Group
// messages reach this helper after their leading mentions are normalized, and
// must use the same parser too.
//
// A mirror of a parser is a parser. Delegating costs one allocation on a path
// that already does I/O, and removes the whole class.
func isIssueCommand(body string) bool {
	_, ok := engine.ParseIssueCommand(body)
	return ok
}

// channelMsgType maps the raw aibot msg_type onto the normalized enum.
func channelMsgType(wecomType string) channel.MsgType {
	switch strings.ToLower(wecomType) {
	case "text":
		return channel.MsgTypeText
	case "image":
		return channel.MsgTypeImage
	case "file":
		return channel.MsgTypeFile
	case "voice", "audio":
		return channel.MsgTypeAudio
	case "video":
		return channel.MsgTypeVideo
	case "mixed":
		// 图文混排: text runs and attachments interleaved. It maps to Text
		// because the message IS text once ownText has rendered it — runs in
		// composition order, each attachment standing in as its placeholder —
		// and the attachments travel separately as MediaRefs, exactly as they
		// do for Lark's `post`, the same shape under another name:
		// lark/feishu_channel.go:167 maps post → MsgTypeText while
		// lark/media_ingest.go:272 pulls that same post's img/media spans.
		//
		// This line previously read Unknown, and the comment there was right
		// for the code that existed: dispatchFrame routed only msgtype ==
		// "text", so a mixed message never reached normalization and calling
		// it Text would have claimed a routing that did not happen. That
		// claim is what this change makes true. The two must land together —
		// mapping to Text without the routing is the dead, misleading
		// mapping the old comment warned about.
		return channel.MsgTypeText
	default:
		// A kind ownText cannot read never reaches this normalization: it
		// is answered with the unsupported-kind receipt instead. Kept as
		// Unknown rather than mapping "mixed" → Text, which would imply a
		// 图文混排 is routed as plain text when it is not.
		return channel.MsgTypeUnknown
	}
}

// ---- outbound helpers ----

// subscribeBody builds an aibot_subscribe body. The server responds with an
// echoed req_id and errcode 0 on success.
func subscribeBody(botID, secret string) map[string]any {
	return map[string]any{"bot_id": botID, "secret": secret}
}

// sendMsgTextBody builds an aibot_send_msg body carrying plain-text
// content. aibot_send_msg's supported msgtypes are markdown and
// template_card only — text is NOT accepted on this cmd (contrast
// aibot_respond_msg, which does accept text). We therefore ship as
// msgTypeMarkdown is the only aibot msgtype the adapter writes. It is also
// what lands in channel_outbound_queue.msg_type, so the queue records what
// wire form a row was enqueued as rather than assuming one at send time.
const msgTypeMarkdown = "markdown"

// markdown; the WeCom client renders plain text through the markdown
// path without any special escaping. chatType is 1 for single, 2 for
// group.
func sendMsgTextBody(chatID string, chatType int, content string) (map[string]any, error) {
	if chatID == "" {
		return nil, errors.New("wecom: send_msg requires chat_id")
	}
	if chatType != chatTypeSingleInt && chatType != chatTypeGroupInt {
		return nil, errors.New("wecom: send_msg chat_type must be 1 (single) or 2 (group)")
	}
	return map[string]any{
		"chatid":    chatID,
		"chat_type": chatType,
		"msgtype":   msgTypeMarkdown,
		"markdown":  map[string]string{"content": content},
	}, nil
}

// sendMsgContentLimit is the cap on one aibot_send_msg markdown body: the same
// 20480 utf8 bytes the stream frame gets
// (https://developer.work.weixin.qq.com/document/path/101138). A body past it
// is refused WHOLE — the server does not clip it — and the refusal arrives as
// errcode 45002 on the ack, so before splitForWire a long answer simply never
// appeared in the chat.
const sendMsgContentLimit = streamContentLimit

// splitForWire cuts a reply into pieces the platform will accept, and returns
// the input untouched when it already fits — which is nearly always, so the
// common path allocates nothing.
//
// Splitting rather than truncating is the point. A long answer is a code
// review, a pasted log, a document draft: the tail is not filler, and neither
// a reply the server refuses whole nor one that stops at an ellipsis with no
// way to read the rest is an answer. The cut prefers a line boundary, then a
// rune boundary, so a piece never ends mid-character and rarely ends mid-line.
//
// Each piece carries a marker so the reader knows the answer continues. This
// is the one place the adapter adds words to an agent's own text, which is why
// the marker is a bare counter rather than a sentence: it belongs to no
// language, so it needs no translation and cannot contradict an answer written
// in one.
func splitForWire(content string) []string {
	if len(content) <= sendMsgContentLimit {
		return []string{content}
	}

	var pieces []string
	remaining := content
	for len(remaining) > 0 {
		// Reserve room for the widest marker this piece could end up with.
		// The total is not known until the split is done, so the placeholder
		// stands in for it: "…" is three bytes, which covers a total up to
		// three digits — far past any answer that reaches this function.
		marker := fmt.Sprintf("\n\n(%d/…)", len(pieces)+1)
		budget := sendMsgContentLimit - len(marker)
		if len(remaining) <= sendMsgContentLimit {
			pieces = append(pieces, remaining)
			break
		}
		cut := wireCutPoint(remaining, budget)
		pieces = append(pieces, remaining[:cut])
		remaining = strings.TrimLeft(remaining[cut:], "\n")
	}

	// The count is only knowable once the split is done, so the markers go on
	// afterwards. The last piece gets none: there is nothing after it to
	// promise, and the reader can see that for themselves.
	total := len(pieces)
	for i := range pieces {
		if i == total-1 {
			continue
		}
		pieces[i] += fmt.Sprintf("\n\n(%d/%d)", i+1, total)
	}
	return pieces
}

// wireCutPoint picks where to end a piece: the last line break inside the
// budget when there is one worth using, otherwise the last rune boundary.
func wireCutPoint(s string, budget int) int {
	if budget >= len(s) {
		return len(s)
	}
	// A line break in the last quarter of the budget is worth taking; one
	// near the start would waste most of a frame.
	if nl := strings.LastIndexByte(s[:budget], '\n'); nl > budget*3/4 {
		return nl
	}
	cut := budget
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	if cut == 0 {
		// A single rune wider than the budget cannot happen at this size, but
		// returning 0 would loop forever, so fall back to the raw cut.
		return budget
	}
	return cut
}

// aibotChatTypeFromChannel maps the engine's ChatType enum to the int the
// aibot_send_msg body wants.
func aibotChatTypeFromChannel(t channel.ChatType) int {
	if t == channel.ChatTypeGroup {
		return chatTypeGroupInt
	}
	return chatTypeSingleInt
}

// ---- streaming replies ----

// streamContentLimit is aibot's cap on stream.content: 20480 bytes of utf8
// (https://developer.work.weixin.qq.com/document/path/101031). Content is a
// FULL replacement of the bubble's body on every frame, never a delta, so this
// bounds the whole answer rather than one chunk of it.
const streamContentLimit = 20480

// streamThinkingPlaceholder is what the opening frame says. Per 101031 a
// content carrying <think></think> renders as the client's own thinking
// affordance — the animated dots — which is exactly the "working on it" bubble
// we want and costs no copy in any language. Tencent's own OpenClaw plugin
// opens its streams with the same literal.
const streamThinkingPlaceholder = "<think></think>"

// aibot errcodes worth branching on.
//
// A wrong guess about either degrades rather than breaks — an errcode this file
// does not recognise falls through to the plain-message fallback, which is
// where a refused frame ends up anyway.
const (
	// errcodeStreamExpired — the stream ran past its window and the server
	// will not take another frame for it.
	//
	// Measured on 2026-08-09 against the live tenant, not inferred: a stream
	// framed every thirty seconds was refused with exactly this code, and the
	// server's own errmsg named the reason — "stream message update expired
	// (>10 minutes), cannot update". The published global errcode table
	// defines it the same way. The window it implies is streamMaxAge; see
	// stream_store.go for the numbers and what else the probe settled.
	errcodeStreamExpired = 846608

	// errcodeStreamBadReqID — this req_id may not carry a stream.
	//
	// ASSUMPTION, unconfirmed by the vendor and unmeasured: this number is
	// read off Tencent's OpenClaw plugin source, with no version pinned, and
	// it does not appear in WeCom's published error tables the way 846608
	// does. What would settle it: the code appearing in the published tables,
	// a support answer naming it, or a probe that opens a stream on an event
	// callback's req_id and reads back what the server says.
	//
	// An event callback's req_id looks usable and is not; only a message
	// callback's works, so the event path has to use aibot_send_msg.
	errcodeStreamBadReqID = 846605
)

// streamError is a server rejection of a stream frame, carrying the errcode so
// callers can tell "this bubble is beyond saving" from "that frame did not
// land".
type streamError struct {
	Code int
	Msg  string
}

func (e *streamError) Error() string {
	return fmt.Sprintf("wecom: stream frame rejected errcode=%d errmsg=%s", e.Code, e.Msg)
}

// Unusable reports whether the rejection means this stream can never be
// written to again — the caller must fall back to a plain message rather than
// retry.
func (e *streamError) Unusable() bool {
	return e.Code == errcodeStreamExpired || e.Code == errcodeStreamBadReqID
}

// streamUnusable is the package-level predicate over any error, so callers do
// not each re-implement the type assertion.
//
// Only a verdict from the server counts. A write that failed, an ack that
// never came, errNoLiveConnection from a registry holding no socket for the
// installation — none of those is in here, because none of them says anything
// about the stream. A req_id belongs to the turn and not to the connection it
// arrived on (measured 2026-08-09; sendersRegistry.stream), so a missing socket
// is a fact about this moment rather than about the bubble.
func streamUnusable(err error) bool {
	var se *streamError
	if errors.As(err, &se) {
		return se.Unusable()
	}
	return false
}

// respondStreamBody builds an aibot_respond_msg body carrying one frame of a
// streaming reply. finish=false paints or updates the bubble; finish=true
// seals it, after which the message is immutable.
//
// The blank-closing-frame check is the one rule that is not obvious from the
// wire format: WeCom ignores content with nothing visible in it, so a closing
// frame of spaces closes nothing and leaves the user with a bubble that spins
// forever. Refusing here means every caller inherits the check.
func respondStreamBody(streamID, content string, finish bool) (map[string]any, error) {
	if streamID == "" {
		return nil, errors.New("wecom: stream frame requires a stream id")
	}
	if finish {
		content = defuseThinkTags(content)
	}
	content = truncateStreamContent(content)
	if finish && !hasVisibleChar(content) {
		return nil, errors.New("wecom: closing stream frame needs visible content")
	}
	return map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"finish":  finish,
			"content": content,
		},
	}, nil
}

// defuseThinkTags stops an answer that talks about <think> from being read as
// one.
//
// The tag is the client's, not ours: per 101031 a stream body wrapped in
// <think></think> renders as WeCom's own collapsed thinking affordance, which
// is what the opening frame is built from. An answer that happens to contain
// the literal — quoting a prompt, explaining this very feature, pasting XML —
// gets the same treatment, and half the reply disappears into a fold with no
// edit and no unsend to undo it with.
//
// A zero-width space after the angle bracket is enough: the scanner no longer
// matches, and the reader sees the same characters they would have seen. Only
// the tag's own opening is touched, so comparisons, generics and HTML samples
// in the rest of the answer come through as written. Callers apply this to
// closing frames only — the opening frame IS the affordance.
func defuseThinkTags(s string) string {
	if !strings.Contains(s, "<") {
		return s
	}
	const zwsp = "​"
	var b strings.Builder
	last := 0
	// Indexing s directly, never a case-folded copy of it. Walking s while
	// reading strings.ToLower(s) at the same offsets looks equivalent and is
	// not: case folding does not preserve length. U+212A KELVIN SIGN is three
	// bytes and lowercases to a one-byte "k", so the folded copy can be
	// SHORTER than the original and an offset taken from s can be past its
	// end. "KK<x" — two Kelvin signs and an angle bracket — is enough to slice
	// out of range, and the string being scanned is the agent's own answer, so
	// any text a user can talk the agent into echoing would take the backend
	// down with it.
	for i := 0; i < len(s); i++ {
		if s[i] != '<' {
			continue
		}
		j := i + 1
		if j < len(s) && s[j] == '/' {
			j++
		}
		if j+len("think") > len(s) || !strings.EqualFold(s[j:j+len("think")], "think") {
			continue
		}
		b.WriteString(s[last : i+1])
		b.WriteString(zwsp)
		last = i + 1
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// truncateStreamContent cuts content to the protocol's byte limit on a
// character boundary. An answer that arrives clipped still answers; one the
// server rejects for length does not.
func truncateStreamContent(s string) string {
	if len(s) <= streamContentLimit {
		return s
	}
	const ellipsis = "…"
	cut := streamContentLimit - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// hasVisibleChar reports whether s contains a rune that is neither whitespace
// nor a control character. That is the test a closing frame has to pass: one
// the server considers empty is discarded, and the bubble it was meant to seal
// spins for good.
//
// Not the same as "the client will render something", and deliberately not.
// Format runes — U+200B zero width space, U+FEFF, a soft hyphen — are neither
// space nor control, so a body made only of those passes here and still shows
// as nothing. Widening the test would mean carrying a Unicode category table
// for input this adapter does not accept: every ending routes through the
// stream copy constants at the top of typing_indicator.go, all of which are
// ordinary Chinese text.
func hasVisibleChar(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) && !unicode.IsControl(r) {
			return true
		}
	}
	return false
}
