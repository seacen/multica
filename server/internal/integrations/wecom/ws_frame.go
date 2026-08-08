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
	"strings"

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
	// There is deliberately no Voice field here. A standalone voice message
	// is #6599's subject; this adapter still answers it with the
	// unsupported-kind receipt. mixedItem does carry one, because a voice
	// run inside a 图文混排 would otherwise drop a spoken sentence out of
	// the middle of a message whose other runs are read.
	//
	// Quote is the message this one is a reply to, present when the sender
	// used 引用. It carries the quoted message's own msgtype and body and
	// nothing else — no sender, no id, no timestamp.
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

// render turns the quoted message into the text it contributes. A quoted
// attachment renders as its placeholder and is deliberately NOT queued for
// download: it belongs to a message somebody else sent, the agent would have
// no way to tell it apart from what this sender just attached, and its url is
// running down someone else's five-minute clock. Saying a picture was being
// discussed is the part that carries the meaning.
func (q *quotedMessage) render() string {
	if q == nil {
		return ""
	}
	if strings.EqualFold(q.MsgType, "mixed") {
		var runs []string
		for _, item := range q.Mixed.MsgItem {
			if s := item.render(); s != "" {
				runs = append(runs, s)
			}
		}
		return strings.Join(runs, "\n")
	}
	return q.mixedItem.render()
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

// render turns one 图文混排 run into the line it contributes to the message
// body. An item of a kind this adapter does not know contributes nothing
// rather than a stray placeholder.
func (item mixedItem) render() string {
	switch strings.ToLower(item.MsgType) {
	case "text":
		return strings.TrimSpace(item.Text.Content)
	case "voice":
		// WeCom runs the speech recognition on its side and delivers only
		// the result, so a voice run is a sentence that happened to be
		// spoken — no download, no key.
		return strings.TrimSpace(item.Voice.Content)
	default:
		body, kind, ok := mediaFor(item.MsgType, item.Image, item.File, item.Video)
		if !ok || strings.TrimSpace(body.URL) == "" {
			return ""
		}
		return mediaPlaceholder(kind)
	}
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
	var out []InboundMedia
	add := func(body mediaBody, kind channel.MsgType) {
		if strings.TrimSpace(body.URL) == "" {
			return
		}
		out = append(out, InboundMedia{Kind: kind, URL: body.URL, AESKey: body.AESKey})
	}
	if body, kind, ok := mediaFor(mc.MsgType, mc.Image, mc.File, mc.Video); ok {
		add(body, kind)
		return out
	}
	if !strings.EqualFold(mc.MsgType, "mixed") {
		return nil
	}
	for _, item := range mc.Mixed.MsgItem {
		if body, kind, ok := mediaFor(item.MsgType, item.Image, item.File, item.Video); ok {
			add(body, kind)
		}
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

// ownText is the message minus anything it quotes — the words this person
// typed or attached — and whether there are any.
//
// Plain text answers with its body; a photo, file or video answers with a
// bracketed placeholder, because the bytes arrive later on a detached path
// and the message has to say something in the meantime (the placeholder is
// also what survives if the download never succeeds); 图文混排 answers with
// its runs rendered in the order they were composed, so "look at this" still
// reads above the picture it was written about.
//
// Everything else — a standalone voice note, a location card, a kind WeCom
// adds next year — answers false and takes the receipt path.
func (mc aibotMsgCallback) ownText() (string, bool) {
	switch strings.ToLower(mc.MsgType) {
	case "text":
		return mc.Text.Content, true
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

	// Content is the human-readable body: the user's words, the
	// placeholders standing in for their attachments, and any quoted message
	// rendered above them. The cross-platform envelope's Text field is
	// populated from this.
	Content string `json:"content,omitempty"`

	// ReqID is the frame req_id the server sent this message with. We
	// keep it so a future aibot_respond_msg (5s window) can echo it back;
	// iteration 1 uses aibot_send_msg unconditionally and does not need it.
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
// text is the ingestible body the caller already resolved via routableText.
// It is passed in rather than recomputed because the caller has to know
// whether the message is routable at all before it gets here. c is the
// destination's copy pack, needed again here to recompose that body when a
// directive is stripped off it.
func channelMessageFromCallback(botID string, mc aibotMsgCallback, c copyPack, text, reqID string) channel.InboundMessage {
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

	// The command is read off the user's OWN line. A "/issue" inside somebody
	// else's quoted message is their old text and not an instruction this
	// sender just gave — and the sender's own instruction, sitting under the
	// quote, would be missed entirely by anything reading the front of the
	// stored body.
	command, _ := mc.ownText()

	// `/new` under a quoted message used to throw the quote away.
	//
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
		// The sender's OWN words, without the quote block Text carries ahead
		// of them. Command classification is shared (channel/message.go), and
		// without this the shared parser falls back to Text — whose first
		// non-empty line, when the user replied to somebody, is the rendered
		// quote. Lark sets this from its own command body and Slack from its
		// text; WeCom was the one adapter leaving it empty.
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
		// Read off the sender's own line rather than mc.Text.Content: a
		// 图文混排 whose first run is "/issue 登录坏了" and whose second is a
		// screenshot is one issue-filing message, and the raw text field is
		// empty on that callback. Reading it off the stored body instead
		// would find the quote when there is one — so a /issue under a quote
		// would run the agent as well as file the issue, and a /issue inside
		// somebody else's quoted message would file one nobody asked for.
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

// isIssueCommand mirrors engine.ParseIssueCommand's front-of-body detection
// without materializing the parsed struct — we only need the yes/no. A pure
// /issue command starts at the first non-empty line, "/issue" as a whole
// token, optionally followed by whitespace and the title.
func isIssueCommand(body string) bool {
	for _, raw := range strings.Split(body, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if line == "/issue" {
			return true
		}
		if strings.HasPrefix(line, "/issue ") || strings.HasPrefix(line, "/issue\t") {
			return true
		}
		return false
	}
	return false
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
		"msgtype":   "markdown",
		"markdown":  map[string]string{"content": content},
	}, nil
}

// aibotChatTypeFromChannel maps the engine's ChatType enum to the int the
// aibot_send_msg body wants.
func aibotChatTypeFromChannel(t channel.ChatType) int {
	if t == channel.ChatTypeGroup {
		return chatTypeGroupInt
	}
	return chatTypeSingleInt
}
