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

// aibot receiver kinds for aibot_send_msg. WeChat uses ints, not strings.
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
	// Mixed carries 图文混排 — a message the user composed with text runs
	// and attachments interleaved. Each item is itself typed, so a mixed
	// message that contains any text run has something we can ingest.
	Mixed struct {
		MsgItem []struct {
			MsgType string `json:"msgtype"`
			Text    struct {
				Content string `json:"content"`
			} `json:"text"`
		} `json:"msg_item"`
	} `json:"mixed"`
	// Image / voice / file / video carry their own media fields; we do not
	// surface them — the adapter declares CapText only.
}

// routableText returns the text this callback can be ingested as, and
// whether there is any. Plain text messages answer with their body; 图文混排
// answers with its text runs joined (the attachments are dropped — the
// adapter declares CapText, so there is nothing to do with them, and losing
// the picture is better than losing the sentence written next to it).
// Everything else — a bare voice note, photo or file — answers false and
// takes the receipt path.
func (mc aibotMsgCallback) routableText() (string, bool) {
	switch strings.ToLower(mc.MsgType) {
	case "text":
		return mc.Text.Content, true
	case "mixed":
		var runs []string
		for _, item := range mc.Mixed.MsgItem {
			if !strings.EqualFold(item.MsgType, "text") {
				continue
			}
			if s := strings.TrimSpace(item.Text.Content); s != "" {
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

	// MsgID is the WeChat per-message identifier used for two-phase dedup.
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

	// Content is the human-readable text body when MsgType == "text";
	// empty for media / events. The cross-platform envelope's Text field
	// is populated from this.
	Content string `json:"content,omitempty"`

	// ReqID is the frame req_id the server sent this message with. We
	// keep it so a future aibot_respond_msg (5s window) can echo it back;
	// iteration 1 uses aibot_send_msg unconditionally and does not need it.
	ReqID string `json:"req_id,omitempty"`
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
// group message on the wire — WeChat only forwards to the bot when it was
// addressed, so any received group message counts as addressed.
//
// text is the ingestible body the caller resolved via routableText — the
// message's own content for a text message, the joined text runs for 图文混排.
func channelMessageFromCallback(botID string, mc aibotMsgCallback, text, reqID string) channel.InboundMessage {
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

	wm := InboundMessage{
		BotID:        botID,
		MsgID:        mc.MsgID,
		MsgType:      mc.MsgType,
		ChatType:     mc.ChatType,
		ChatID:       chatID,
		SenderUserID: senderID,
		Content:      text,
		ReqID:        reqID,
	}
	raw, _ := json.Marshal(wm)

	return channel.InboundMessage{
		EventID:        mc.MsgID,
		MessageID:      mc.MsgID,
		Type:           channelMsgType(mc.MsgType),
		Text:           text,
		AddressedToBot: true,
		// A pure /issue command in WeChat Work should NOT trigger the
		// agent — the engine already creates the issue and the
		// OutboundReplier already sends "✅ 已创建 #N". Letting the agent
		// see "/issue foo" then produces a "I don't recognize this slash
		// command" reply that just clutters the conversation. wecom is
		// alone on this — Slack/Lark keep the historical "let the agent
		// see /issue and respond too" behaviour.
		SkipAgentRun: isIssueCommand(text),
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
// markdown; the WeChat client renders plain text through the markdown
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

// ---- streaming replies ----

// streamContentLimit is aibot's cap on stream.content: 20480 bytes of utf8
// (https://developer.work.weixin.qq.com/document/path/101031). Content is a
// FULL replacement of the bubble's body on every frame, never a delta, so this
// bounds the whole answer rather than one chunk of it.
const streamContentLimit = 20480

// streamThinkingPlaceholder is what the opening frame says. Per 101031 a
// content carrying <think></think> renders as the client's own thinking
// affordance, which is exactly the "working on it" bubble we want and costs no
// copy in any language. Tencent's own OpenClaw plugin opens its streams with
// the same literal.
const streamThinkingPlaceholder = "<think></think>"

// aibot errcodes worth branching on. Neither is in the published docs; both
// come from Tencent's OpenClaw plugin, which handles them in production.
const (
	// errcodeStreamExpired — the stream ran past its window and the server
	// will not take another frame for it. The plugin's comment puts that
	// window at 6 minutes even though the long-connection doc says 10.
	errcodeStreamExpired = 846608

	// errcodeStreamBadReqID — this req_id may not carry a stream. An event
	// callback's req_id looks usable and is not; only a message callback's
	// works, so the event path has to use aibot_send_msg.
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
// <think></think> renders as WeChat's own collapsed thinking affordance, which
// is what the opening and progress frames are built from. An answer that
// happens to contain the literal — quoting a prompt, explaining this very
// feature, pasting XML — gets the same treatment, and half the reply
// disappears into a fold with no edit and no unsend to undo it with.
//
// A zero-width space after the angle bracket is enough: the scanner no longer
// matches, and the reader sees the same characters they would have seen. Only
// the tag's own opening is touched, so comparisons, generics and HTML samples
// in the rest of the answer come through as written. Callers apply this to
// closing frames only — the frames before it are the affordance.
func defuseThinkTags(s string) string {
	if !strings.Contains(s, "<") {
		return s
	}
	const zwsp = "​"
	var b strings.Builder
	lower := strings.ToLower(s)
	last := 0
	for i := 0; i+1 < len(s); i++ {
		if s[i] != '<' {
			continue
		}
		rest := lower[i+1:]
		rest = strings.TrimPrefix(rest, "/")
		if !strings.HasPrefix(rest, "think") {
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

// hasVisibleChar reports whether s contains anything the WeChat client will
// render. Whitespace and control runes do not count — that is the whole point.
func hasVisibleChar(s string) bool {
	for _, r := range s {
		if !unicode.IsSpace(r) && !unicode.IsControl(r) {
			return true
		}
	}
	return false
}
