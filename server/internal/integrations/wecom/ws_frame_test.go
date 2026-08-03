package wecom

// ws_frame_test.go — the wire contract with WeChat Work. Everything here is
// pinned against frames shaped like the ones documented at
// https://developer.work.weixin.qq.com/document/path/101463 , because the
// only feedback the wire gives us in production is silence: a body with the
// wrong key is accepted by the socket and then does nothing.

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// ---- inbound: aibot_msg_callback ----

// singleTextFrame is a 1:1 text message as WeChat pushes it. chatid is absent
// on the single flavour — the sender's userid is the conversation.
const singleTextFrame = `{
  "cmd": "aibot_msg_callback",
  "headers": {"req_id": "req-8f2c"},
  "body": {
    "msgid": "MSGID-001",
    "aibotid": "wb1234567890abcdef",
    "chattype": "single",
    "from": {"userid": "T-alex"},
    "msgtype": "text",
    "text": {"content": "帮我约个会"}
  }
}`

// groupTextFrame is the group flavour: chatid identifies the room, from.userid
// the person who typed.
const groupTextFrame = `{
  "cmd": "aibot_msg_callback",
  "headers": {"req_id": "req-1a55"},
  "body": {
    "msgid": "MSGID-002",
    "aibotid": "wb1234567890abcdef",
    "chatid": "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
    "chattype": "group",
    "from": {"userid": "T-dana"},
    "msgtype": "text",
    "text": {"content": "@bot 状态如何"}
  }
}`

func decodeCallback(t *testing.T, raw string) (frameEnvelope, aibotMsgCallback) {
	t.Helper()
	var env frameEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	var mc aibotMsgCallback
	if err := json.Unmarshal(env.Body, &mc); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return env, mc
}

// TestDecodeSingleTextCallback pins every field the adapter reads off a 1:1
// text frame, including req_id, which is the correlation id an
// aibot_respond_msg would have to echo.
func TestDecodeSingleTextCallback(t *testing.T) {
	env, mc := decodeCallback(t, singleTextFrame)

	if env.Cmd != cmdMsgCallback {
		t.Errorf("cmd = %q, want %q", env.Cmd, cmdMsgCallback)
	}
	if env.Headers.ReqID != "req-8f2c" {
		t.Errorf("req_id = %q", env.Headers.ReqID)
	}
	if mc.MsgID != "MSGID-001" {
		t.Errorf("msgid = %q — this is the dedup key, it must survive decoding", mc.MsgID)
	}
	if mc.AIBotID != "wb1234567890abcdef" {
		t.Errorf("aibotid = %q", mc.AIBotID)
	}
	if mc.ChatType != "single" {
		t.Errorf("chattype = %q", mc.ChatType)
	}
	if mc.ChatID != "" {
		t.Errorf("chatid = %q, want empty on the single flavour", mc.ChatID)
	}
	if mc.From.UserID != "T-alex" {
		t.Errorf("from.userid = %q", mc.From.UserID)
	}
	if mc.MsgType != "text" {
		t.Errorf("msgtype = %q", mc.MsgType)
	}
	if mc.Text.Content != "帮我约个会" {
		t.Errorf("text.content = %q", mc.Text.Content)
	}
}

// TestDecodeGroupTextCallback pins the group flavour, where chatid and
// from.userid are two different identities and confusing them addresses the
// reply to the wrong place.
func TestDecodeGroupTextCallback(t *testing.T) {
	_, mc := decodeCallback(t, groupTextFrame)

	if mc.ChatType != "group" {
		t.Errorf("chattype = %q", mc.ChatType)
	}
	if mc.ChatID != "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO" {
		t.Errorf("chatid = %q", mc.ChatID)
	}
	if mc.From.UserID != "T-dana" {
		t.Errorf("from.userid = %q, want the typist, not the room", mc.From.UserID)
	}
}

// TestDecodeMixedCallback pins 图文混排: each item carries its own msgtype and
// body, so the text runs have to be picked out of a heterogeneous list.
func TestDecodeMixedCallback(t *testing.T) {
	const raw = `{
      "cmd": "aibot_msg_callback",
      "headers": {"req_id": "req-mix"},
      "body": {
        "msgid": "MSGID-003",
        "chattype": "single",
        "from": {"userid": "T-alex"},
        "msgtype": "mixed",
        "mixed": {"msg_item": [
          {"msgtype": "text", "text": {"content": "看这张"}},
          {"msgtype": "image", "image": {"url": "https://example.invalid/a.png"}},
          {"msgtype": "text", "text": {"content": "行不行"}}
        ]}
      }
    }`
	_, mc := decodeCallback(t, raw)

	if n := len(mc.Mixed.MsgItem); n != 3 {
		t.Fatalf("msg_item length = %d, want 3", n)
	}
	if mc.Mixed.MsgItem[1].MsgType != "image" {
		t.Errorf("item 1 msgtype = %q", mc.Mixed.MsgItem[1].MsgType)
	}
	text, ok := mc.routableText()
	if !ok {
		t.Fatal("a mixed message with two text runs has something to ingest")
	}
	if text != "看这张\n行不行" {
		t.Errorf("routableText = %q, want both runs joined in order", text)
	}
}

// TestRoutableText is the whole "what can this adapter ingest" decision in one
// table. CapText is all we declare, so anything without words takes the
// receipt path instead of the engine.
func TestRoutableText(t *testing.T) {
	mixed := func(items ...[2]string) aibotMsgCallback {
		mc := aibotMsgCallback{MsgType: "mixed"}
		for _, it := range items {
			var item struct {
				MsgType string `json:"msgtype"`
				Text    struct {
					Content string `json:"content"`
				} `json:"text"`
			}
			item.MsgType = it[0]
			item.Text.Content = it[1]
			mc.Mixed.MsgItem = append(mc.Mixed.MsgItem, item)
		}
		return mc
	}
	text := func(s string) aibotMsgCallback {
		mc := aibotMsgCallback{MsgType: "text"}
		mc.Text.Content = s
		return mc
	}

	cases := []struct {
		name     string
		mc       aibotMsgCallback
		wantText string
		wantOK   bool
	}{
		{"plain text", text("你好"), "你好", true},
		{"text with an odd casing on the wire", func() aibotMsgCallback {
			mc := text("你好")
			mc.MsgType = "TEXT"
			return mc
		}(), "你好", true},
		{"voice note", aibotMsgCallback{MsgType: "voice"}, "", false},
		{"photo", aibotMsgCallback{MsgType: "image"}, "", false},
		{"file drop", aibotMsgCallback{MsgType: "file"}, "", false},
		{"video", aibotMsgCallback{MsgType: "video"}, "", false},
		{"unknown future type", aibotMsgCallback{MsgType: "sphere_of_influence"}, "", false},
		{"mixed with one run", mixed([2]string{"text", "看这张"}, [2]string{"image", ""}), "看这张", true},
		{"mixed with only pictures", mixed([2]string{"image", ""}), "", false},
		{"mixed whose run is blank", mixed([2]string{"text", "   "}, [2]string{"image", ""}), "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := c.mc.routableText()
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if got != c.wantText {
				t.Errorf("text = %q, want %q", got, c.wantText)
			}
		})
	}
}

// TestChannelMsgTypeMapping keeps the raw wecom type mapped onto the
// cross-platform enum the audit trail records.
func TestChannelMsgTypeMapping(t *testing.T) {
	cases := map[string]channel.MsgType{
		"text":   channel.MsgTypeText,
		"mixed":  channel.MsgTypeText,
		"image":  channel.MsgTypeImage,
		"file":   channel.MsgTypeFile,
		"voice":  channel.MsgTypeAudio,
		"audio":  channel.MsgTypeAudio,
		"video":  channel.MsgTypeVideo,
		"VOICE":  channel.MsgTypeAudio,
		"stream": channel.MsgTypeUnknown,
		"":       channel.MsgTypeUnknown,
	}
	for raw, want := range cases {
		if got := channelMsgType(raw); got != want {
			t.Errorf("channelMsgType(%q) = %q, want %q", raw, got, want)
		}
	}
}

// ---- normalization into the engine's envelope ----

// TestChannelMessageFromCallbackRoutingIdentity is the single-vs-group
// addressing contract. Getting ChatID wrong sends a private answer into a
// group room, or a group answer into a DM.
func TestChannelMessageFromCallbackRoutingIdentity(t *testing.T) {
	cases := []struct {
		name         string
		raw          string
		wantChatType channel.ChatType
		wantChatID   string
		wantSender   string
	}{
		{
			name:         "single: the userid is the conversation",
			raw:          singleTextFrame,
			wantChatType: channel.ChatTypeP2P,
			wantChatID:   "T-alex",
			wantSender:   "T-alex",
		},
		{
			name:         "group: the room is the conversation, the typist is the sender",
			raw:          groupTextFrame,
			wantChatType: channel.ChatTypeGroup,
			wantChatID:   "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
			wantSender:   "T-dana",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env, mc := decodeCallback(t, c.raw)
			text, ok := mc.routableText()
			if !ok {
				t.Fatal("fixture is a text message")
			}
			msg := channelMessageFromCallback("wb1234567890abcdef", mc, text, env.Headers.ReqID)

			if msg.Source.ChannelType != TypeWecom {
				t.Errorf("ChannelType = %q", msg.Source.ChannelType)
			}
			if msg.Source.ChatType != c.wantChatType {
				t.Errorf("ChatType = %q, want %q", msg.Source.ChatType, c.wantChatType)
			}
			if msg.Source.ChatID != c.wantChatID {
				t.Errorf("ChatID = %q, want %q", msg.Source.ChatID, c.wantChatID)
			}
			if msg.Source.SenderID != c.wantSender {
				t.Errorf("SenderID = %q, want %q", msg.Source.SenderID, c.wantSender)
			}
			if !msg.AddressedToBot {
				t.Error("WeChat only forwards what was addressed to the bot, so every frame counts as addressed")
			}
			if msg.MessageID != mc.MsgID || msg.EventID != mc.MsgID {
				t.Errorf("MessageID/EventID = %q/%q, want the msgid %q (the dedup key)", msg.MessageID, msg.EventID, mc.MsgID)
			}
			if msg.Text != mc.Text.Content {
				t.Errorf("Text = %q", msg.Text)
			}
			if msg.Type != channel.MsgTypeText {
				t.Errorf("Type = %q", msg.Type)
			}
		})
	}
}

// TestChannelMessageRawCarriesThePlatformFields: the resolvers read BotID and
// the sender off Raw, so a rename there breaks routing silently.
func TestChannelMessageRawCarriesThePlatformFields(t *testing.T) {
	env, mc := decodeCallback(t, groupTextFrame)
	text, _ := mc.routableText()
	msg := channelMessageFromCallback("wb1234567890abcdef", mc, text, env.Headers.ReqID)

	wm, err := wecomMsgFromRaw(msg)
	if err != nil {
		t.Fatalf("Raw does not decode back into the wecom envelope: %v", err)
	}
	if wm.BotID != "wb1234567890abcdef" {
		t.Errorf("Raw.bot_id = %q — the installation resolver routes on this", wm.BotID)
	}
	if wm.MsgID != "MSGID-002" {
		t.Errorf("Raw.msg_id = %q", wm.MsgID)
	}
	if wm.ChatType != "group" || wm.ChatID != mc.ChatID {
		t.Errorf("Raw chat identity = %q/%q", wm.ChatType, wm.ChatID)
	}
	if wm.SenderUserID != "T-dana" {
		t.Errorf("Raw.sender_user_id = %q", wm.SenderUserID)
	}
	if wm.MsgType != "text" || wm.Content != text {
		t.Errorf("Raw body = %q/%q", wm.MsgType, wm.Content)
	}
	if wm.ReqID != "req-1a55" {
		t.Errorf("Raw.req_id = %q — aibot_respond_msg would have to echo it", wm.ReqID)
	}
}

// TestIssueCommandSkipsTheAgentRun: a bare /issue is answered by the engine
// itself, so letting the agent see it too produces a second, useless reply.
func TestIssueCommandSkipsTheAgentRun(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{"/issue 登录页面报错", true},
		{"/issue", true},
		{"/issue\t带个 tab", true},
		{"\n\n/issue 前面有空行", true},
		{"  /issue 前面有空格", true},
		{"/issues 复数不是命令", false},
		{"请帮我 /issue 一下", false},
		{"先说一句\n/issue 第二行才写命令", false},
		{"", false},
	}
	for _, c := range cases {
		mc := aibotMsgCallback{MsgID: "m", MsgType: "text", ChatType: "single"}
		mc.From.UserID = "T-alex"
		mc.Text.Content = c.body
		msg := channelMessageFromCallback("bot", mc, c.body, "req")
		if msg.SkipAgentRun != c.want {
			t.Errorf("SkipAgentRun for %q = %v, want %v", c.body, msg.SkipAgentRun, c.want)
		}
	}
}

// ---- inbound: aibot_event_callback ----

// TestDecodeDisconnectedEvent — the frame that says another connection took
// our place. The read loop keys the reconnect handoff off this exact string.
func TestDecodeDisconnectedEvent(t *testing.T) {
	const raw = `{"cmd":"aibot_event_callback","headers":{"req_id":"r"},
	  "body":{"event":{"eventtype":"disconnected_event"}}}`
	var env frameEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Cmd != cmdEventCallback {
		t.Fatalf("cmd = %q", env.Cmd)
	}
	var ec aibotEventCallback
	if err := json.Unmarshal(env.Body, &ec); err != nil {
		t.Fatalf("decode event body: %v", err)
	}
	if ec.Event.EventType != eventDisconnected {
		t.Errorf("eventtype = %q, want %q", ec.Event.EventType, eventDisconnected)
	}
}

// TestDecodeServerAck — the anonymous response frame. errcode/errmsg sit on
// the envelope, not in body, and req_id is what pairs it with our write.
func TestDecodeServerAck(t *testing.T) {
	const ok = `{"headers":{"req_id":"req-8f2c"},"errcode":0,"errmsg":"ok"}`
	const bad = `{"headers":{"req_id":"req-8f2c"},"errcode":40001,"errmsg":"invalid credential"}`

	var good frameEnvelope
	if err := json.Unmarshal([]byte(ok), &good); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if good.Cmd != "" || good.ErrCode != 0 || good.Headers.ReqID != "req-8f2c" {
		t.Errorf("ack = %+v", good)
	}

	var rejected frameEnvelope
	if err := json.Unmarshal([]byte(bad), &rejected); err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if rejected.ErrCode != 40001 || rejected.ErrMsg != "invalid credential" {
		t.Errorf("reject = %+v", rejected)
	}
}

// ---- outbound frames ----

// TestSubscribeBodyIsTheDocumentedShape — the auth frame. Wrong key names are
// an errcode we would only see in a log.
func TestSubscribeBodyIsTheDocumentedShape(t *testing.T) {
	body := subscribeBody("wb1234567890abcdef", "s3cret")
	raw, err := json.Marshal(map[string]any{
		"cmd":     cmdSubscribe,
		"headers": frameHeaders{ReqID: "req-1"},
		"body":    body,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["cmd"] != "aibot_subscribe" {
		t.Errorf("cmd = %v", got["cmd"])
	}
	hdr, _ := got["headers"].(map[string]any)
	if hdr["req_id"] != "req-1" {
		t.Errorf("headers = %v", got["headers"])
	}
	b, _ := got["body"].(map[string]any)
	if b["bot_id"] != "wb1234567890abcdef" || b["secret"] != "s3cret" {
		t.Errorf("body = %v", b)
	}
}

// TestSendMsgTextBodyIsTheDocumentedShape — aibot_send_msg accepts markdown
// and template_card only; "text" is silently useless on this cmd.
func TestSendMsgTextBodyIsTheDocumentedShape(t *testing.T) {
	cases := []struct {
		name     string
		chatID   string
		chatType int
	}{
		{"single", "T-alex", chatTypeSingleInt},
		{"group", "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO", chatTypeGroupInt},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body, err := sendMsgTextBody(c.chatID, c.chatType, "答案是 42")
			if err != nil {
				t.Fatalf("sendMsgTextBody: %v", err)
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got["chatid"] != c.chatID {
				t.Errorf("chatid = %v", got["chatid"])
			}
			if got["chat_type"] != float64(c.chatType) {
				t.Errorf("chat_type = %v, want the int %d", got["chat_type"], c.chatType)
			}
			if got["msgtype"] != "markdown" {
				t.Errorf("msgtype = %v, want markdown (aibot_send_msg rejects text)", got["msgtype"])
			}
			md, _ := got["markdown"].(map[string]any)
			if md["content"] != "答案是 42" {
				t.Errorf("markdown.content = %v", md["content"])
			}
			if _, unexpected := got["text"]; unexpected {
				t.Error("a text key on aibot_send_msg is dead weight the server ignores")
			}
		})
	}
}

// TestSendMsgTextBodyRejectsWhatTheWireWillNot — a body the platform will
// never accept must fail here, not sit in the outbound queue retrying.
func TestSendMsgTextBodyRejectsWhatTheWireWillNot(t *testing.T) {
	if _, err := sendMsgTextBody("", chatTypeSingleInt, "x"); err == nil {
		t.Error("an empty chat id must be rejected")
	}
	for _, bad := range []int{0, 3, -1} {
		if _, err := sendMsgTextBody("T-alex", bad, "x"); err == nil {
			t.Errorf("chat_type %d must be rejected", bad)
		}
	}
}

// TestAibotChatTypeFromChannel — the enum-to-int hop. p2p and anything
// unrecognized fall to single, which is the safe direction: a single send to a
// group id fails loudly, a group send to a userid could reach a room.
func TestAibotChatTypeFromChannel(t *testing.T) {
	if got := aibotChatTypeFromChannel(channel.ChatTypeP2P); got != chatTypeSingleInt {
		t.Errorf("p2p → %d, want %d", got, chatTypeSingleInt)
	}
	if got := aibotChatTypeFromChannel(channel.ChatTypeGroup); got != chatTypeGroupInt {
		t.Errorf("group → %d, want %d", got, chatTypeGroupInt)
	}
	if got := aibotChatTypeFromChannel(channel.ChatType("")); got != chatTypeSingleInt {
		t.Errorf("unset → %d, want single", got)
	}
}

// TestSendTextWritesOneWellFormedFrame walks the sender end to end: what
// lands on the socket for one reply.
func TestSendTextWritesOneWellFormedFrame(t *testing.T) {
	conn := &recordingConn{}
	s := newWSSender(conn, testLogger())

	if err := s.sendText("T-alex", chatTypeSingleInt, "答案是 42"); err != nil {
		t.Fatalf("sendText: %v", err)
	}
	frames := conn.sends()
	if len(frames) != 1 {
		t.Fatalf("want one frame, got %d", len(frames))
	}
	f := frames[0]
	if f["cmd"] != cmdSendMsg {
		t.Errorf("cmd = %v", f["cmd"])
	}
	hdr, _ := f["headers"].(map[string]any)
	reqID, _ := hdr["req_id"].(string)
	if strings.TrimSpace(reqID) == "" {
		t.Error("every frame needs a req_id to pair with its ack")
	}
	if got := contentsOf(conn); len(got) != 1 || got[0] != "答案是 42" {
		t.Errorf("content = %v", got)
	}
}

// TestReqIDsAreDistinct — two frames sharing a req_id would pair with each
// other's acks.
func TestReqIDsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := newReqID()
		if id == "" {
			t.Fatal("empty req_id")
		}
		if seen[id] {
			t.Fatalf("req_id %q repeated within 100 draws", id)
		}
		seen[id] = true
	}
}

// TestDefuseThinkTagsCoversTheShapesTheClientMatches — the client's scanner is
// what decides, so every spelling it would fold has to be covered, and nothing
// else may be.
func TestDefuseThinkTagsCoversTheShapesTheClientMatches(t *testing.T) {
	defused := []string{
		"<think>",
		"</think>",
		"<THINK>",
		"<Think>x</Think>",
		"<thinking>",
		"答案里带了 <think> 这个词",
	}
	for _, in := range defused {
		got := defuseThinkTags(in)
		if strings.Contains(strings.ToLower(got), "<think") || strings.Contains(strings.ToLower(got), "</think") {
			t.Errorf("defuseThinkTags(%q) = %q, still matches the client's scanner", in, got)
		}
		if !strings.Contains(strings.ToLower(got), "think") {
			t.Errorf("defuseThinkTags(%q) = %q, lost the word", in, got)
		}
	}

	untouched := []string{
		"",
		"a < b && c > d",
		"Vec<Thing>",
		"<div>hi</div>",
		"1<2",
		"trailing <",
	}
	for _, in := range untouched {
		if got := defuseThinkTags(in); got != in {
			t.Errorf("defuseThinkTags(%q) = %q, want it untouched", in, got)
		}
	}
}
