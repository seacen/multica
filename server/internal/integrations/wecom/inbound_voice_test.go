package wecom

// inbound_voice_test.go — a voice note in WeChat Work arrives as text.
// WeCom runs the speech recognition itself and hands the bot
// {"voice":{"content":"..."}}; there is no audio to fetch and no key to
// decrypt. Answering "please send it as text" to a message that already IS
// text is the bug these tests pin.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

func voiceFrame(msgID, transcript string) frameEnvelope {
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "bot",
		"chattype": "single",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "voice",
		"voice":    map[string]any{"content": transcript},
	})
	return frameEnvelope{Cmd: cmdMsgCallback, Body: body}
}

// TestVoiceMessageRoutesItsTranscript: the transcript is the message. It goes
// to the engine like any other sentence the user typed.
func TestVoiceMessageRoutesItsTranscript(t *testing.T) {
	var got channel.InboundMessage
	c, conn, _ := testChannel(t, func(_ context.Context, m channel.InboundMessage) error {
		got = m
		return nil
	})
	sender := newWSSender(conn, nil)

	if err := c.dispatchFrame(context.Background(), voiceFrame("msg-voice", "帮我查一下今天的日程"), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if got.Text != "帮我查一下今天的日程" {
		t.Fatalf("engine received Text=%q, want the voice transcript", got.Text)
	}
	if got.MessageID != "msg-voice" {
		t.Fatalf("MessageID = %q, want msg-voice", got.MessageID)
	}
	if got.Type != channel.MsgTypeAudio {
		t.Fatalf("Type = %v, want MsgTypeAudio — the user spoke it, even though the body is text", got.Type)
	}
	if n := len(conn.sends()); n != 0 {
		t.Fatalf("a voice note must not draw a 'text only, please' receipt, got %d", n)
	}
}

// TestVoiceMessageWithBlankTranscriptGetsAReceipt: recognition can come back
// empty (background noise, a half-second press). There is nothing to ingest,
// so the user still gets told rather than met with silence.
func TestVoiceMessageWithBlankTranscriptGetsAReceipt(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error {
		t.Fatal("an empty transcript must not reach the engine handler")
		return nil
	})
	sender := newWSSender(conn, nil)

	if err := c.dispatchFrame(context.Background(), voiceFrame("msg-voice-blank", "   "), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if n := len(conn.sends()); n != 1 {
		t.Fatalf("want one receipt for a blank transcript, got %d", n)
	}
}

// TestVoiceRoutableTextTrimsNothingItShouldKeep: the transcript is the user's
// sentence — internal whitespace and punctuation survive verbatim.
func TestVoiceRoutableTextIsTheTranscriptVerbatim(t *testing.T) {
	mc := aibotMsgCallback{MsgType: "voice"}
	mc.Voice.Content = "  明天 10 点，和 Alex 对一下 Q3 的数  "
	got, ok := mc.routableText()
	if !ok {
		t.Fatal("a voice callback with a transcript must be routable")
	}
	if got != "明天 10 点，和 Alex 对一下 Q3 的数" {
		t.Fatalf("routableText = %q", got)
	}
}
