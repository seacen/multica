package wecom

// voice_test.go — WeCom runs speech recognition on its side and delivers the
// transcript in voice.content. Answering a voice note with "I only handle
// text" refuses a sentence we were already handed.

import "testing"

func TestVoiceTranscriptIsTreatedAsText(t *testing.T) {
	mc := aibotMsgCallback{MsgType: "voice"}
	mc.Voice.Content = "把登录跳转的 bug 记一下"

	body, ok := mc.ownText()
	if !ok {
		t.Fatal("a voice note with a transcript was refused — the sentence was already in hand")
	}
	if body != "把登录跳转的 bug 记一下" {
		t.Errorf("body = %q, want the transcript", body)
	}

	msg := channelMessageFromCallback("bot-1", "", mc, body, "req-1")
	if msg.Text != "把登录跳转的 bug 记一下" {
		t.Errorf("InboundMessage.Text = %q, want the transcript", msg.Text)
	}
}

// A spoken /issue is still a command.
func TestSpokenIssueCommandIsACommand(t *testing.T) {
	mc := aibotMsgCallback{MsgType: "voice"}
	mc.Voice.Content = "/issue the login redirect is broken"
	text, _ := mc.ownText()
	msg := channelMessageFromCallback("bot-1", "", mc, text, "req-1")
	if !msg.SkipAgentRun {
		t.Error("a spoken /issue did not register as a command")
	}
}

// Recognition returns empty on background noise or a half-second press. An
// empty body would reach the agent as a turn with nothing in it.
func TestEmptyTranscriptIsNotIngested(t *testing.T) {
	for _, content := range []string{"", "   ", "\t\n"} {
		mc := aibotMsgCallback{MsgType: "voice"}
		mc.Voice.Content = content
		if _, ok := mc.ownText(); ok {
			t.Errorf("an empty transcript (%q) was ingested as a turn", content)
		}
	}
}

func TestTextIsUnaffected(t *testing.T) {
	mc := aibotMsgCallback{MsgType: "text"}
	mc.Text.Content = "typed as usual"
	body, ok := mc.ownText()
	if !ok || body != "typed as usual" {
		t.Errorf("ownText() = (%q, %v), want the typed text", body, ok)
	}
}

// Reading a transcript changes nothing about the downloadable kinds: one that
// arrives with no url still has nothing to say and takes the receipt path.
// (What a downloadable kind WITH a url resolves to is media_ingest's subject.)
func TestDownloadableKindsWithNoURLStillTakeTheReceiptPath(t *testing.T) {
	for _, kind := range []string{"image", "file", "video", "mixed"} {
		mc := aibotMsgCallback{MsgType: kind}
		if _, ok := mc.ownText(); ok {
			t.Errorf("%s with no url was routed as text", kind)
		}
	}
}
