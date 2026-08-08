package wecom

// voice_test.go — WeCom runs speech recognition on its side and delivers the
// transcript in voice.content. Answering a voice note with "I only handle
// text" refuses a sentence we were already handed.
//
// The body resolver is ownText: it began as bodyText (text + voice) and became
// ownText once the downloadable kinds started being surfaced too, so voice is
// asserted here against the resolver that now answers for every kind.

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

	msg := channelMessageFromCallback("bot-1", mc, body, "req-1")
	if msg.Text != "把登录跳转的 bug 记一下" {
		t.Errorf("InboundMessage.Text = %q, want the transcript", msg.Text)
	}
}

// A spoken /issue is still a command.
func TestSpokenIssueCommandIsACommand(t *testing.T) {
	mc := aibotMsgCallback{MsgType: "voice"}
	mc.Voice.Content = "/issue the login redirect is broken"
	body, _ := mc.ownText()
	msg := channelMessageFromCallback("bot-1", mc, body, "req-1")
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

// The downloadable kinds are surfaced now that media ingest is in the tree, so
// what separates ingested from refused is whether the callback carries a url to
// download — not the kind. A kind with no url still takes the receipt path,
// which is the case an empty transcript shares.
func TestDownloadableKindsNeedAURL(t *testing.T) {
	for _, kind := range []string{"image", "file", "video"} {
		bare := aibotMsgCallback{MsgType: kind}
		if _, ok := bare.ownText(); ok {
			t.Errorf("%s with no url was routed as a turn; it has nothing to say and nothing to fetch", kind)
		}

		withURL := aibotMsgCallback{MsgType: kind}
		body := mediaBody{URL: "https://example.invalid/o", AESKey: "k"}
		switch kind {
		case "image":
			withURL.Image = body
		case "file":
			withURL.File = body
		case "video":
			withURL.Video = body
		}
		if _, ok := withURL.ownText(); !ok {
			t.Errorf("%s with a url was refused; media ingest is in this tree and it should be surfaced", kind)
		}
	}
}
