package wecom

// capabilities_test.go — Capabilities() is a promise other code acts on.
//
// Nothing branches on it in production yet, which is exactly why it drifts: an
// adapter grows a feature and the declaration does not follow, so the first
// caller that ever reads the mask routes WeCom on facts that stopped being
// true. Each bit below is asserted against the mechanism that makes it true,
// so a bit cannot be added without the feature, and a feature cannot be
// removed while the bit stays.

import (
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// TestCapabilitiesDeclareExactlyWhatTheAdapterDoes pins the whole mask. An
// exact comparison rather than a Has() check, because the failure that matters
// runs in both directions: a missing bit hides a feature from a caller that
// would have used it, and a spare bit makes a caller act on one that is not
// there.
func TestCapabilitiesDeclareExactlyWhatTheAdapterDoes(t *testing.T) {
	t.Parallel()
	want := channel.CapText |
		channel.CapVoice |
		channel.CapAttachment |
		channel.CapTypingIndicator |
		channel.CapMessageEdit
	if got := (&wecomChannel{}).Capabilities(); got != want {
		t.Errorf("Capabilities() = %s, want %s", got, want)
	}
}

// Each declared bit, against the code that makes it true.
func TestEachDeclaredCapabilityHasAMechanism(t *testing.T) {
	t.Parallel()
	caps := (&wecomChannel{}).Capabilities()

	// CapText — a typed message is routed as its body.
	typed := aibotMsgCallback{MsgType: "text"}
	typed.Text.Content = "hello"
	if _, ok := typed.ownText(); !ok {
		t.Fatal("text is not routed; the premise of every other bit is gone")
	}
	if !caps.Has(channel.CapText) {
		t.Error("text is routed but CapText is not declared")
	}

	// CapVoice — WeCom hands over the transcript and the adapter routes it.
	spoken := aibotMsgCallback{MsgType: "voice"}
	spoken.Voice.Content = "把登录跳转的 bug 记一下"
	body, routed := spoken.ownText()
	if !routed || body != "把登录跳转的 bug 记一下" {
		t.Fatalf("voice transcript not routed (body=%q ok=%v)", body, routed)
	}
	if !caps.Has(channel.CapVoice) {
		t.Error("voice notes are read but CapVoice is not declared — a caller deciding where to send " +
			"a voice message would route it away from WeCom, which handles it")
	}

	// CapAttachment — a photo with a url is surfaced rather than refused.
	photo := aibotMsgCallback{MsgType: "image"}
	photo.Image = mediaBody{URL: "https://example.invalid/o", AESKey: "k"}
	if _, ok := photo.ownText(); !ok {
		t.Fatal("an image with a url is not surfaced; media ingest is missing from this tree")
	}
	if !caps.Has(channel.CapAttachment) {
		t.Error("attachments are ingested but CapAttachment is not declared")
	}

	// CapTypingIndicator and CapMessageEdit are the two halves of one
	// mechanism: the opening stream frame is a placeholder the user sees while
	// the run works, and every later frame reuses the SAME stream id to
	// replace that bubble's body.
	if streamThinkingPlaceholder == "" {
		t.Error("no thinking placeholder; there is nothing for CapTypingIndicator to describe")
	}
	if !caps.Has(channel.CapTypingIndicator) {
		t.Error("the bubble opens with a thinking placeholder but CapTypingIndicator is not declared — " +
			"a caller choosing a channel that can show progress would skip WeCom, which can")
	}
	if !caps.Has(channel.CapMessageEdit) {
		t.Error("a stream frame replaces the bubble's body in place but CapMessageEdit is not declared — " +
			"a caller would send a second message where WeCom can rewrite the first")
	}
}

// The bits the adapter must NOT claim. Declaring one of these is worse than
// omitting a real capability: the caller acts on it and the user gets a reply
// the platform cannot render, or none at all.
func TestCapabilitiesDoNotClaimWhatTheAdapterCannotDo(t *testing.T) {
	t.Parallel()
	caps := (&wecomChannel{}).Capabilities()
	for _, tc := range []struct {
		bit channel.Capability
		why string
	}{
		{channel.CapRichCard, "replies are markdown text; the aibot protocol has no interactive card"},
		{channel.CapThreadReply, "the aibot protocol has no threads to reply into"},
		{channel.CapQuoteReply, "an inbound quote is read for context; nothing quote-replies"},
	} {
		if caps.Has(tc.bit) {
			t.Errorf("Capabilities() claims %s — %s; a caller will act on it and the user gets a reply "+
				"WeCom cannot render", tc.bit, tc.why)
		}
	}
}
