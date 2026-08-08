package wecom

// inbound_quote_test.go — "引用某条消息再提问" is how people ask about
// something specific in a busy chat: they long-press the message, hit 引用,
// and type their question. WeCom puts the quoted message on the callback in a
// `quote` field; the adapter ignored it entirely, so the agent got the
// question with the subject removed — "这个数对吗" about nothing — and a reply
// of "对，就这么办" had nothing to attach to.

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// quotingFrame is a text message that quotes something.
func quotingFrame(msgID, own string, quote map[string]any) frameEnvelope {
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "bot",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": own},
		"quote":    quote,
	})
	return frameEnvelope{Cmd: cmdMsgCallback, Body: body}
}

// TestQuotedTextReachesTheAgent: the quoted message goes in above the
// question, marked as a quote so the agent can tell whose words are whose.
func TestQuotedTextReachesTheAgent(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q1", "这个数对吗", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))

	c := copyFor(DefaultLocale)
	want := "> " + c.QuotePrefix + "Q3 毛利率 42.1%\n这个数对吗"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q — without the quoted line the agent is asked whether a number it was never shown is correct", got.Text, want)
	}
}

// TestQuotedMultiLineTextStaysOneBlock: every line of the quote is marked, or
// the second paragraph reads as the user's own words.
func TestQuotedMultiLineTextStaysOneBlock(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q2", "第二条还没做", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "1. 对齐口径\n2. 回填数据"},
	}))

	c := copyFor(DefaultLocale)
	want := "> " + c.QuotePrefix + "1. 对齐口径\n> 2. 回填数据\n第二条还没做"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q", got.Text, want)
	}
	for _, line := range strings.Split(got.Text, "\n")[:2] {
		if !strings.HasPrefix(line, "> ") {
			t.Fatalf("quoted line %q is not marked as quoted, so it reads as this sender's own words", line)
		}
	}
}

// TestAQuotedImageArrivesWithItsBytes is the reference an agent could not
// resolve.
//
// A quote of a picture renders as one line, `> Quoted: [Image]`, and the
// callback carries nothing else about it: no sender, no message id, no
// timestamp — the documented `quote` object is msgtype plus the body, and
// that is all. So an agent handed that line cannot tell a screenshot it read
// a minute ago (which it has, with an attachment id) from one it has never
// seen, and "左下角那块看不清" is a question about a picture it does not have.
//
// The url and key on the quote are the only thing that settles it, and they
// are handed over in this same callback. Fetching them turns the quote into an
// attachment, which is a thing the agent can actually look at.
func TestAQuotedImageArrivesWithItsBytes(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q3", "左下角那块看不清", map[string]any{
		"msgtype": "image",
		"image":   map[string]any{"url": "https://cos.invalid/quoted.enc", "aeskey": testAESKey},
	}))

	c := copyFor(DefaultLocale)
	want := "> " + c.QuotePrefix + "[Image]\n左下角那块看不清"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q", got.Text, want)
	}
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 1 {
		t.Fatalf("the quoted image produced %d attachments, want 1 — the agent is left with a bare %q "+
			"and no way to tell whether it is a picture it has already read or one it has never seen: %+v",
			len(wm.Media), "[Image]", wm.Media)
	}
	if wm.Media[0].Kind != channel.MsgTypeImage {
		t.Errorf("kind = %v, want image", wm.Media[0].Kind)
	}
	if wm.Media[0].URL != "https://cos.invalid/quoted.enc" {
		t.Errorf("url = %q, want the quote's own", wm.Media[0].URL)
	}
	if wm.Media[0].AESKey != testAESKey {
		t.Error("the quoted image's key did not travel with it, so the bytes cannot be decrypted")
	}
}

// TestAQuotedPictureComesBeforeTheSendersOwn: the quote block is rendered
// above the sender's words, so the quoted picture's placeholder is the first
// one in the body — and the attachments have to be in that same order, or an
// agent asked to compare two pictures compares them the wrong way round.
func TestAQuotedPictureComesBeforeTheSendersOwn(t *testing.T) {
	t.Parallel()
	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-q3b",
		"aibotid":  "bot",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "mixed",
		"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "这版好一些吗"}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/mine", "aeskey": testAESKey}},
		}},
		"quote": map[string]any{
			"msgtype": "image",
			"image":   map[string]any{"url": "https://cos.invalid/theirs", "aeskey": testAESKey},
		},
	})
	got, _, _ := dispatchOne(t, frameEnvelope{Cmd: cmdMsgCallback, Body: body})

	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 2 {
		t.Fatalf("attachments = %+v, want the quoted picture and the sender's own", wm.Media)
	}
	if wm.Media[0].URL != "https://cos.invalid/theirs" || wm.Media[1].URL != "https://cos.invalid/mine" {
		t.Fatalf("attachment order = [%q %q], want the quoted one first: its placeholder is the first "+
			"in the body, and the agent reads the two lists against each other",
			wm.Media[0].URL, wm.Media[1].URL)
	}
}

// TestAQuoted图文混排ContributesEveryPictureInIt: a quoted 图文混排 renders one
// placeholder per run, so it has to produce one attachment per run too — a
// missing one shifts every placeholder after it onto the wrong picture.
func TestAQuotedMixedContributesEveryPictureInIt(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q3c", "第二张是哪天的", map[string]any{
		"msgtype": "mixed",
		"mixed": map[string]any{"msg_item": []any{
			map[string]any{"msgtype": "text", "text": map[string]any{"content": "两版对比"}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/a", "aeskey": testAESKey}},
			map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/b", "aeskey": testAESKey}},
		}},
	}))

	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 2 {
		t.Fatalf("attachments = %+v, want both pictures in the quoted 图文混排", wm.Media)
	}
	if strings.Count(got.Text, "[Image]") != 2 {
		t.Fatalf("Text = %q, want a placeholder per picture — the count has to match the attachments", got.Text)
	}
}

// TestAQuotedAttachmentWithNoURLIsNotQueued: a body with nothing to fetch must
// not produce an attachment, because it does not produce a placeholder either.
// An intent-ledger row for an object that can never exist is the cost of
// getting this wrong; a body whose placeholder and attachment disagree is the
// bigger one.
func TestAQuotedAttachmentWithNoURLIsNotQueued(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q3d", "这个", map[string]any{
		"msgtype": "image",
		"image":   map[string]any{"aeskey": testAESKey},
	}))

	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 0 {
		t.Fatalf("attachments = %+v, want none — there is no url to fetch", wm.Media)
	}
	if strings.Contains(got.Text, "[Image]") {
		t.Fatalf("Text = %q, want no placeholder for a picture that was never there", got.Text)
	}
}

// TestQuotedKindsEachRenderTheirOwnWay: a quote carries whatever kind the
// original was, and each one renders the way it would as a message of its own.
func TestQuotedKindsEachRenderTheirOwnWay(t *testing.T) {
	t.Parallel()
	c := copyFor(DefaultLocale)
	cases := []struct {
		name  string
		quote map[string]any
		want  string
	}{
		{"a quoted 图文混排 keeps its order", map[string]any{
			"msgtype": "mixed",
			"mixed": map[string]any{"msg_item": []any{
				map[string]any{"msgtype": "text", "text": map[string]any{"content": "两版对比"}},
				map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/a"}},
			}},
		}, "两版对比\n[Image]"},
		{"a quoted file", map[string]any{
			"msgtype": "file",
			"file":    map[string]any{"url": "https://cos.invalid/b"},
		}, "[File]"},
		{"a quoted voice run is its transcript", map[string]any{
			"msgtype": "mixed",
			"mixed": map[string]any{"msg_item": []any{
				map[string]any{"msgtype": "voice", "voice": map[string]any{"content": "周五之前给我"}},
			}},
		}, "周五之前给我"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _, _ := dispatchOne(t, quotingFrame("msg-q-"+tc.name, "问题在这", tc.quote))
			var b strings.Builder
			for i, line := range strings.Split(tc.want, "\n") {
				if i > 0 {
					b.WriteString("\n> ")
				} else {
					b.WriteString("> " + c.QuotePrefix)
				}
				b.WriteString(line)
			}
			want := b.String() + "\n问题在这"
			if got.Text != want {
				t.Fatalf("Text = %q, want %q", got.Text, want)
			}
		})
	}
}

// TestQuoteWithNoQuestionIsStillAMessage: quoting something and saying
// nothing is "look at this", which is worth ingesting.
func TestQuoteWithNoQuestionIsStillAMessage(t *testing.T) {
	t.Parallel()
	got, _, conn := dispatchOne(t, quotingFrame("msg-q4", "", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "客户改主意了"},
	}))
	c := copyFor(DefaultLocale)
	if got.Text != "> "+c.QuotePrefix+"客户改主意了" {
		t.Fatalf("Text = %q", got.Text)
	}
	if n := len(conn.frames); n != 0 {
		t.Fatalf("a bare quote drew %d receipts; it is a readable message, not an unsupported kind", n)
	}
}

// TestAnEmptyQuoteChangesNothing: a quote object with nothing readable in it
// must not decorate the message with an empty block.
func TestAnEmptyQuoteChangesNothing(t *testing.T) {
	t.Parallel()
	for _, q := range []map[string]any{
		{},
		{"msgtype": "text", "text": map[string]any{"content": "   "}},
		{"msgtype": "sphere_of_influence"},
	} {
		got, _, _ := dispatchOne(t, quotingFrame("msg-q5", "在吗", q))
		if got.Text != "在吗" {
			t.Fatalf("quote %v produced Text = %q, want the message alone", q, got.Text)
		}
	}
}

// A quote decorates a message that was readable on its own. It does not
// rescue one that was not: ingesting the quote off the back of a kind we
// cannot read would store somebody else's words as this person's message,
// with nothing of theirs in it.
func TestAQuoteDoesNotRescueAnUnreadableMessage(t *testing.T) {
	t.Parallel()
	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-q-unreadable",
		"aibotid":  "bot",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "location",
		"quote": map[string]any{
			"msgtype": "text",
			"text":    map[string]any{"content": "客户改主意了"},
		},
	})

	var reached bool
	c := testChannel(func(context.Context, channel.InboundMessage) error {
		reached = true
		return nil
	})
	conn := &recordingConn{}
	if err := c.dispatchFrame(context.Background(),
		frameEnvelope{Cmd: cmdMsgCallback, Body: body},
		conn.autoAck(newWSSender(conn, nil)), slog.Default()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	if reached {
		t.Fatal("a location card with a quote was ingested; the stored message would be entirely somebody else's words")
	}
	if len(conn.frames) != 1 {
		t.Fatalf("the unsupported-kind receipt was not sent (%d frames)", len(conn.frames))
	}
}

// ---- the command that now sits behind a quote ----

// TestTheIssueCommandReadsTheUsersOwnWords is the one that bites. /issue is
// parsed off the front of the body, and the quote now sits in front of it —
// so the command has to be read from the un-quoted text, both for the
// SkipAgentRun decision here and for the parse the engine does downstream.
func TestTheIssueCommandReadsTheUsersOwnWords(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q6", "/issue 回填 Q3 数据", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))
	if !got.SkipAgentRun {
		t.Fatal("a /issue command behind a quote is still a /issue command; the agent will now answer it as prose as well as the engine filing it")
	}
	if got.CommandText != "/issue 回填 Q3 数据" {
		t.Fatalf("CommandText = %q, want the user's own line — the shared parser reads this, and a quote in front of it is read as prose", got.CommandText)
	}
	if strings.HasPrefix(got.CommandText, ">") {
		t.Fatal("the quote leaked into the command source")
	}
}

// TestAQuotedSlashCommandIsNotACommand: the quoted message is somebody else's
// text. Reading a command out of it would let one person's old message create
// an issue the moment another person quotes it.
func TestAQuotedSlashCommandIsNotACommand(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q7", "这条我处理过了", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "/issue 修一下登录"},
	}))
	if got.SkipAgentRun {
		t.Fatal("a /issue inside a quote was treated as this message's command: one person's old text would file an issue the moment somebody quotes it")
	}
}

// TestTheCommandIsTheBodyWhenNothingIsQuoted keeps the ordinary case honest:
// with no quote, the command source and the stored body are the same text.
func TestTheCommandIsTheBodyWhenNothingIsQuoted(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q8", "/issue 修一下登录", nil))
	if got.CommandText != got.Text || got.Text != "/issue 修一下登录" {
		t.Fatalf("body = %q, command = %q", got.Text, got.CommandText)
	}
	if !got.SkipAgentRun {
		t.Fatal("a plain /issue is still a command")
	}
}

// /new under a quote must keep the quote.
//
// This is the ordinary shape of the request: quote the number, ask for it to
// be looked at again in a clean session. The router strips the directive off
// CommandText and writes what is left into Text — the sender's own words,
// without the quote block. So the agent was asked to re-analyse a figure it
// had never been shown, in a session that by construction holds no earlier
// context to find it in.
func TestAFreshSessionUnderAQuoteKeepsTheQuote(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q9", "/new 重新分析这个数", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))

	if !got.ForceFresh {
		t.Fatal("ForceFresh is unset, so the router will strip Text itself and drop the quote with it")
	}
	if !strings.Contains(got.Text, "Q3 毛利率 42.1%") {
		t.Fatalf("Text = %q, want the quoted figure still in it", got.Text)
	}
	if !strings.Contains(got.Text, "重新分析这个数") {
		t.Fatalf("Text = %q, want the sender's own words", got.Text)
	}
	if strings.Contains(got.Text, "/new") {
		t.Fatalf("Text = %q, want the directive stripped", got.Text)
	}
	// CommandText stays as typed so the shared parser classifies it the same
	// way it does on every other platform.
	if got.CommandText != "/new 重新分析这个数" {
		t.Fatalf("CommandText = %q, want the command as the user typed it", got.CommandText)
	}
}

// A bare /new behind a quote is left alone: the router returns before it
// stores anything, so recomposing Text would only be inert — and claiming
// ForceFresh for a message that is never written is a lie about state.
func TestABareFreshCommandUnderAQuoteIsNotRecomposed(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q10", "/new", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))
	if got.ForceFresh {
		t.Fatal("a bare /new must not claim the adapter already stripped anything")
	}
}

// Without a quote there is nothing to preserve, so the router keeps doing the
// stripping — the adapter must not take that over and leave "/new" in Text.
func TestAFreshSessionWithoutAQuoteIsLeftToTheRouter(t *testing.T) {
	t.Parallel()
	got, _, _ := dispatchOne(t, quotingFrame("msg-q11", "/new 重新分析", nil))
	if got.ForceFresh {
		t.Fatal("ForceFresh set with no quote to preserve; the router would then leave /new in Text")
	}
	if got.Text != "/new 重新分析" {
		t.Fatalf("Text = %q, want it untouched for the router", got.Text)
	}
}

// ---- what the language lookup costs ----

// The copy pack is two indexed reads. Almost every message is a plain
// sentence with nothing quoted and reads nothing off the pack, so paying for
// the lookup on all of them would put a database round trip on the read loop
// for no user-visible difference at all.
func TestOnlyAMessageThatReadsThePackPaysForTheLookup(t *testing.T) {
	t.Parallel()
	plain := aibotMsgCallback{MsgType: "text"}
	plain.Text.Content = "在吗"
	if plain.needsCopy() {
		t.Error("a plain sentence asked for a locale lookup it has no string to spend it on")
	}

	quoting := plain
	quoting.Quote = &quotedMessage{}
	quoting.Quote.MsgType = "text"
	quoting.Quote.Text.Content = "客户改主意了"
	if !quoting.needsCopy() {
		t.Error("a quoting message skipped the lookup, so its quote prefix would be written in the deployment default rather than the destination's language")
	}

	unreadable := aibotMsgCallback{MsgType: "location"}
	if !unreadable.needsCopy() {
		t.Error("an unreadable kind skipped the lookup, so its receipt would ignore the reader's language")
	}
}
