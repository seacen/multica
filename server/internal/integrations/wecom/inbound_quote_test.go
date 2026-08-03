package wecom

// inbound_quote_test.go — "引用某条消息再提问" is how people ask about
// something specific in a busy chat: they long-press the message, hit 引用,
// and type their question. WeCom puts the quoted message on the callback in a
// `quote` field; the adapter used to ignore it entirely, so the agent got the
// question with the subject removed — "这个数对吗" about nothing.

import (
	"context"
	"encoding/json"
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

func dispatchOne(t *testing.T, env frameEnvelope) (channel.InboundMessage, *recordingConn) {
	t.Helper()
	var got channel.InboundMessage
	c, conn, _ := testChannel(t, func(_ context.Context, m channel.InboundMessage) error {
		got = m
		return nil
	})
	if err := c.dispatchFrame(context.Background(), env, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	return got, conn
}

// TestQuotedTextReachesTheAgent: the quoted message goes in above the
// question, marked as a quote so the agent can tell whose words are whose.
func TestQuotedTextReachesTheAgent(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q1", "这个数对吗", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))

	c := copyFor(DefaultLocale)
	want := "> " + c.QuotePrefix + "Q3 毛利率 42.1%\n这个数对吗"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q", got.Text, want)
	}
}

// TestQuotedMultiLineTextStaysOneBlock: every line of the quote is marked, or
// the second paragraph reads as the user's own words.
func TestQuotedMultiLineTextStaysOneBlock(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q2", "第二条还没做", map[string]any{
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
			t.Fatalf("quoted line %q is not marked as quoted", line)
		}
	}
}

// TestQuotedMediaRendersAsItsPlaceholder — and is NOT fetched. The quoted
// message's own attachment would arrive with no way to tell it apart from
// one the user just sent, and its url is on somebody else's five-minute
// clock. The placeholder says a picture was being talked about, which is the
// part that matters.
func TestQuotedMediaRendersAsItsPlaceholder(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q3", "左下角那块看不清", map[string]any{
		"msgtype": "image",
		"image":   map[string]any{"url": "https://cos.invalid/quoted.enc", "aeskey": testAESKey},
	}))

	c := copyFor(DefaultLocale)
	want := "> " + c.QuotePrefix + c.MediaImage + "\n左下角那块看不清"
	if got.Text != want {
		t.Fatalf("Text = %q, want %q", got.Text, want)
	}
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if len(wm.Media) != 0 {
		t.Fatalf("the quoted message's attachment was queued for download: %+v", wm.Media)
	}
}

// TestQuotedVoiceAndMixed: a quote carries whatever kind the original was, and
// each one renders the way it would as a message of its own.
func TestQuotedVoiceAndMixed(t *testing.T) {
	c := copyFor(DefaultLocale)
	cases := []struct {
		name  string
		quote map[string]any
		want  string
	}{
		{"a quoted voice note is its transcript", map[string]any{
			"msgtype": "voice",
			"voice":   map[string]any{"content": "周五之前给我"},
		}, "周五之前给我"},
		{"a quoted 图文混排 keeps its order", map[string]any{
			"msgtype": "mixed",
			"mixed": map[string]any{"msg_item": []any{
				map[string]any{"msgtype": "text", "text": map[string]any{"content": "两版对比"}},
				map[string]any{"msgtype": "image", "image": map[string]any{"url": "https://cos.invalid/a"}},
			}},
		}, "两版对比\n" + c.MediaImage},
		{"a quoted file", map[string]any{
			"msgtype": "file",
			"file":    map[string]any{"url": "https://cos.invalid/b"},
		}, c.MediaFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := dispatchOne(t, quotingFrame("msg-q-"+tc.name, "问题在这", tc.quote))
			first := strings.Split(tc.want, "\n")
			var b strings.Builder
			for i, line := range first {
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
	got, conn := dispatchOne(t, quotingFrame("msg-q4", "", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "客户改主意了"},
	}))
	c := copyFor(DefaultLocale)
	if got.Text != "> "+c.QuotePrefix+"客户改主意了" {
		t.Fatalf("Text = %q", got.Text)
	}
	if n := len(conn.sends()); n != 0 {
		t.Fatalf("a bare quote drew %d receipts", n)
	}
}

// TestAnEmptyQuoteChangesNothing: a quote object with nothing readable in it
// must not decorate the message with an empty block.
func TestAnEmptyQuoteChangesNothing(t *testing.T) {
	for _, q := range []map[string]any{
		{},
		{"msgtype": "text", "text": map[string]any{"content": "   "}},
		{"msgtype": "sphere_of_influence"},
	} {
		got, _ := dispatchOne(t, quotingFrame("msg-q5", "在吗", q))
		if got.Text != "在吗" {
			t.Fatalf("quote %v produced Text = %q, want the message alone", q, got.Text)
		}
	}
}

// TestTheIssueCommandReadsTheUsersOwnWords is the one that bites. /issue is
// parsed off the front of the body, and the quote now sits in front of it —
// so the command has to be read from the un-quoted text, both for the
// SkipAgentRun decision here and for the parse the engine does downstream.
func TestTheIssueCommandReadsTheUsersOwnWords(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q6", "/issue 回填 Q3 数据", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "Q3 毛利率 42.1%"},
	}))
	if !got.SkipAgentRun {
		t.Fatal("a /issue command behind a quote is still a /issue command")
	}
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if wm.CommandBody != "/issue 回填 Q3 数据" {
		t.Fatalf("CommandBody = %q, want the user's own line", wm.CommandBody)
	}
	if strings.HasPrefix(wm.CommandBody, ">") {
		t.Fatal("the quote leaked into the command body")
	}
}

// TestAQuotedSlashCommandIsNotACommand: the quoted message is somebody else's
// text. Reading a command out of it would let one person's old message create
// an issue the moment another person quotes it.
func TestAQuotedSlashCommandIsNotACommand(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q7", "这条我处理过了", map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "/issue 修一下登录"},
	}))
	if got.SkipAgentRun {
		t.Fatal("a /issue inside a quote must not be treated as this message's command")
	}
}

// TestCommandBodyIsTheBodyWhenNothingIsQuoted keeps the ordinary case honest:
// with no quote, the command and the stored body are the same text.
func TestCommandBodyIsTheBodyWhenNothingIsQuoted(t *testing.T) {
	got, _ := dispatchOne(t, quotingFrame("msg-q8", "/issue 修一下登录", nil))
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if wm.CommandBody != got.Text || got.Text != "/issue 修一下登录" {
		t.Fatalf("body = %q, command = %q", got.Text, wm.CommandBody)
	}
	if !got.SkipAgentRun {
		t.Fatal("a plain /issue is still a command")
	}
}
