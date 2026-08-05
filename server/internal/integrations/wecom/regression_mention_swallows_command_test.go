package wecom

// regression_mention_swallows_command_test.go — a slash command typed after an
// @-mention has to still be a command.
//
// In a WeCom group you reach the bot by @-mentioning it, and the mention is
// part of the message text: "@Andrew /new re-analyse this". The shared command
// parser reads the first non-empty line and wants the command at the start of
// it, so with the mention still there it sees "@Andrew" and decides this is
// ordinary prose. /new is silently dropped — the session is not reset, the
// agent answers with the previous conversation still in hand, and nothing
// tells the person their command did not take.
//
// Slack strips the mention token before classifying (slack/inbound.go
// cleanText), and Feishu is handed an already-clean command body by the
// platform. WeCom is the one adapter that passes the raw text through, which
// is the same shape as the quote-block defect: the command source has to be
// the sender's OWN words, with the addressing removed.

import (
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// mentionedGroupMessage is a real group callback: the user long-pressed a
// message to quote it, @-mentioned the bot, and typed a command.
func mentionedGroupMessage(body string) aibotMsgCallback {
	mc := aibotMsgCallback{MsgID: "MSGID-M", ChatID: "wr-room", ChatType: "group"}
	mc.From.UserID = "T-alex"
	mc.MsgType = "text"
	mc.Text.Content = body
	return mc
}

// TestACommandAfterAMentionIsStillACommand: what breaks for a person when this
// regresses is that /new does nothing at all. They ask for a fresh start, the
// bot keeps the old thread, and its answer refers back to a conversation they
// were trying to leave behind.
func TestACommandAfterAMentionIsStillACommand(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"mention then command", "@Andrew /new 重新分析一下"},
		{"two mentions then command", "@Andrew @Bowen /new 重新分析一下"},
		{"mention, full-width space, command", "@Andrew　/new 重新分析一下"},
		{"quoted message, mention, command", "@Andrew /new 重新分析一下"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mc := mentionedGroupMessage(tc.body)
			pack := copyFor(DefaultLocale)
			own, ok := mc.ownText(pack)
			if !ok {
				t.Fatalf("setup: %q produced no readable text", tc.body)
			}
			msg := channelMessageFromCallback("BOT-1", mc, pack, own, "REQ-M")

			if _, isFresh := engine.ParseFreshSessionCommand(msg.CommandText); !isFresh {
				t.Errorf("the shared parser did not see /new in %q.\n"+
					"CommandText = %q — the @-mention is still in front of the command, so the person's "+
					"request for a fresh session is silently dropped and the agent answers with the old "+
					"conversation still in hand.", tc.body, msg.CommandText)
			}
		})
	}
}

// TestAMentionDoesNotEatOrdinaryWords: stripping the addressing must not eat
// the message. Someone who @-mentions the bot and then asks a question still
// has to have their question reach the agent.
func TestAMentionDoesNotEatOrdinaryWords(t *testing.T) {
	mc := mentionedGroupMessage("@Andrew 昨天的日志里有什么报错")
	pack := copyFor(DefaultLocale)
	own, _ := mc.ownText(pack)
	msg := channelMessageFromCallback("BOT-1", mc, pack, own, "REQ-N")

	if !strings.Contains(msg.Text, "昨天的日志里有什么报错") {
		t.Fatalf("the stored message lost the question: %q", msg.Text)
	}
	if !strings.Contains(msg.CommandText, "昨天的日志里有什么报错") {
		t.Fatalf("the command source lost the question: %q", msg.CommandText)
	}
}

// TestAMentionOfSomebodyElseIsNotStripped: only the addressing at the very
// front is a mention of the bot. A message that talks ABOUT a colleague must
// keep their name.
func TestAMentionOfSomebodyElseIsNotStripped(t *testing.T) {
	mc := mentionedGroupMessage("@Andrew 帮我问一下 @李雷 昨天那个数")
	pack := copyFor(DefaultLocale)
	own, _ := mc.ownText(pack)
	msg := channelMessageFromCallback("BOT-1", mc, pack, own, "REQ-O")

	if !strings.Contains(msg.CommandText, "@李雷") {
		t.Fatalf("a mention inside the sentence was stripped too: %q", msg.CommandText)
	}
}
