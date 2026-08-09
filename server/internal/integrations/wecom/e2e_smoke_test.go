package wecom

// e2e_smoke_test.go — the six things a person would actually try in WeCom,
// driven through the real Connect loop against a scripted socket, plus one
// pinned limit. Each one is a capability this batch of work added; before it,
// every one of these arrived and was answered with "抱歉，我目前只能处理文字消
// 息。" or with nothing at all.
//
// This is not a substitute for trying it in the WeCom client — the client's
// own rendering (the animated dots, how a card looks) can only be judged
// there. It is here so that "the message reaches the agent" is a fact rather
// than a hope.
//
// "Reaches the agent" is the weakest claim worth making, so every scenario
// asserts what has to be true downstream of arrival as well: the words the
// agent will read, whether the router will treat them as a command, and for a
// screenshot, whether there is anything for the ingest to fetch. A count of
// one proves only that the socket did not refuse the frame.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// captured records what the engine would have been handed.
type captured struct {
	mu   sync.Mutex
	msgs []channel.InboundMessage
}

func (c *captured) handle(_ context.Context, m channel.InboundMessage) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	return nil
}

func (c *captured) wait(d time.Duration) []channel.InboundMessage {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(c.msgs)
		out := append([]channel.InboundMessage(nil), c.msgs...)
		c.mu.Unlock()
		if n > 0 {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func msgCallbackBody(t *testing.T, extra map[string]any) json.RawMessage {
	t.Helper()
	base := map[string]any{
		"msgid":    "M-1",
		"aibotid":  "bot-1",
		"chatid":   "T-alex",
		"chattype": "single",
		"from":     map[string]any{"userid": "T-alex"},
	}
	for k, v := range extra {
		base[k] = v
	}
	b, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	return b
}

// runInbound drives one inbound frame through the real Connect loop and
// returns the single message that reached the handler. It fails the test if
// nothing arrived, so every caller below can read got[0] and say what it is
// about that message that matters.
func runInbound(t *testing.T, body json.RawMessage) channel.InboundMessage {
	t.Helper()
	return runInboundAs(t, body, "")
}

// runInboundAs is runInbound with the bot's configured display name set, which
// is what a group @-mention has to be matched against when the name contains
// a space.
func runInboundAs(t *testing.T, body json.RawMessage, botDisplayName string) channel.InboundMessage {
	t.Helper()
	cap := &captured{}
	conn := newWelcomeConn(serverFrame(t, cmdMsgCallback, "req-1", body))
	c := connectedChannel(t, conn,
		&fakeWelcomeLookup{binding: db.ChannelUserBinding{MulticaUserID: mustTestUUID(t)}},
		cap.handle)
	c.botDisplayName = botDisplayName

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()
	got := cap.wait(3 * time.Second)
	cancel()
	<-done

	if len(got) == 0 {
		t.Fatal("nothing reached the agent: the message was dropped or answered at the socket, so there is no session, no agent run and nothing in the web UI")
	}
	if len(got) > 1 {
		t.Fatalf("one inbound frame reached the agent %d times — a duplicate is a second answer to a question asked once", len(got))
	}
	return got[0]
}

// 1. A typed message — the baseline that always worked, and the control for
// everything below: if this one breaks, the socket harness is broken and the
// other six are telling you nothing.
func TestE2E_TypedMessageReachesTheAgent(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"msgtype": "text",
		"text":    map[string]any{"content": "登录跳转坏了"},
	}))
	if got.Text != "登录跳转坏了" {
		t.Errorf("the agent reads %q, want the words that were typed", got.Text)
	}
	if got.Type != channel.MsgTypeText {
		t.Errorf("type = %v, want text", got.Type)
	}
}

// 2. A voice note. WeCom runs the recognition on its side and delivers only
// the transcript, so this is a sentence that happened to be spoken. It used to
// take the unreadable-kind path and come back as a receipt: the person got an
// apology instead of an answer, and nothing reached the agent at all.
func TestE2E_VoiceNoteReachesTheAgentAsItsTranscript(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"msgtype": "voice",
		"voice":   map[string]any{"content": "帮我把这个 bug 记一下"},
	}))
	if got.Text != "帮我把这个 bug 记一下" {
		t.Errorf("the agent reads %q, want the transcript — a voice note whose words do not arrive is a message the agent cannot answer", got.Text)
	}
}

// 3. A spoken slash command is still a command. The transcript is what the
// command parser reads, so "/issue …" said out loud has to file an issue and
// stop, exactly as the typed one does — not reach the agent and draw an "I
// don't recognize this slash command" on top of the issue that was just filed.
func TestE2E_SpokenIssueCommandIsRecognised(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"msgtype": "voice",
		"voice":   map[string]any{"content": "/issue 登录跳转坏了"},
	}))
	if got.CommandText != "/issue 登录跳转坏了" {
		t.Errorf("CommandText = %q — the shared parser reads this field, and a spoken command that does not land in it is classified as prose", got.CommandText)
	}
	if !got.SkipAgentRun {
		t.Error("a spoken /issue was not recognised as a command: the issue is filed AND the agent is asked about it, so the chat gets a confirmation followed by a complaint about an unknown command")
	}
}

// 4. A screenshot. Before this it was dropped at the socket — no session, no
// agent run, nothing in the web UI.
//
// "Reached the agent" is not enough to assert here, and asserting only that
// would let this pass on a build where the photo arrives as an empty message
// with nothing to fetch. The bytes are not on this path: the read loop hands
// over a placeholder body plus the download reference, and the ingest
// (media_ingest.go) fetches from that reference afterwards. So the three
// things worth pinning are the three things downstream reads — the typed kind,
// a body that says something in the meantime, and a reference HasMedia can
// find.
func TestE2E_AScreenshotReachesTheAgentWithSomethingToFetch(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"msgtype": "image",
		"image": map[string]any{
			"url":    "https://wecom.example/cos/obj?sig=x",
			"aeskey": testAESKey,
		},
	}))
	if got.Type != channel.MsgTypeImage {
		t.Errorf("type = %v, want image — a screenshot that arrives typed as text is not attachable", got.Type)
	}
	if strings.TrimSpace(got.Text) == "" {
		t.Error("the message body is empty: the placeholder is what the transcript shows until the download lands, and what it keeps if the download never does")
	}
	// The production predicate, not a re-parse of the JSON: this is the exact
	// question the connector asks before it grants the message a media
	// deadline. Answer no and the attachment is never fetched, however well
	// the rest of the path works.
	if !(&wecomMediaResolver{}).HasMedia(got) {
		t.Fatal("nothing for the ingest to fetch: the download reference did not survive onto the message")
	}
	wm, err := wecomMsgFromRaw(got)
	if err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if wm.Media[0].URL == "" {
		t.Error("media reference has no url: there is nothing to download")
	}
	// The key is the half that is easy to lose silently — a wrong field name
	// on the callback leaves it empty and the url still looks fine. WeCom
	// serves these objects encrypted, so a url without its key downloads bytes
	// nobody can decrypt.
	if wm.Media[0].AESKey == "" {
		t.Error("media reference has no aeskey: the download succeeds and decryption then fails on every byte of it")
	}
}

// 5. In a group with a ONE-WORD bot name, the whitespace heuristic is enough.
// Every slash command in every group used to be dropped here, because the
// mention arrives as literal text in front of the command and nothing stripped
// it before the parser looked.
func TestE2E_GroupMentionThenSlashCommand_OneWordName(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"chattype": "group",
		"chatid":   "GROUP-1",
		"msgtype":  "text",
		"text":     map[string]any{"content": "@Multica /issue 登录跳转坏了"},
	}))
	if got.CommandText != "/issue 登录跳转坏了" {
		t.Errorf("CommandText = %q, want the command with the addressing removed", got.CommandText)
	}
	if !got.SkipAgentRun {
		t.Error("a slash command behind a group @-mention was not recognised — this is the case where NO slash command worked at all")
	}
}

// 5b. A name WITH a space works only once the display name is configured: the
// name is matched whole, so the strip takes all of it and leaves the command.
func TestE2E_GroupMentionThenSlashCommand_NameWithSpace(t *testing.T) {
	got := runInboundAs(t, msgCallbackBody(t, map[string]any{
		"chattype": "group",
		"chatid":   "GROUP-1",
		"msgtype":  "text",
		"text":     map[string]any{"content": "@Multica Bot /issue 登录跳转坏了"},
	}), "Multica Bot")
	if got.CommandText != "/issue 登录跳转坏了" {
		t.Errorf("CommandText = %q — the configured name was not matched whole, so part of it is still glued to the command", got.CommandText)
	}
	if !got.SkipAgentRun {
		t.Error("a configured multi-word bot name did not match its own @-mention")
	}
}

// 5c. The known limit, pinned so it is a documented behaviour rather than a
// surprise: a name with a space and NO configured display name falls back to
// the whitespace heuristic, which cuts at the first space and leaves
// "Bot /issue …" — not a command. This is exactly what the "Bot name in chat"
// field on the connect dialog exists to fix, and why its hint says to fill it
// in when the name contains a space.
func TestE2E_GroupMentionWithSpacedNameAndNoConfigIsAKnownLimit(t *testing.T) {
	got := runInbound(t, msgCallbackBody(t, map[string]any{
		"chattype": "group",
		"chatid":   "GROUP-1",
		"msgtype":  "text",
		"text":     map[string]any{"content": "@Multica Bot /issue 登录跳转坏了"},
	}))
	if got.CommandText != "Bot /issue 登录跳转坏了" {
		t.Errorf("CommandText = %q — the heuristic is documented as cutting at the first space; if that changed, this test and the connect dialog's hint both need rewriting", got.CommandText)
	}
	if got.SkipAgentRun {
		t.Error("this now works unconfigured — good, but the hint telling admins to fill the name in is then misleading and should be updated")
	}
}
