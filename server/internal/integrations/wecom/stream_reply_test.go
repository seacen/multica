package wecom

// stream_reply_test.go — the streaming reply, end to end.
//
// The behaviour under test is what the user sees: send a question, get a
// bubble immediately, and watch that same bubble turn into the answer. Every
// test here is written against that sentence rather than against the frames,
// except where the frames themselves are the contract (req_id passthrough,
// visible closing content) — those are the two things WeCom silently punishes
// and no integration test would catch.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// ---- scaffolding ----

// ackingConn is a recordingConn that also plays the server's half of the
// exchange: every aibot_respond_msg frame gets an ack, with whatever errcode
// the test wants. Without it respondStream would sit waiting for a verdict
// that never comes.
type ackingConn struct {
	recordingConn
	mu     sync.Mutex
	sender *wsSender
	code   int
	msg    string
	silent bool
}

func (c *ackingConn) WriteMessage(t int, data []byte) error {
	if err := c.recordingConn.WriteMessage(t, data); err != nil {
		return err
	}
	var f struct {
		Cmd     string       `json:"cmd"`
		Headers frameHeaders `json:"headers"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	if f.Cmd != cmdRespondMsg {
		return nil
	}
	c.mu.Lock()
	sender, code, msg, silent := c.sender, c.code, c.msg, c.silent
	c.mu.Unlock()
	if sender == nil || silent {
		return nil
	}
	sender.deliverAck(f.Headers.ReqID, code, msg)
	return nil
}

func (c *ackingConn) rejectWith(code int, msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.code, c.msg = code, msg
}

// streamView is one aibot_respond_msg frame, flattened to the fields that
// carry meaning.
type streamView struct {
	ReqID   string
	ID      string
	Content string
	Finish  bool
}

func framesOf(c *recordingConn, cmd string) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]any
	for _, f := range c.frames {
		if f["cmd"] == cmd {
			out = append(out, f)
		}
	}
	return out
}

func streamViews(t *testing.T, c *recordingConn) []streamView {
	t.Helper()
	var out []streamView
	for _, f := range framesOf(c, cmdRespondMsg) {
		headers, _ := f["headers"].(map[string]any)
		body, _ := f["body"].(map[string]any)
		st, _ := body["stream"].(map[string]any)
		if st == nil {
			t.Fatalf("aibot_respond_msg frame without a stream body: %v", f)
		}
		reqID, _ := headers["req_id"].(string)
		id, _ := st["id"].(string)
		content, _ := st["content"].(string)
		finish, _ := st["finish"].(bool)
		out = append(out, streamView{ReqID: reqID, ID: id, Content: content, Finish: finish})
	}
	return out
}

// streamInbound builds the wecom-side inbound envelope carrying a callback
// req_id — the value the whole feature hangs on.
func streamInbound(reqID, chatID string) channel.InboundMessage {
	mc := aibotMsgCallback{MsgID: "MSGID-1", ChatID: chatID, ChatType: "single"}
	mc.From.UserID = chatID
	mc.MsgType = "text"
	mc.Text.Content = "帮我查一下"
	return channelMessageFromCallback("BOT-1", mc, copyFor(DefaultLocale), mc.Text.Content, reqID)
}

// streamRig is one wired-up installation: a live socket, the registry that
// finds it, the store, and the typing manager.
//
// The rig's default sender IS the principal, in their own chat, because that
// is the turn every other test in this package is about. progress_level_test.go
// is where the other audiences are set up.
type streamRig struct {
	conn       *ackingConn
	senders    *sendersRegistry
	streams    *streamStore
	typing     *TypingIndicatorManager
	identities *fakeIdentities
	inst       engine.ResolvedInstallation
	session    pgtype.UUID

	// principalSender is the WeCom userid bound to the installation's
	// principal — the sender ingest() speaks as.
	principalSender string
}

func newStreamRig(t *testing.T) *streamRig {
	t.Helper()
	conn := &ackingConn{}
	sender := newWSSender(conn, testLogger())
	conn.mu.Lock()
	conn.sender = sender
	conn.mu.Unlock()

	instID := uuidOf(7)
	senders := NewSendersRegistry()
	senders.log = testLogger()
	senders.set(instID, sender)

	const principalSender = "T-alex"
	principal := uuidOf(9)
	identities := &fakeIdentities{byChannelUser: map[string]pgtype.UUID{principalSender: principal}}

	streams := newStreamStore()
	typing := NewTypingIndicator(TypingIndicatorConfig{
		Senders:    senders,
		Streams:    streams,
		Identities: identities,
		Logger:     testLogger(),
	})
	return &streamRig{
		conn:       conn,
		senders:    senders,
		streams:    streams,
		typing:     typing,
		identities: identities,
		inst: engine.ResolvedInstallation{
			ID:              instID,
			InstallerUserID: principal,
			Platform:        Installation{Locale: string(LocaleZhHans)},
		},
		session:         uuidOf(21),
		principalSender: principalSender,
	}
}

func (r *streamRig) ingest(t *testing.T, reqID string) {
	t.Helper()
	r.ingestMessage(t, streamInbound(reqID, r.principalSender))
}

// ingestMessage opens the bubble for an envelope the test built itself — a
// group room, or a colleague's own chat.
func (r *streamRig) ingestMessage(t *testing.T, msg channel.InboundMessage) {
	t.Helper()
	r.typing.OnIngested(context.Background(), r.inst, msg, r.session)
}

// chatDone publishes what the agent's finished turn publishes.
func chatDoneEvent(sessionID pgtype.UUID, content string) events.Event {
	return events.Event{
		Type:          protocol.EventChatDone,
		ChatSessionID: uuidText(sessionID),
		Payload:       protocol.ChatDonePayload{ChatSessionID: uuidText(sessionID), Content: content},
	}
}

// ---- the first frame ----

// TestFirstFrameOpensTheBubbleAndKeepsTheHandle is the whole point of the
// feature: the user sees something the moment the message lands, and the
// adapter keeps what it needs to replace it later.
func TestFirstFrameOpensTheBubbleAndKeepsTheHandle(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 1 {
		t.Fatalf("want exactly one opening frame, got %d", len(frames))
	}
	if frames[0].Finish {
		t.Error("the opening frame must not finish the stream")
	}
	if frames[0].ReqID != "REQ-42" {
		t.Errorf("req_id = %q, want the callback's REQ-42 — WeCom refuses any other", frames[0].ReqID)
	}
	if frames[0].ID == "" {
		t.Error("the opening frame needs a stream id to update later")
	}
	if frames[0].Content != streamThinkingPlaceholder {
		t.Errorf("content = %q, want the thinking placeholder", frames[0].Content)
	}

	h, ok := rig.streams.peek(rig.session)
	if !ok {
		t.Fatal("no handle stored; the answer would have nowhere to land")
	}
	if h.ReqID != "REQ-42" || h.StreamID != frames[0].ID {
		t.Errorf("stored handle %+v does not match the frame that was sent", h)
	}
}

// TestNoBubbleWithoutACallbackReqID — an event callback's req_id is refused by
// the server (846605), so a message that arrives without a usable one gets no
// bubble rather than a broken one.
func TestNoBubbleWithoutACallbackReqID(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "")

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 0 {
		t.Fatalf("sent %d stream frames with no req_id to echo", got)
	}
	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("stored a handle that can never be used")
	}
}

// TestIssueCommandOpensNoBubble — a standalone /issue is answered by the
// replier and never triggers an agent run, so nothing downstream would ever
// close a bubble opened for it.
func TestIssueCommandOpensNoBubble(t *testing.T) {
	rig := newStreamRig(t)
	msg := streamInbound("REQ-1", "T-alex")
	msg.SkipAgentRun = true
	rig.typing.OnIngested(context.Background(), rig.inst, msg, rig.session)

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 0 {
		t.Fatalf("opened %d bubbles for a message that will never be answered", got)
	}
}

// TestSecondMessageJoinsTheOpenBubble — two messages inside one debounce
// window produce one run, so a second bubble would be one nobody closes.
func TestSecondMessageJoinsTheOpenBubble(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-1")
	rig.ingest(t, "REQ-2")

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 1 {
		t.Fatalf("want one bubble for the window, got %d", len(frames))
	}
	if h, _ := rig.streams.peek(rig.session); h.ReqID != "REQ-1" {
		t.Errorf("handle req_id = %q, want the first message's REQ-1", h.ReqID)
	}
}

// TestAnOpeningFrameThatLostTheRaceKeepsItsHandle — the opening frame yields
// its ack slot to anything already in flight on the same req_id and comes back
// errStreamBusy. Dropping the handle there is the worst of both: the frame that
// beat it to the socket carried a new stream id, so the bubble exists on the
// user's screen, and nothing is left that could ever close it.
func TestAnOpeningFrameThatLostTheRaceKeepsItsHandle(t *testing.T) {
	rig := newStreamRig(t)
	sender := rig.senders.get(rig.inst.ID)
	if _, ok := sender.awaitAck("REQ-42", false); !ok {
		t.Fatal("could not occupy the ack slot")
	}

	rig.ingest(t, "REQ-42")

	h, ok := rig.streams.peek(rig.session)
	if !ok {
		t.Fatal("the handle was dropped; nothing can close the bubble now")
	}
	if h.ReqID != "REQ-42" {
		t.Errorf("handle req_id = %q, want the callback's", h.ReqID)
	}
}

// TestAnOpeningFrameTheServerRefusedIsLetGo — the other half. A refusal that
// says the stream is unusable means no bubble was ever painted, so keeping the
// handle would swallow the answer instead of delivering it.
func TestAnOpeningFrameTheServerRefusedIsLetGo(t *testing.T) {
	rig := newStreamRig(t)
	rig.conn.rejectWith(errcodeStreamBadReqID, "bad req_id")

	rig.ingest(t, "REQ-42")

	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("kept a handle for a stream the server refused outright")
	}
}

// ---- the answer ----

func newOutboundUnder(rig *streamRig) *Outbound {
	return NewOutbound(&fakeOutboundQueries{}, rig.senders, rig.streams, testLogger())
}

// boundQueries answers the lookups the fallback path makes on its way to a
// plain aibot_send_msg.
func boundQueries(instID pgtype.UUID) *fakeOutboundQueries {
	return &fakeOutboundQueries{
		binding: db.ChannelChatSessionBinding{
			InstallationID: instID,
			ChannelChatID:  "T-alex",
			ChatType:       "p2p",
		},
		install: db.ChannelInstallation{ID: instID, Status: string(InstallationActive)},
	}
}

// TestAnswerReplacesTheBubbleInPlace — the core promise. One question, one
// bubble, and the answer arrives inside it rather than underneath it.
func TestAnswerReplacesTheBubbleInPlace(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	opening := streamViews(t, &rig.conn.recordingConn)[0]

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("want opening + closing frame, got %d", len(frames))
	}
	final := frames[1]
	if !final.Finish {
		t.Error("the answer frame must finish the stream")
	}
	if final.ID != opening.ID || final.ReqID != opening.ReqID {
		t.Errorf("answer frame %+v does not address the bubble the question opened", final)
	}
	if final.Content != "答案是 42" {
		t.Errorf("content = %q, want the agent's answer", final.Content)
	}
	if got := len(framesOf(&rig.conn.recordingConn, cmdSendMsg)); got != 0 {
		t.Errorf("sent %d separate messages; the answer should have landed in the bubble", got)
	}
	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("the handle outlived the answer; a finished stream can never be updated again")
	}
}

// TestAnswerWithoutAHandleGoesOutAsANewMessage — a restart, a lease flip or a
// turn that never opened a bubble all land here. The answer still arrives.
func TestAnswerWithoutAHandleGoesOutAsANewMessage(t *testing.T) {
	rig := newStreamRig(t)
	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 0 {
		t.Fatalf("sent %d stream frames with no bubble open", got)
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("plain messages = %v, want the answer", got)
	}
}

// TestExpiredStreamFallsBackToANewMessage — past its window WeCom refuses the
// frame with 846608. The answer must not die with the bubble.
func TestExpiredStreamFallsBackToANewMessage(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")

	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("plain messages = %v, want the answer to fall back to a new message", got)
	}
}

// TestClosingFrameAlwaysCarriesVisibleText — WeCom ignores invisible content,
// so a blank closing frame closes nothing and the user is left with a spinner
// that never stops.
func TestClosingFrameAlwaysCarriesVisibleText(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "   \n  "))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("want a closing frame, got %+v", frames)
	}
	if !hasVisibleChar(frames[1].Content) {
		t.Errorf("closing content %q has nothing visible in it", frames[1].Content)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamNoReply {
		t.Errorf("closing content = %q, want the localized no-reply copy", frames[1].Content)
	}
}

// TestAnAnswerAboutThinkTagsIsStillReadable — <think></think> is not markup
// this adapter invented, it is what the WeChat client folds into its own
// thinking affordance, and the progress frames are built out of it. An answer
// that happens to contain the literal — quoting a prompt, discussing this
// feature, pasting XML — would be folded away with it, in a chat with no edit
// and no unsend.
func TestAnAnswerAboutThinkTagsIsStillReadable(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "进度是用 <think>…</think> 画出来的"))

	frames := streamViews(t, &rig.conn.recordingConn)
	last := frames[len(frames)-1]
	if !last.Finish {
		t.Fatalf("last frame = %+v, want the sealed answer", last)
	}
	if strings.Contains(last.Content, "<think>") || strings.Contains(last.Content, "</think>") {
		t.Fatalf("the answer still carries a live think tag: %q", last.Content)
	}
	if !strings.Contains(last.Content, "think") || !strings.Contains(last.Content, "画出来的") {
		t.Fatalf("the answer lost its words defusing the tag: %q", last.Content)
	}
}

// TestOrdinaryMarkdownIsLeftAlone — the fix must reach the one construct that
// breaks the bubble and nothing else. Comparisons, generics and HTML samples
// are ordinary answer text.
func TestOrdinaryMarkdownIsLeftAlone(t *testing.T) {
	const answer = "if a < b && c > d { … }\n\n<div>hi</div>\n\n`Vec<Thing>`"
	body, err := respondStreamBody("s1", answer, true)
	if err != nil {
		t.Fatalf("respondStreamBody: %v", err)
	}
	st, _ := body["stream"].(map[string]any)
	if got, _ := st["content"].(string); got != answer {
		t.Errorf("content = %q, want it untouched", got)
	}
}

// TestAProgressFrameKeepsItsThinkTags — the mid-run frames ARE the affordance,
// so the closing frame is the only place this may apply.
func TestAProgressFrameKeepsItsThinkTags(t *testing.T) {
	body, err := respondStreamBody("s1", "<think>正在读取 x.go</think>", false)
	if err != nil {
		t.Fatalf("respondStreamBody: %v", err)
	}
	st, _ := body["stream"].(map[string]any)
	if got, _ := st["content"].(string); got != "<think>正在读取 x.go</think>" {
		t.Errorf("content = %q, want the thinking wrapper intact", got)
	}
}

// TestBodyBuilderRefusesABlankClosingFrame pins the same rule one level down,
// where every future caller passes through.
func TestBodyBuilderRefusesABlankClosingFrame(t *testing.T) {
	if _, err := respondStreamBody("s1", "  \t ", true); err == nil {
		t.Error("a closing frame with nothing visible must be refused, not sent")
	}
	if _, err := respondStreamBody("s1", "  ", false); err != nil {
		t.Errorf("a mid-stream frame may be blank: %v", err)
	}
	if _, err := respondStreamBody("", "hi", true); err == nil {
		t.Error("a stream frame with no stream id must be refused")
	}
}

// TestStreamContentIsCutToTheProtocolLimit — 20480 bytes, and the cut must not
// land inside a multi-byte character.
func TestStreamContentIsCutToTheProtocolLimit(t *testing.T) {
	long := strings.Repeat("测", streamContentLimit)
	got := truncateStreamContent(long)
	if len(got) > streamContentLimit {
		t.Fatalf("content is %d bytes, over the %d limit", len(got), streamContentLimit)
	}
	if !utf8.ValidString(got) {
		t.Error("truncation cut a character in half")
	}
	if short := truncateStreamContent("答案"); short != "答案" {
		t.Errorf("short content was altered: %q", short)
	}
}

// ---- the ways a bubble ends without an answer ----

// TestSettledClosesTheBubble — agent offline, archived, or an enqueue that
// failed. No task run means no chat-done event will ever arrive, so this is
// the only chance to stop the spinner.
func TestSettledClosesTheBubble(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	rig.typing.OnSettled(context.Background(), rig.session)

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("want a closing frame, got %+v", frames)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamNotStarted {
		t.Errorf("closing content = %q, want the not-started copy", frames[1].Content)
	}
	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("handle survived the close")
	}
}

// TestTaskFailureClosesTheBubble — a failed run publishes task:failed and
// never publishes chat:done, so the outbound subscriber never fires.
func TestTaskFailureClosesTheBubble(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	bus := events.New()
	rig.typing.Register(bus)
	bus.Publish(events.Event{
		Type:    protocol.EventTaskFailed,
		Payload: map[string]any{"chat_session_id": uuidText(rig.session)},
	})

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("want a closing frame, got %+v", frames)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamFailed {
		t.Errorf("closing content = %q, want the failure copy", frames[1].Content)
	}
}

// TestGuardClosesTheBubbleBeforeTheWindowRunsOut — a run longer than the
// protocol's stream window would otherwise leave a spinner the server will no
// longer let us touch.
func TestGuardClosesTheBubbleBeforeTheWindowRunsOut(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.guardAfter = 10 * time.Millisecond
	rig.ingest(t, "REQ-42")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(streamViews(t, &rig.conn.recordingConn)) == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("the guard never closed the bubble: %+v", frames)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamStillWorking {
		t.Errorf("closing content = %q, want the still-working copy", frames[1].Content)
	}
	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("handle survived the guard")
	}
}

// TestAFailedCloseStillTellsTheUser — StreamFailed is the only "that run did
// not go through" WeCom ever sees; the replier speaks for needs_binding,
// offline, archived and issue_created and for nothing else. A closing frame
// that cannot go out — a lease flip, the Supervisor's backoff, the seconds
// after a reconnect — used to take that notice with it and leave the bubble
// spinning with no explanation ever coming.
func TestAFailedCloseStillTellsTheUser(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.senders.clear(rig.inst.ID) // the reconnect window

	rig.typing.OnSettled(context.Background(), rig.session)

	if n := rig.senders.pending.depth(rig.inst.ID); n != 1 {
		t.Fatalf("queue depth %d; the notice was not held for the next connection", n)
	}
	conn := &recordingConn{}
	rig.senders.set(rig.inst.ID, newWSSender(conn, testLogger()))
	rig.senders.flushPending(rig.inst.ID)

	got := contentsOf(conn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamNotStarted {
		t.Fatalf("after the reconnect the user received %v, want the notice as a plain message", got)
	}
}

// TestAFailedCloseAddressesTheChatThatAsked — the fallback has to reach the
// same conversation the bubble was in, and by then the binding row may point
// somewhere else. The addressing captured at ingest is what makes that safe.
func TestAFailedCloseAddressesTheChatThatAsked(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.senders.clear(rig.inst.ID)

	rig.typing.OnSettled(context.Background(), rig.session)

	msg, ok := rig.senders.pending.pop(rig.inst.ID)
	if !ok {
		t.Fatal("nothing held for the next connection")
	}
	if msg.ChatID != "T-alex" || msg.ChatType != chatTypeSingleInt {
		t.Fatalf("held message addresses %+v, want the chat the question came from", msg)
	}
}

// TestStaleHandleIsTreatedAsGone — a handle past the protocol window would
// swallow the answer rather than deliver it, so the store must disown it.
func TestStaleHandleIsTreatedAsGone(t *testing.T) {
	store := newStreamStore()
	base := time.Now()
	store.now = func() time.Time { return base }
	if !store.claim(uuidOf(3), streamHandle{ReqID: "R", StreamID: "S"}) {
		t.Fatal("claim refused on an empty store")
	}
	store.now = func() time.Time { return base.Add(streamMaxAge + time.Second) }

	if _, ok := store.peek(uuidOf(3)); ok {
		t.Error("peek returned a handle the server would refuse")
	}
	if _, ok := store.take(uuidOf(3), roundOver); ok {
		t.Error("take returned a handle the server would refuse")
	}
	if !store.claim(uuidOf(3), streamHandle{ReqID: "R2", StreamID: "S2"}) {
		t.Error("an expired handle must not block a fresh one")
	}
}

// ---- concurrency ----

// TestStreamStoreIsRaceFree — the store is written from the read loop's
// detached typing goroutine and read from the bus subscriber. Run under -race.
func TestStreamStoreIsRaceFree(t *testing.T) {
	rig := newStreamRig(t)
	out := newOutboundUnder(rig)

	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		session := uuidOf(byte(100 + i%8))
		wg.Add(2)
		go func() {
			defer wg.Done()
			rig.typing.OnIngested(context.Background(), rig.inst, streamInbound("REQ-x", "T-alex"), session)
		}()
		go func() {
			defer wg.Done()
			out.handleEvent(chatDoneEvent(session, "答案"))
		}()
	}
	wg.Wait()
}
