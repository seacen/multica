package wecom

// stream_progress_test.go — the mid-run refresh. Same bubble, new words, and
// none of it allowed to get in the way of the answer.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// fakeTasks answers the one lookup the progress subscriber makes: which chat
// session does this task belong to. Counted under a mutex because the same
// subscriber is driven from several goroutines in the race test.
type fakeTasks struct {
	mu      sync.Mutex
	session pgtype.UUID
	err     error
	calls   int
}

func (f *fakeTasks) GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error) {
	f.mu.Lock()
	f.calls++
	err, session := f.err, f.session
	f.mu.Unlock()
	if err != nil {
		return db.AgentTaskQueue{}, err
	}
	return db.AgentTaskQueue{ChatSessionID: session}, nil
}

func (f *fakeTasks) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestProgressRewritesTheSameBubble — the point of the refresh: the user
// watches one bubble change its mind, not a column of status messages.
func TestProgressRewritesTheSameBubble(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	opening := streamViews(t, &rig.conn.recordingConn)[0]

	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("want opening + refresh, got %d frames", len(frames))
	}
	refresh := frames[1]
	if refresh.Finish {
		t.Error("a progress refresh must leave the bubble open")
	}
	if refresh.ID != opening.ID || refresh.ReqID != opening.ReqID {
		t.Errorf("refresh %+v addresses a different bubble than the opening frame", refresh)
	}
	if !strings.Contains(refresh.Content, "正在查日历") {
		t.Errorf("content = %q, want the progress line in it", refresh.Content)
	}
	if !strings.Contains(refresh.Content, "<think>") {
		t.Errorf("content = %q, want the line rendered as thinking rather than as the answer", refresh.Content)
	}
	if _, ok := rig.streams.peek(rig.session); !ok {
		t.Error("the handle must survive a refresh; the answer still has to land")
	}
}

// TestAnswerStillLandsAfterARefresh — a refresh must not consume the bubble it
// was written into.
func TestAnswerStillLandsAfterARefresh(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "明天上午有空"))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 3 {
		t.Fatalf("want opening + refresh + answer, got %d frames", len(frames))
	}
	if !frames[2].Finish || frames[2].Content != "明天上午有空" {
		t.Errorf("last frame = %+v, want the sealed answer", frames[2])
	}
	if got := len(framesOf(&rig.conn.recordingConn, cmdSendMsg)); got != 0 {
		t.Errorf("sent %d separate messages; the answer belonged in the bubble", got)
	}
}

// TestProgressYieldsToAnUnackedFrame — the backpressure rule. A refresh whose
// predecessor is still in flight is dropped rather than queued: progress is
// worth less than the answer behind it.
func TestProgressYieldsToAnUnackedFrame(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	sender := rig.senders.get(rig.inst.ID)

	// Stand in for a frame still waiting on its ack.
	if _, ok := sender.awaitAck("REQ-42", false); !ok {
		t.Fatal("could not occupy the ack slot")
	}
	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Fatalf("wrote %d frames, want the refresh to have yielded", got)
	}
}

// TestTheAnswerNeverYieldsToAPendingRefresh — the other half of that rule. A
// refresh that never gets its ack must not be able to hold the answer back.
func TestTheAnswerNeverYieldsToAPendingRefresh(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	sender := rig.senders.get(rig.inst.ID)
	if _, ok := sender.awaitAck("REQ-42", false); !ok {
		t.Fatal("could not occupy the ack slot")
	}

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish || frames[1].Content != "答案是 42" {
		t.Fatalf("the answer did not go out past the stalled refresh: %+v", frames)
	}
}

// TestProgressWithoutABubbleIsDropped — progress fires for every task on the
// bus, most of which have no WeCom bubble to write into.
func TestProgressWithoutABubbleIsDropped(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 0 {
		t.Fatalf("wrote %d frames with no bubble open", got)
	}
}

// TestProgressFromTheBusFindsTheSession — task:progress carries a task id and
// no chat session, so the subscriber has to read the session back.
func TestProgressFromTheBusFindsTheSession(t *testing.T) {
	rig := newStreamRig(t)
	tasks := &fakeTasks{session: rig.session}
	rig.typing.tasks = tasks
	rig.ingest(t, "REQ-42")

	bus := events.New()
	rig.typing.Register(bus)
	bus.Publish(events.Event{
		Type:    protocol.EventTaskProgress,
		TaskID:  uuidText(uuidOf(33)),
		Payload: protocol.TaskProgressPayload{TaskID: uuidText(uuidOf(33)), Summary: "Launching claude"},
	})

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || frames[1].Finish {
		t.Fatalf("want an open refresh frame, got %+v", frames)
	}
	if !strings.Contains(frames[1].Content, "Launching claude") {
		t.Errorf("content = %q, want the run's own progress line", frames[1].Content)
	}
}

// TestProgressSkipsTheLookupWithNoBubblesOpen — this subscriber sees every
// task's progress on a shared bus. On a deployment with no WeCom traffic in
// flight it must not put a database read behind each one.
func TestProgressSkipsTheLookupWithNoBubblesOpen(t *testing.T) {
	rig := newStreamRig(t)
	tasks := &fakeTasks{session: rig.session}
	rig.typing.tasks = tasks

	bus := events.New()
	rig.typing.Register(bus)
	bus.Publish(events.Event{
		Type:    protocol.EventTaskProgress,
		TaskID:  uuidText(uuidOf(33)),
		Payload: protocol.TaskProgressPayload{Summary: "Launching claude"},
	})

	if tasks.count() != 0 {
		t.Errorf("looked the task up %d times with no bubble anywhere", tasks.count())
	}
}

// ---- the run's own work, played into the bubble ----

// chatTaskID / issueTaskID stand in for the run behind an event. Real task
// ids are uuids and the subscriber parses them before it will read a row.
func chatTaskID() string  { return uuidText(uuidOf(33)) }
func issueTaskID() string { return uuidText(uuidOf(34)) }

// taskMessageEvent is what POST /api/daemon/tasks/{id}/messages publishes,
// once per message in the batch.
func taskMessageEvent(taskID string, msg protocol.TaskMessagePayload) events.Event {
	msg.TaskID = taskID
	return events.Event{Type: protocol.EventTaskMessage, TaskID: taskID, Payload: msg}
}

// busRig is a rig whose typing manager is subscribed and whose clock the test
// drives, so the throttle can be exercised without sleeping.
func busRig(t *testing.T) (*streamRig, *events.Bus, *fakeTasks, *testClock) {
	t.Helper()
	rig := newStreamRig(t)
	clock := newTestClock()
	rig.streams.now = clock.now
	tasks := &fakeTasks{session: rig.session}
	rig.typing.tasks = tasks
	bus := events.New()
	rig.typing.Register(bus)
	return rig, bus, tasks, clock
}

// TestToolCallsShowUpInTheBubble is the change in one test: what the agent is
// actually doing reaches the user while it is doing it.
func TestToolCallsShowUpInTheBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")
	opening := streamViews(t, &rig.conn.recordingConn)[0]

	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "/srv/app/config.go"})))
	clock.advance(progressMinInterval)
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Bash", map[string]any{"command": "go test ./..."})))

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 3 {
		t.Fatalf("want opening + two refreshes, got %d: %+v", len(frames), frames)
	}
	last := frames[2]
	if last.Finish {
		t.Error("a progress refresh must leave the bubble open")
	}
	if last.ID != opening.ID || last.ReqID != opening.ReqID {
		t.Errorf("refresh %+v addresses a different bubble than the question opened", last)
	}
	if !strings.Contains(last.Content, "正在读取 config.go") || !strings.Contains(last.Content, "正在执行 go 命令") {
		t.Errorf("content = %q, want both steps", last.Content)
	}
	if strings.Contains(last.Content, "go test ./...") {
		t.Errorf("content = %q leaked the command", last.Content)
	}
}

// TestNothingIsLookedUpWithNoBubbleOpen — this subscriber sees every task
// message on a shared bus. On a deployment with no WeCom turn in flight it
// must not put a database read behind each one.
func TestNothingIsLookedUpWithNoBubbleOpen(t *testing.T) {
	_, bus, tasks, _ := busRig(t)

	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))

	if tasks.count() != 0 {
		t.Errorf("looked the task up %d times with no bubble anywhere", tasks.count())
	}
}

// TestTheSessionIsLookedUpOnce — task:message carries a task id and no chat
// session, and a single run posts dozens of them.
func TestTheSessionIsLookedUpOnce(t *testing.T) {
	rig, bus, tasks, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	for i := 0; i < 6; i++ {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Grep", map[string]any{"pattern": "x"})))
		clock.advance(progressMinInterval)
	}
	if tasks.count() != 1 {
		t.Errorf("read the task row %d times for one run", tasks.count())
	}
}

// TestNonChatRunsNeverTouchTheBubble — issue and autopilot runs publish the
// same events and have no chat session. The absence has to be remembered too,
// or every one of their messages re-asks the database.
func TestNonChatRunsNeverTouchTheBubble(t *testing.T) {
	rig, bus, tasks, clock := busRig(t)
	tasks.session = pgtype.UUID{} // an issue run
	rig.ingest(t, "REQ-42")

	for i := 0; i < 5; i++ {
		bus.Publish(taskMessageEvent(issueTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))
		clock.advance(progressMinInterval)
	}
	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Errorf("wrote %d frames; an issue run must not touch somebody else's bubble", got)
	}
	if tasks.count() != 1 {
		t.Errorf("read the task row %d times for a run already known to have no session", tasks.count())
	}
}

// TestABurstOfToolCallsSendsOneFrame — tool calls land several a second and
// every frame is a write on the bot's single socket.
func TestABurstOfToolCallsSendsOneFrame(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	for i := 0; i < 8; i++ {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": string(rune('a'+i)) + ".go"})))
		clock.advance(100 * time.Millisecond)
	}

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("wrote %d frames for one burst, want the opening frame plus a single refresh", len(frames))
	}
	clock.advance(progressMinInterval)
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Bash", map[string]any{"command": "ls"})))
	frames = streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 3 {
		t.Fatalf("the throttle never released: %d frames", len(frames))
	}
	if !strings.Contains(frames[2].Content, "h.go") {
		t.Errorf("content = %q, want the throttled steps folded in rather than lost", frames[2].Content)
	}
}

// TestResultsAndProseDoNotRefresh — half the volume on this event is tool
// output and the agent's own text. Neither belongs in the spinner, and
// rejecting them before the lookup is what keeps the cost near zero.
func TestResultsAndProseDoNotRefresh(t *testing.T) {
	rig, bus, tasks, _ := busRig(t)
	rig.ingest(t, "REQ-42")

	bus.Publish(taskMessageEvent(chatTaskID(), protocol.TaskMessagePayload{Type: "tool_result", Tool: "Bash", Output: "sk-live-42"}))
	bus.Publish(taskMessageEvent(chatTaskID(), protocol.TaskMessagePayload{Type: "text", Content: "the answer is 42"}))

	if got := len(streamViews(t, &rig.conn.recordingConn)); got != 1 {
		t.Errorf("wrote %d frames; only tool calls and errors are progress", got)
	}
	if tasks.count() != 0 {
		t.Errorf("looked the task up %d times for messages that can never refresh anything", tasks.count())
	}
}

// TestTheAnswerIsNeverThrottled — the floor between refreshes must not be able
// to hold the answer back, however soon after a refresh it arrives.
func TestTheAnswerIsNeverThrottled(t *testing.T) {
	rig, bus, _, _ := busRig(t)
	rig.ingest(t, "REQ-42")
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))

	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	frames := streamViews(t, &rig.conn.recordingConn)
	last := frames[len(frames)-1]
	if !last.Finish || last.Content != "答案是 42" {
		t.Fatalf("last frame = %+v, want the sealed answer", last)
	}
}

// ---- the answer is the last word ----

// TestARefreshThatLostTheRaceNeverReachesTheBubble is the failure this fence
// exists for. The subscriber reads the handle out of the store, renders, and
// then writes; the answer can take that same handle in between. A frame landing
// behind the closing one would paint "正在读取 x.go" over the answer and leave a
// bubble nothing can close.
func TestARefreshThatLostTheRaceNeverReachesTheBubble(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")

	// What a refresh already in flight is holding when the answer arrives.
	stale, ok := rig.streams.peek(rig.session)
	if !ok {
		t.Fatal("no bubble to race for")
	}
	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	err := rig.senders.stream(context.Background(), stale, "<think>正在读取 x.go</think>", false)
	if !errors.Is(err, errStreamSuperseded) {
		t.Errorf("straggling refresh returned %v, want it refused as superseded", err)
	}
	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("want opening + answer, got %d frames: %+v", len(frames), frames)
	}
	if last := frames[1]; !last.Finish || last.Content != "答案是 42" {
		t.Errorf("last frame = %+v, want the sealed answer and nothing after it", last)
	}
}

// TestAVerdictBelongsToTheFrameThatEarnedIt — an ack carries the req_id and
// nothing else, and a turn now writes a dozen frames under one. A refresh whose
// caller gave up still has a verdict coming; handing that verdict to the
// closing frame is how a refused answer reads as delivered and is never sent at
// all.
func TestAVerdictBelongsToTheFrameThatEarnedIt(t *testing.T) {
	conn := &recordingConn{}
	sender := newWSSender(conn, testLogger())

	// A refresh whose caller stops waiting — the mid-run budget is short on
	// purpose, and the server answers later anyway.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := sender.respondStream(ctx, "REQ-42", "S-1", "<think>正在读取</think>", false); !errors.Is(err, errStreamAckTimeout) {
		t.Fatalf("refresh returned %v, want the caller to have given up on it", err)
	}
	cancel()

	answered := make(chan error, 1)
	go func() {
		answered <- sender.respondStream(context.Background(), "REQ-42", "S-1", "答案是 42", true)
	}()
	waitForStreamFrames(t, conn, 2)

	// The refresh's verdict, arriving late and refusing the frame.
	sender.deliverAck("REQ-42", errcodeStreamExpired, "stream expired")
	select {
	case err := <-answered:
		t.Fatalf("the answer took the refresh's verdict: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	sender.deliverAck("REQ-42", 0, "")
	if err := <-answered; err != nil {
		t.Errorf("the answer's own verdict said ok, got %v", err)
	}
}

// TestALateVerdictIsNotHandedToTheNextFrame — the mid-run budget is shorter
// than the server's worst case, so a refresh's verdict routinely lands after
// its caller has stopped waiting. Whoever wrote next must not be given it: a
// closing frame that reads a stale "accepted" returns success, the caller has
// no reason to fall back, and the answer is never sent anywhere.
func TestALateVerdictIsNotHandedToTheNextFrame(t *testing.T) {
	conn := &recordingConn{}
	sender := newWSSender(conn, testLogger())
	sender.ackTimeout = 20 * time.Millisecond

	if err := sender.respondStream(context.Background(), "REQ-42", "S-1", "<think>正在读取</think>", false); !errors.Is(err, errStreamAckTimeout) {
		t.Fatalf("refresh returned %v, want the wait given up on", err)
	}
	sender.ackTimeout = 2 * time.Second // the answer waits it out properly

	answered := make(chan error, 1)
	go func() {
		answered <- sender.respondStream(context.Background(), "REQ-42", "S-1", "答案是 42", true)
	}()
	waitForStreamFrames(t, conn, 2)

	// The refresh's verdict, arriving after nobody is waiting for it.
	sender.deliverAck("REQ-42", 0, "")
	select {
	case err := <-answered:
		t.Fatalf("the closing frame took the refresh's verdict: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Its own verdict — a refusal, which is what sends the answer out as a
	// new message instead of losing it.
	sender.deliverAck("REQ-42", errcodeStreamExpired, "stream expired")
	if err := <-answered; !streamUnusable(err) {
		t.Fatalf("the closing frame's verdict was %v, want the server's refusal", err)
	}
}

// TestAVerdictThatNeverComesCostsTheBubbleNotTheAnswer — the other half. When
// the verdict really is lost the turn cannot be put back in step, so every
// later frame times out too. That costs the bubble and nothing else: an ack
// timeout is exactly what tells the caller to send a plain message.
func TestAVerdictThatNeverComesCostsTheBubbleNotTheAnswer(t *testing.T) {
	conn := &recordingConn{}
	sender := newWSSender(conn, testLogger())
	sender.ackTimeout = 20 * time.Millisecond

	_ = sender.respondStream(context.Background(), "REQ-42", "S-1", "<think>正在读取</think>", false)

	err := sender.respondStream(context.Background(), "REQ-42", "S-1", "答案是 42", true)
	if !errors.Is(err, errStreamAckTimeout) {
		t.Fatalf("closing frame returned %v, want the caller told to send a new message", err)
	}
}

// waitForStreamFrames blocks until the connection has recorded n
// aibot_respond_msg frames, so a test can act on a frame written by another
// goroutine without sleeping for a fixed time.
func waitForStreamFrames(t *testing.T, c *recordingConn, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(framesOf(c, cmdRespondMsg)) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d stream frames were written, want %d", len(framesOf(c, cmdRespondMsg)), n)
}

// TestABubbleTheServerDisownsIsLetGo — 846608 means this stream will never take
// another frame. Holding the handle would buy a refusal every 1.5s for the rest
// of the run and then spend the answer's own ack timeout learning the same
// thing.
func TestABubbleTheServerDisownsIsLetGo(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")

	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	if _, ok := rig.streams.peek(rig.session); ok {
		t.Error("kept a handle the server has disowned")
	}
	out := NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger())
	out.handleEvent(chatDoneEvent(rig.session, "答案是 42"))
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 2 || got[1] != "答案是 42" {
		t.Errorf("plain messages = %v, want the notice and then the answer", got)
	}
}

// TestADisownedBubbleIsExplained — 846608 leaves a spinner nothing can stop:
// the stream will take no further frame, and letting the handle go stops the
// guard that would otherwise have closed it. Silence on top of that is a
// loading animation that runs forever with no account of itself anywhere.
func TestADisownedBubbleIsExplained(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")

	rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamStuck {
		t.Fatalf("the user was told %v about a spinner that will never stop", got)
	}
}

// TestTheExplanationIsSaidOnce — the handle goes with it, so a run with fifty
// tool calls left must not narrate the same bad news fifty times.
func TestTheExplanationIsSaidOnce(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42")
	rig.conn.rejectWith(errcodeStreamExpired, "stream expired")

	for i := 0; i < 5; i++ {
		rig.typing.UpdateProgress(context.Background(), rig.session, "正在查日历")
	}

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 {
		t.Fatalf("said it %d times: %v", len(got), got)
	}
}

// TestATaskThatIsGoneIsAskedAboutOnce — a run cancelled and deleted mid-flush
// keeps publishing messages for a row that is not there. The absence is an
// answer and has to be remembered like any other.
func TestATaskThatIsGoneIsAskedAboutOnce(t *testing.T) {
	rig, bus, tasks, clock := busRig(t)
	tasks.err = pgx.ErrNoRows
	rig.ingest(t, "REQ-42")

	for i := 0; i < 4; i++ {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))
		clock.advance(progressMinInterval)
	}
	if tasks.count() != 1 {
		t.Errorf("read the missing task row %d times", tasks.count())
	}
}

// TestLastTurnsProgressNeverPaintsTheNewBubble — the store is keyed on the chat
// session and the feed knows nothing about runs, so a transcript still flushing
// from the previous turn resolves to the same session and lands in the bubble
// the user's NEXT question opened. The steps are real; they are just the wrong
// run's, and the user reads them as what is happening now.
func TestLastTurnsProgressNeverPaintsTheNewBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	// This turn's run takes the bubble.
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "this-turn.go"})))
	clock.advance(progressMinInterval)

	// The previous turn's run, still flushing, resolving to the same session.
	bus.Publish(taskMessageEvent(issueTaskID(), toolUse("Read", map[string]any{"file_path": "last-turn.go"})))

	for _, f := range streamViews(t, &rig.conn.recordingConn) {
		if strings.Contains(f.Content, "last-turn.go") {
			t.Fatalf("a step from another run reached this turn's bubble: %q", f.Content)
		}
	}
}

// TestTheFirstRunToSpeakOwnsTheBubble — the flip side. Whichever run gets there
// first is the one the bubble belongs to, and it must keep getting through.
func TestTheFirstRunToSpeakOwnsTheBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)
	rig.ingest(t, "REQ-42")

	for i, name := range []string{"one.go", "two.go", "three.go"} {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": name})))
		if i < 2 {
			clock.advance(progressMinInterval)
		}
	}

	frames := streamViews(t, &rig.conn.recordingConn)
	last := frames[len(frames)-1]
	if !strings.Contains(last.Content, "one.go") || !strings.Contains(last.Content, "two.go") {
		t.Fatalf("the owning run's own steps stopped getting through: %q", last.Content)
	}
}

// TestASweptTaskStillClosesTheBubble — task:failed has two publishers with
// different payloads. broadcastTaskEvent (FailTask) carries chat_session_id;
// HandleFailedTasks — the sweepers, recover-orphans, the heartbeat timeout —
// carries task_id and nothing else. That second one is the whole crashed-daemon
// path, and it left the bubble spinning until the five-minute guard replaced it
// with "still working, I'll reply separately" for a run that died long ago.
func TestASweptTaskStillClosesTheBubble(t *testing.T) {
	rig, bus, _, _ := busRig(t)
	rig.ingest(t, "REQ-42")

	bus.Publish(events.Event{
		Type: protocol.EventTaskFailed,
		Payload: map[string]any{
			"task_id":        chatTaskID(),
			"status":         "failed",
			"failure_reason": "daemon heartbeat timeout",
		},
	})

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("want the bubble closed, got %+v", frames)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamFailed {
		t.Errorf("closing content = %q, want the failure copy", frames[1].Content)
	}
}

// TestAFailedTaskCostsNothingWithNoBubbleOpen — task:failed fires for every
// issue and autopilot run in the workspace. Reading a task row for each of them
// on a deployment with no WeCom turn in flight is the cost the two progress
// subscribers already refuse to pay.
func TestAFailedTaskCostsNothingWithNoBubbleOpen(t *testing.T) {
	_, bus, tasks, _ := busRig(t)

	bus.Publish(events.Event{
		Type:    protocol.EventTaskFailed,
		Payload: map[string]any{"task_id": issueTaskID(), "status": "failed"},
	})

	if tasks.count() != 0 {
		t.Errorf("looked the task up %d times with no bubble anywhere", tasks.count())
	}
}

// TestALookupThatFailedIsNotRetriedPerMessage — the read that failed is the
// one worth caching most. A slow or flapping database is exactly when a batch
// of transcript messages costs one round trip each, on the daemon's own
// request, and that is how a spinner takes the transcript down with it.
func TestALookupThatFailedIsNotRetriedPerMessage(t *testing.T) {
	rig, bus, tasks, clock := busRig(t)
	rig.typing.taskSessions.now = clock.now
	tasks.err = errors.New("connection reset by peer")
	rig.ingest(t, "REQ-42")

	for i := 0; i < 8; i++ {
		bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))
	}

	if tasks.count() != 1 {
		t.Fatalf("read the task row %d times for one batch of messages", tasks.count())
	}
}

// TestAFailedLookupIsForgottenQuickly — the other side of it. Unlike a task
// with no session, a failure is not an answer, so it may only be remembered
// long enough to cover the batch that provoked it.
func TestAFailedLookupIsForgottenQuickly(t *testing.T) {
	rig, bus, tasks, clock := busRig(t)
	rig.typing.taskSessions.now = clock.now
	tasks.err = errors.New("connection reset by peer")
	rig.ingest(t, "REQ-42")

	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))
	clock.advance(taskSessionFailTTL + time.Second)
	tasks.err = nil
	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "y.go"})))

	if tasks.count() != 2 {
		t.Fatalf("asked %d times; a database that came back has to be asked again", tasks.count())
	}
	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !strings.Contains(frames[1].Content, "y.go") {
		t.Fatalf("the bubble never got the step the recovered lookup found: %+v", frames)
	}
}

// TestProgressSubscribersAreRaceFree — task:message is published from the
// daemon's HTTP handler, chat:done from another, and the guard timer from a
// third. Run under -race.
func TestProgressSubscribersAreRaceFree(t *testing.T) {
	rig, bus, _, _ := busRig(t)
	out := newOutboundUnder(rig)
	rig.ingest(t, "REQ-42")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			bus.Publish(taskMessageEvent(chatTaskID(), toolUse("Read", map[string]any{"file_path": "x.go"})))
		}()
		go func() {
			defer wg.Done()
			bus.Publish(taskMessageEvent(chatTaskID(), protocol.TaskMessagePayload{Type: "error", Content: "boom"}))
		}()
	}
	wg.Wait()
	out.handleEvent(chatDoneEvent(rig.session, "答案"))
}
