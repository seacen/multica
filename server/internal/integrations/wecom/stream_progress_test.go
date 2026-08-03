package wecom

// stream_progress_test.go — the mid-run refresh. Same bubble, new words, and
// none of it allowed to get in the way of the answer.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// fakeTasks answers the one lookup the progress subscriber makes: which chat
// session does this task belong to.
type fakeTasks struct {
	session pgtype.UUID
	err     error
	calls   int
}

func (f *fakeTasks) GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error) {
	f.calls++
	if f.err != nil {
		return db.AgentTaskQueue{}, f.err
	}
	return db.AgentTaskQueue{ChatSessionID: f.session}, nil
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

	if tasks.calls != 0 {
		t.Errorf("looked the task up %d times with no bubble anywhere", tasks.calls)
	}
}
