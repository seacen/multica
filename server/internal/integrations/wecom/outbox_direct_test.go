package wecom

// outbox_direct_test.go — the reply the user reads twice.
//
// The outbound queue's reconciler compensates for a replica that died between
// finishing a task and enqueueing its reply: it scans terminal tasks, and any
// one with no queue row is treated as a reply that never went out. That is
// sound while every send is an enqueue. It stopped being sound the moment this
// adapter grew a bubble — a stream frame addressed by the req_id of the
// callback that opened the turn, which only the connection that received that
// callback can write, and which therefore never becomes a row. The user read
// the answer in the bubble and read it again about a minute later as an
// ordinary message, with the backend logging a rescue for a reply nothing had
// dropped.
//
// So these tests are about a row nobody will ever deliver: the record a socket
// delivery leaves so the reconciler can tell it happened.

import (
	"slices"
	"testing"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel/outbox"
)

// failureRecordingRig is the bubble rig with the two dependencies the failure
// notice needs to file itself: a producer to write the record with, and the
// binding lookup that turns a session into the row's address.
//
// A rig of its own rather than a change to newBubbleRig, because handing every
// bubble test a Bindings lookup would change what sayTheRunFailed does when no
// round is on file — it would start finding an address and speaking where those
// tests expect silence.
func failureRecordingRig(t *testing.T) *bubbleRig {
	t.Helper()
	rig := newBubbleRig(t)
	producer, err := outbox.NewProducer(channelTypeWecom, rig.queue, nil, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	rig.typing = NewTypingIndicator(TypingIndicatorConfig{
		Senders:    rig.senders,
		Streams:    rig.streams,
		Tasks:      rig.q,
		Bindings:   rig.q,
		Producer:   producer,
		GuardAfter: -1,
	})
	rig.bus = events.New()
	rig.typing.Register(rig.bus)
	return rig
}

// reconcileSourceKinds is what the candidate scan filters on. Asserting a
// recorded key against this list rather than against a literal is the point:
// a record written under a kind the scan does not look for suppresses nothing,
// and looks identical from the call site.
func reconcileSourceKinds() []string { return (&reconcileBuilder{}).SourceKinds() }

// TestAnAnswerDeliveredInTheBubbleIsRecordedOnTheQueue is the duplicate the
// user reported, stated as the thing that has to be true for it not to happen.
//
// The assertion is deliberately not "a row was written": the bubble path must
// NOT enqueue, or the answer really would be sent twice. It is that the queue
// was told the delivery happened, under exactly the business key the
// reconciler's candidate scan looks for — installation, one of its source
// kinds, and the task id as source id.
func TestAnAnswerDeliveredInTheBubbleIsRecordedOnTheQueue(t *testing.T) {
	t.Parallel()
	rig := newBubbleRig(t)
	rig.ran(t, "REQ-D1", 1, "task-1")
	rig.answer(t, "毛利率是 42.1%", "task-1")

	if n := len(rig.queue.rows); n != 0 {
		t.Fatalf("the bubble answer also enqueued %d row(s); it would be delivered a second time as an ordinary message", n)
	}
	if len(rig.queue.delivered) != 1 {
		t.Fatalf("the bubble delivered the answer and recorded %d deliveries, want 1: with no record the reconciler finds a completed task with no queue row, concludes the realtime path dropped the reply, and sends the same answer again about a minute later",
			len(rig.queue.delivered))
	}

	got := rig.queue.delivered[0]
	if got.SourceID != taskUUID(t, "task-1") {
		t.Errorf("recorded source_id = %q, want the task id %q — the candidate scan matches on the task id, so any other value records nothing it will look for",
			got.SourceID, taskUUID(t, "task-1"))
	}
	if !slices.Contains(reconcileSourceKinds(), got.SourceKind) {
		t.Errorf("recorded source_kind = %q, which is not one the reconciler scans for (%v); the record suppresses nothing",
			got.SourceKind, reconcileSourceKinds())
	}
	if got.InstallationID != rig.instID {
		t.Errorf("recorded installation = %v, want %v — the business key is scoped to the installation",
			got.InstallationID, rig.instID)
	}
	if got.ChannelType != channelTypeWecom {
		t.Errorf("recorded channel_type = %q, want %q", got.ChannelType, channelTypeWecom)
	}
}

// TestALongAnswerSplitUnderTheBubbleIsRecordedOnce: the head goes in the
// bubble and sendRest puts the remainder underneath it, both over the socket
// and neither as a row. The reconciler's unit is the task, so one record
// covers the whole turn — and the turn must not be left unrecorded just
// because it did not fit in one frame.
func TestALongAnswerSplitUnderTheBubbleIsRecordedOnce(t *testing.T) {
	t.Parallel()
	rig := newBubbleRig(t)
	rig.ran(t, "REQ-D2", 1, "task-1")
	rig.answer(t, aLongAnswer(), "task-1")

	if len(rig.queue.rows) != 0 {
		t.Fatalf("a split answer enqueued %d row(s); the pieces go out over the socket", len(rig.queue.rows))
	}
	if !rig.queue.deliveredFor(sourceKindChatDone, taskUUID(t, "task-1")) {
		t.Fatal("a long answer split under the bubble recorded no delivery; the reconciler re-sends the whole answer, unsplit, on top of the one already on screen")
	}
	if n := len(rig.queue.delivered); n != 1 {
		t.Errorf("recorded %d deliveries for one turn, want 1", n)
	}
}

// TestAFallBackAnswerIsEnqueuedRatherThanRecorded is the other direction, and
// the reason this cannot be a blanket record at the top of the handler: when
// the closing frame is refused the answer has NOT been delivered, and the queue
// is what delivers it. Recording there would suppress the reconciler for a
// reply that never went anywhere.
func TestAFallBackAnswerIsEnqueuedRatherThanRecorded(t *testing.T) {
	t.Parallel()
	rig := newBubbleRig(t)
	rig.conn.refuseClosingCode = errcodeStreamExpired
	rig.ran(t, "REQ-D3", 1, "task-1")
	rig.answer(t, "the agent reply", "task-1")

	if len(rig.queue.delivered) != 0 {
		t.Fatalf("a refused bubble recorded %d delivery/deliveries; nothing was delivered over the socket and the queue is the only thing that will send it",
			len(rig.queue.delivered))
	}
}

// TestAFailedRunsNoticeIsRecordedOnTheQueue is the same defect on the other
// ending. Nothing enqueues task_failed in the realtime path — the notice is
// written straight into the bubble — so a failed run leaves the queue exactly
// the empty-handed scan a dropped reply leaves, and the reconciler delivers its
// own notice underneath the one already on screen.
func TestAFailedRunsNoticeIsRecordedOnTheQueue(t *testing.T) {
	t.Parallel()
	rig := failureRecordingRig(t)
	rig.ran(t, "REQ-D4", 1, "task-1")
	rig.failed(t, "task-1", false)

	if !rig.queue.deliveredFor(sourceKindTaskFailed, taskUUID(t, "task-1")) {
		t.Fatalf("the bubble said the run failed and recorded nothing: the reconciler builds its own task_failed row for the same task and the user is told twice, in two different sets of words. recorded: %v",
			rig.queue.delivered)
	}
	if !slices.Contains(reconcileSourceKinds(), sourceKindTaskFailed) {
		t.Fatalf("task_failed is not among the kinds the reconciler scans for (%v); this test is asserting against the wrong key", reconcileSourceKinds())
	}
}

// TestARunTheAdapterRefusesToAnnounceRecordsNothing keeps the record honest
// about what it means. A run whose origin cannot be established is not this
// adapter's to announce — it says nothing, and the reconciler applies the same
// channel-ingested gate before it delivers anything. Recording a delivery
// there would claim a message that was never written.
func TestARunTheAdapterRefusesToAnnounceRecordsNothing(t *testing.T) {
	t.Parallel()
	rig := failureRecordingRig(t)
	// No round on file: the run was started in a browser on a session that
	// happens to be bound to a WeCom chat, which is the one shape where this
	// adapter has to read the origin off the task row and refuses.
	rig.askedInTheBrowser(t, "task-1")
	rig.failed(t, "task-1", false)

	if len(rig.queue.delivered) != 0 {
		t.Fatalf("a run this adapter refused to announce recorded %d delivery/deliveries: the record says a message reached the user, and none did",
			len(rig.queue.delivered))
	}
}

// TestRecordedDeliveriesCarryNoPayload: the row is a record that something
// went out, not a copy of it. The body was rendered and written by the path
// that delivered it, and a queue row that outlives the turn is the wrong place
// to keep a user's words.
func TestRecordedDeliveriesCarryNoPayload(t *testing.T) {
	t.Parallel()
	rig := newBubbleRig(t)
	rig.ran(t, "REQ-D6", 1, "task-1")
	rig.answer(t, "毛利率是 42.1%", "task-1")

	if len(rig.queue.delivered) != 1 {
		t.Fatalf("recorded %d deliveries, want 1", len(rig.queue.delivered))
	}
	// RecordChannelOutboundDeliveredParams has no payload field at all, which
	// is the guarantee. Assert the addressing that IS on it instead.
	if got := rig.queue.delivered[0].TargetChatID; got != "CHAT_1" {
		t.Errorf("recorded target chat = %q, want the bound chat", got)
	}
	if got := rig.queue.delivered[0].WorkspaceID; got != fakeWorkspaceUUID {
		t.Errorf("recorded workspace = %v, want the installation's %v", got, fakeWorkspaceUUID)
	}
}
