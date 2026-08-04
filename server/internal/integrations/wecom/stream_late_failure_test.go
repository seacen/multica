package wecom

// stream_late_failure_test.go — the run that fails after its bubble is gone.
//
// A bubble is consumed by whoever ends the round first, and the five-minute
// guard is allowed to be that one: it writes "still working, I'll reply
// separately" and hands the handle back to nobody. The run keeps going. When it
// then fails, task:failed arrives at a session with no handle left — and the
// promise the guard made is the whole reason the failure has to reach the user
// anyway.
//
// Two addresses answer that. The round's own, remembered from the handle the
// guard took, which is the chat that asked even if the binding has since been
// re-pointed. And the binding row, for a round this process never saw open —
// a restart mid-run, or a turn whose opening frame the server refused.
//
// The other half of the file is the silence that has to stay silent: a round
// whose answer landed owes nothing, and neither does one whose failure has
// already been said.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// ---- scaffolding ----

// fakeBindings answers the two lookups a failure notice makes when it has no
// handle to address itself with, and counts them: a round the store still
// remembers must not buy a database read.
type fakeBindings struct {
	mu         sync.Mutex
	binding    db.ChannelChatSessionBinding
	bindingErr error
	install    db.ChannelInstallation
	installErr error
	calls      int
}

func (f *fakeBindings) GetChannelChatSessionBindingBySession(context.Context, db.GetChannelChatSessionBindingBySessionParams) (db.ChannelChatSessionBinding, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.binding, f.bindingErr
}

func (f *fakeBindings) GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	return f.install, f.installErr
}

func (f *fakeBindings) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// boundElsewhere is a binding row pointing at a different chat than the one the
// question came from, so a test can tell which address a notice used.
func boundElsewhere(instID pgtype.UUID) *fakeBindings {
	return &fakeBindings{
		binding: db.ChannelChatSessionBinding{
			InstallationID: instID,
			ChannelChatID:  "T-somewhere-else",
			ChatType:       "p2p",
		},
		install: db.ChannelInstallation{ID: instID, Status: string(InstallationActive)},
	}
}

// waitForFrames blocks until the connection has recorded n stream frames, so a
// test can act on the guard's own goroutine without sleeping a fixed time.
func waitForFrames(t *testing.T, rig *streamRig, n int) []streamView {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := streamViews(t, &rig.conn.recordingConn); len(got) >= n {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	got := streamViews(t, &rig.conn.recordingConn)
	t.Fatalf("wrote %d frames, want %d: %+v", len(got), n, got)
	return nil
}

// guarded puts a rig in the state this file is about: a bubble opened, the
// guard already fired and taken the handle with it, and the run still going.
func guarded(t *testing.T, rig *streamRig) {
	t.Helper()
	rig.typing.guardAfter = 20 * time.Millisecond
	rig.ingest(t, "REQ-42")

	frames := waitForFrames(t, rig, 2)
	if frames[1].Content != copyPacks[LocaleZhHans].StreamStillWorking {
		t.Fatalf("setup: the guard said %q, want the still-working copy", frames[1].Content)
	}
	if depth := rig.streams.depth(); depth != 0 {
		t.Fatalf("setup: store depth %d, want the guard to have taken the handle", depth)
	}
}

// ---- the promise the guard made ----

// TestAFailureAfterTheGuardStillReachesTheUser is the hole this file was
// written for. The guard closes the bubble at five minutes and promises a
// separate reply; the run then fails, and task:failed used to find no handle
// and return without a word. Any run longer than five minutes that then failed
// left the user with a promise and nothing else.
func TestAFailureAfterTheGuardStillReachesTheUser(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	guarded(t, rig)

	failTheRun(rig)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("the user received %v, want the failure as a plain message", got)
	}
	if frames := len(streamViews(t, &rig.conn.recordingConn)); frames != 2 {
		t.Errorf("wrote %d frames; the bubble was already sealed", frames)
	}
}

// TestTheLateFailureSpeaksInTheChatThatAsked — the notice belongs where the
// question was asked, and by now the binding row may point somewhere else. The
// addressing captured at ingest is what makes that safe, and a round the store
// still remembers must not pay a database read to rediscover it.
func TestTheLateFailureSpeaksInTheChatThatAsked(t *testing.T) {
	rig := newStreamRig(t)
	bindings := boundElsewhere(rig.inst.ID)
	rig.typing.bindings = bindings
	guarded(t, rig)
	rig.senders.clear(rig.inst.ID)

	failTheRun(rig)

	msg, ok := rig.senders.pending.pop(rig.inst.ID)
	if !ok {
		t.Fatal("nothing held for the next connection")
	}
	if msg.ChatID != rig.principalSender || msg.ChatType != chatTypeSingleInt {
		t.Errorf("the notice was addressed to %q/%d, want the chat that asked", msg.ChatID, msg.ChatType)
	}
	if n := bindings.count(); n != 0 {
		t.Errorf("read the binding %d times for a round the store still remembers", n)
	}
}

// TestTheLateFailureWaitsForTheNextConnection — the same notice, produced
// during a reconnect window. Nothing about a bubble that is already gone may
// cost the fallback its holding queue.
func TestTheLateFailureWaitsForTheNextConnection(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	guarded(t, rig)
	rig.senders.clear(rig.inst.ID)

	failTheRun(rig)

	if n := rig.senders.pending.depth(rig.inst.ID); n != 1 {
		t.Fatalf("queue depth %d; the failure notice was not held for the next connection", n)
	}
	conn := &recordingConn{}
	rig.senders.set(rig.inst.ID, newWSSender(conn, testLogger()))
	rig.senders.flushPending(rig.inst.ID)

	if got := contentsOf(conn); len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("after the reconnect the user received %v, want the failure notice", got)
	}
}

// TestTheLateFailureIsSaidOnce — task:failed has two publishers and a sweeper
// tick can repeat one. The user is owed the news, not a column of it.
func TestTheLateFailureIsSaidOnce(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	guarded(t, rig)

	failTheRun(rig)
	failTheRun(rig)

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 {
		t.Fatalf("the user was told %d times: %v", len(got), got)
	}
}

// ---- the silence that has to stay silent ----

// TestAnAnsweredRoundGetsNoFailureNotice — the answer took the handle the same
// way the guard does, and the two must not read alike. A task:failed arriving
// behind a delivered answer — an auto-retry's first attempt, a sweeper that ran
// late — has nothing left to tell anyone.
func TestAnAnsweredRoundGetsNoFailureNotice(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	rig.ingest(t, "REQ-42")
	newOutboundUnder(rig).handleEvent(chatDoneEvent(rig.session, "答案是 42"))

	failTheRun(rig)

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v after an answer that landed", got)
	}
	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || frames[1].Content != "答案是 42" {
		t.Fatalf("frames = %+v, want the opening frame and the answer", frames)
	}
}

// TestAnAnswerDeliveredWithoutABubbleAlsoEndsTheRound — the same rule one step
// out. When the bubble is already gone the answer goes out as a plain message,
// and that is just as much the round's ending as a sealed bubble is.
func TestAnAnswerDeliveredWithoutABubbleAlsoEndsTheRound(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	guarded(t, rig)

	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, "答案是 42"))
	failTheRun(rig)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != "答案是 42" {
		t.Fatalf("the user received %v, want the answer and nothing after it", got)
	}
}

// TestTheBubbleStillWinsWhenItIsThere — the path that already worked. A
// failure with the handle still in hand seals the bubble, and looks nothing up
// to do it.
func TestTheBubbleStillWinsWhenItIsThere(t *testing.T) {
	rig := newStreamRig(t)
	bindings := boundElsewhere(rig.inst.ID)
	rig.typing.bindings = bindings
	rig.ingest(t, "REQ-42")

	failTheRun(rig)

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 || !frames[1].Finish {
		t.Fatalf("want a closing frame, got %+v", frames)
	}
	if frames[1].Content != copyPacks[LocaleZhHans].StreamFailed {
		t.Errorf("closing content = %q, want the failure copy", frames[1].Content)
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Errorf("the user also received %v; the bubble said it already", got)
	}
	if n := bindings.count(); n != 0 {
		t.Errorf("read the binding %d times with the handle in hand", n)
	}
}

// ---- the round this process never saw ----

// TestAFailureWithNoBubbleFindsTheChatFromTheBinding — a restart mid-run, or a
// turn whose opening frame the server refused. Nothing is remembered, and the
// binding row is the only address there is. Before this the user's spinner (or
// silence) was the whole account of a run that died.
func TestAFailureWithNoBubbleFindsTheChatFromTheBinding(t *testing.T) {
	rig := newStreamRig(t)
	bindings := boundElsewhere(rig.inst.ID)
	rig.typing.bindings = bindings

	failTheRun(rig)

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("the user received %v, want the failure notice", got)
	}
	msgs := framesOf(&rig.conn.recordingConn, cmdSendMsg)
	if len(msgs) != 1 {
		t.Fatalf("wrote %d plain messages, want one", len(msgs))
	}
	if n := bindings.count(); n != 1 {
		t.Errorf("read the binding %d times, want exactly one", n)
	}
}

// TestAFailureFoundFromTheBindingIsAlsoSaidOnce — the binding path has no
// handle to consume, so nothing about it is self-limiting. It has to remember
// that it spoke.
func TestAFailureFoundFromTheBindingIsAlsoSaidOnce(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)

	failTheRun(rig)
	failTheRun(rig)

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 {
		t.Fatalf("the user was told %d times: %v", len(got), got)
	}
}

// TestAFailureOnANonWecomSessionSaysNothing — this subscriber sees every failed
// run on a shared bus, including Slack's, Lark's and the web UI's. A session
// with no wecom binding is not ours to speak in.
func TestAFailureOnANonWecomSessionSaysNothing(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = &fakeBindings{bindingErr: pgx.ErrNoRows}

	failTheRun(rig)

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v for a session that is not wecom's", got)
	}
}

// TestAFailureOnARevokedInstallationSaysNothing — the binding row outlives the
// revoke, so a session still looks reachable after the bot has been removed.
func TestAFailureOnARevokedInstallationSaysNothing(t *testing.T) {
	rig := newStreamRig(t)
	bindings := boundElsewhere(rig.inst.ID)
	bindings.install.Status = string(InstallationRevoked)
	rig.typing.bindings = bindings

	failTheRun(rig)

	if got := contentsOf(&rig.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v from a revoked installation", got)
	}
}

// TestWithNoLookupConfiguredOnlyTheRememberedRoundsAreSpokenFor — the lookup is
// optional and only the third address depends on it. A round this process
// closed itself is answered out of memory; one it never saw is where a
// deployment wired without the lookup goes quiet.
func TestWithNoLookupConfiguredOnlyTheRememberedRoundsAreSpokenFor(t *testing.T) {
	remembered := newStreamRig(t)
	guarded(t, remembered)
	failTheRun(remembered)

	got := contentsOf(&remembered.conn.recordingConn)
	if len(got) != 1 || got[0] != copyPacks[LocaleZhHans].StreamFailed {
		t.Fatalf("the user received %v for a round the store remembers", got)
	}

	unseen := newStreamRig(t)
	failTheRun(unseen)

	if got := contentsOf(&unseen.conn.recordingConn); len(got) != 0 {
		t.Fatalf("the user received %v for a round with nowhere to look it up", got)
	}
}

// ---- the store's side of it ----

// TestTheStoreRemembersWhereARoundWasSpeaking is the contract in one test.
// After the handle is gone the store still answers "where, and is anything
// owed", once.
func TestTheStoreRemembersWhereARoundWasSpeaking(t *testing.T) {
	store := newStreamStore()
	session := uuidOf(3)
	if _, verdict := store.claimEnding(session); verdict != roundForgotten {
		t.Errorf("verdict %v for a session the store never saw, want roundForgotten", verdict)
	}

	store.open(session, streamHandle{ReqID: "R", StreamID: "S", ChatID: "T-alex", ChatType: chatTypeSingleInt})
	if _, ok := store.takeHead(session, roundContinues); !ok {
		t.Fatal("take refused a handle just claimed")
	}

	addr, verdict := store.claimEnding(session)
	if verdict != roundOwesAnEnding {
		t.Fatalf("verdict %v after the guard closed early, want the ending still owed", verdict)
	}
	if addr.ChatID != "T-alex" || addr.ChatType != chatTypeSingleInt {
		t.Errorf("addressing %+v, want the chat that asked", addr)
	}
	if _, verdict := store.claimEnding(session); verdict != roundToldAlready {
		t.Errorf("verdict %v on the second claim, want the round accounted for", verdict)
	}
}

// TestARoundThatEndedProperlyOwesNothing — the other half of the contract, and
// the one that keeps a delivered answer from growing a failure notice.
func TestARoundThatEndedProperlyOwesNothing(t *testing.T) {
	store := newStreamStore()
	session := uuidOf(3)
	store.open(session, streamHandle{ReqID: "R", StreamID: "S", ChatID: "T-alex"})
	store.takeHead(session, roundOver)

	if _, verdict := store.claimEnding(session); verdict != roundToldAlready {
		t.Errorf("verdict %v after the round ended, want it accounted for", verdict)
	}
}

// TestTheStoreForgetsARoundEventually — the memory outlives the bubble on
// purpose, so it needs its own end. A process that runs for weeks must not keep
// a row per session it ever answered.
func TestTheStoreForgetsARoundEventually(t *testing.T) {
	store := newStreamStore()
	base := time.Now()
	store.now = func() time.Time { return base }
	session := uuidOf(3)
	store.open(session, streamHandle{ReqID: "R", StreamID: "S", ChatID: "T-alex"})
	store.takeHead(session, roundContinues)

	store.now = func() time.Time { return base.Add(roundMemory + time.Second) }
	store.open(uuidOf(4), streamHandle{ReqID: "R2", StreamID: "S2"})

	if _, verdict := store.claimEnding(session); verdict != roundForgotten {
		t.Errorf("verdict %v for a round swept long ago, want roundForgotten", verdict)
	}
}

// TestALateFailureRacesTheAnswerSafely — both endings arrive at once, which is
// exactly what a retry that succeeded while the sweeper was failing its parent
// looks like. Whichever wins, the store hands the round to one of them.
func TestALateFailureRacesTheAnswerSafely(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = boundElsewhere(rig.inst.ID)
	guarded(t, rig)

	bus := events.New()
	rig.typing.Register(bus)
	out := NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger())

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		out.handleEvent(chatDoneEvent(rig.session, "答案是 42"))
	}()
	go func() {
		defer wg.Done()
		bus.Publish(events.Event{
			Type:    protocol.EventTaskFailed,
			Payload: map[string]any{"chat_session_id": uuidText(rig.session)},
		})
	}()
	wg.Wait()

	if got := contentsOf(&rig.conn.recordingConn); len(got) > 2 {
		t.Fatalf("the user received %v, want at most the answer and one failure notice", got)
	}
}
