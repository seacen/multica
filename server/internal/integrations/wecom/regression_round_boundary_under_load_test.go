package wecom

// regression_round_boundary_under_load_test.go — guards the decision "does this
// message get a bubble of its own" against how busy the database happens to be.
//
// Two messages are one round or two, and the engine's batcher settles that the
// moment each one arrives: it re-arms a per-session timer and fires exactly one
// run per silence window (engine/batcher.go). The store answers the same
// question a second time, later — on the detached typing goroutine, behind the
// dedup write and the three indexed reads that name the asker and pick their
// language — and it measures the gap with its own clock at that later moment.
// The two measurements of one gap differ by however much those reads differed
// between the two messages, so a loaded pool is enough to make the store
// disagree with the batcher about a gap anywhere near the window.
//
// Both directions of disagreement cost the user something in a chat with no
// edit and no unsend. One way, a question that IS getting its own run gets no
// bubble: its asker sees only the previous question's spinner, has nothing
// saying their message landed, and the answer to it arrives later outside any
// bubble as a plain message. The other way, one run gets two bubbles: the answer
// seals the first and the second turns until the five-minute guard replaces it
// with "still working, I'll reply separately" — a promise about a run that
// finished long ago and will never say another word.
//
// Every other test of this boundary drives the store's clock directly
// (stream_queued_test.go), which is exactly the seam that hides this: it sets
// the gap the store sees and never lets it differ from the gap the batcher saw.
// The two tests here ingest through OnIngested with the lookups stalling on that
// same clock, which is the only way the difference shows up at all.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---- scaffolding ----

// stallingPool is the identity and language lookup pair standing in for a
// connection pool under load: the first read a message makes burns the delay
// the test gave that message, on the same clock the store measures gaps with.
// One delay per message rather than per read, because what matters is how long
// the whole ingest path took to reach the store, not which query was slow.
type stallingPool struct {
	mu      sync.Mutex
	clock   *testClock
	pending time.Duration
	bound   map[string]pgtype.UUID
}

// stall makes the next read this pool serves take d.
func (p *stallingPool) stall(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = d
}

func (p *stallingPool) burn() {
	p.mu.Lock()
	d := p.pending
	p.pending = 0
	p.mu.Unlock()
	if d > 0 {
		p.clock.advance(d)
	}
}

func (p *stallingPool) GetChannelUserBindingByUserID(_ context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error) {
	p.burn()
	p.mu.Lock()
	id, ok := p.bound[arg.ChannelUserID]
	p.mu.Unlock()
	if !ok {
		return db.ChannelUserBinding{}, pgx.ErrNoRows
	}
	return db.ChannelUserBinding{MulticaUserID: id}, nil
}

func (p *stallingPool) GetUser(_ context.Context, id pgtype.UUID) (db.User, error) {
	p.burn()
	return db.User{ID: id}, nil
}

func (p *stallingPool) IsWorkspaceMember(context.Context, pgtype.UUID, pgtype.UUID) (bool, error) {
	return true, nil
}

// arrival is one callback in a schedule: when it reached the adapter — which is
// the only thing the batcher's verdict depends on — and how long that message's
// own reads took before the store got to classify it.
type arrival struct {
	reqID   string
	at      time.Duration // from the start of the schedule
	lookups time.Duration
}

// playArrivals ingests a schedule of callbacks through OnIngested on a driven
// clock, so the same two questions can be replayed against an idle pool and a
// loaded one with everything else — the messages, the times they arrived, the
// order — held identical.
func playArrivals(t *testing.T, plan []arrival) *streamRig {
	t.Helper()
	rig := newStreamRig(t)
	clock := newTestClock()
	rig.streams.now = clock.now
	pool := &stallingPool{
		clock: clock,
		bound: map[string]pgtype.UUID{rig.principalSender: rig.inst.InstallerUserID},
	}
	rig.typing.identities = pool
	rig.typing.languages = pool

	start := clock.now()
	for _, a := range plan {
		wait := start.Add(a.at).Sub(clock.now())
		if wait < 0 {
			t.Fatalf("schedule error: %s arrives at %v but the clock has already reached %v",
				a.reqID, a.at, clock.now().Sub(start))
		}
		clock.advance(wait)
		pool.stall(a.lookups)
		rig.ingest(t, a.reqID)
	}
	return rig
}

// bubbleCount is how many bubbles the user has on screen for this session.
func bubbleCount(t *testing.T, rig *streamRig) int {
	t.Helper()
	return len(streamViews(t, &rig.conn.recordingConn))
}

// ---- the two directions ----

// TestASecondQuestionKeepsItsReceiptWhenTheDatabaseIsSlow — two questions far
// enough apart that the engine gives each its own run and each run its own
// answer. The second one must be acknowledged the moment it lands, whatever the
// database was doing when the first one landed.
//
// What breaks for a person when this regresses: they ask, wait, ask again, and
// the second question produces nothing at all on their screen — no bubble, no
// receipt, no sign it was read — while the previous question's spinner turns on
// for however long that run needs. The answer to the second question then
// arrives outside any bubble as a loose message with no visible connection to
// what they asked. Nothing distinguishes any of that from a message the bot
// never received, and they cannot tell which question the loose answer belongs
// to. Whether it happens is decided by how loaded the connection pool was
// seconds earlier, so it happens intermittently and never in a quiet
// environment.
func TestASecondQuestionKeepsItsReceiptWhenTheDatabaseIsSlow(t *testing.T) {
	// Past the debounce window: the batcher fires one run for the first
	// question and a second run for this one.
	const gap = sameRoundWindow + 400*time.Millisecond

	idle := playArrivals(t, []arrival{
		{reqID: "REQ-1", at: 0, lookups: 20 * time.Millisecond},
		{reqID: "REQ-2", at: gap, lookups: 20 * time.Millisecond},
	})
	if got := bubbleCount(t, idle); got != 2 {
		t.Fatalf("setup: with an idle pool two questions %v apart produced %d bubbles, want one each", gap, got)
	}

	// The same two questions, typed at the same two moments. The only thing
	// that changed is that the first message's reads queued behind a loaded
	// pool for 1.9s.
	loaded := playArrivals(t, []arrival{
		{reqID: "REQ-1", at: 0, lookups: 1900 * time.Millisecond},
		{reqID: "REQ-2", at: gap, lookups: 100 * time.Millisecond},
	})

	frames := streamViews(t, &loaded.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("the second question got %d bubble(s) instead of %d — it arrived %v after the first, "+
			"past the %v window, so it gets a run and an answer of its own, but the asker was shown nothing "+
			"acknowledging it and its answer will arrive as a loose message. The identical schedule against "+
			"an idle pool produced %d: the receipt was lost to database latency, not to anything the user did",
			len(frames), bubbleCount(t, idle), gap, sameRoundWindow, bubbleCount(t, idle))
	}
	if frames[1].ReqID != "REQ-2" {
		t.Errorf("the second bubble rode req_id %q, want the second question's own REQ-2 — a bubble on any "+
			"other callback's req_id is refused by the server (846605)", frames[1].ReqID)
	}
	if frames[0].ID == frames[1].ID {
		t.Error("both questions share one stream id, so the second answer would repaint the first answer's bubble")
	}
	if depth := loaded.streams.depth(); depth != 2 {
		t.Errorf("store depth = %d, want a round open per run — the answer to the round with no bubble "+
			"falls out to a plain message", depth)
	}
}

// TestOneRunLeavesOneSpinnerWhenTheDatabaseIsSlow — the same disagreement the
// other way round. Two questions typed close together are one run with one
// answer, so they must share one bubble however long either one's reads took.
//
// What breaks for a person when this regresses: they type twice in quick
// succession and get two spinners for the one reply that is coming. The answer
// lands in the first and the second keeps turning, for five minutes, until the
// guard replaces it with "still working, I'll reply separately" — a promise made
// about a run that finished before the promise was written, so nothing else is
// ever coming and the last thing in the chat says otherwise. WeCom has no edit
// and no unsend, so that stays on their screen.
func TestOneRunLeavesOneSpinnerWhenTheDatabaseIsSlow(t *testing.T) {
	// Inside the debounce window: the batcher re-arms and both messages are
	// collected into a single run with a single answer.
	const gap = sameRoundWindow - 200*time.Millisecond

	idle := playArrivals(t, []arrival{
		{reqID: "REQ-1", at: 0, lookups: 20 * time.Millisecond},
		{reqID: "REQ-2", at: gap, lookups: 20 * time.Millisecond},
	})
	if got := bubbleCount(t, idle); got != 1 {
		t.Fatalf("setup: with an idle pool two questions %v apart produced %d bubbles, want one shared", gap, got)
	}

	// Same two moments; this time the second message is the one whose reads
	// queue behind a loaded pool.
	loaded := playArrivals(t, []arrival{
		{reqID: "REQ-1", at: 0, lookups: 50 * time.Millisecond},
		{reqID: "REQ-2", at: gap, lookups: 600 * time.Millisecond},
	})

	if got := bubbleCount(t, loaded); got != 1 {
		t.Fatalf("two messages %v apart opened %d bubbles — inside the %v window they are one run with one "+
			"answer, so the extra spinner has nothing coming to close it and turns until the guard promises "+
			"a separate reply for a run that already finished. The identical schedule against an idle pool "+
			"produced %d", gap, got, sameRoundWindow, bubbleCount(t, idle))
	}

	// And the one answer that run produces leaves nothing behind turning.
	newOutboundUnder(loaded).handleEvent(chatDoneEvent(loaded.session, "答案是 42"))
	if depth := loaded.streams.depth(); depth != 0 {
		t.Errorf("%d bubble(s) still open after the run's only answer sealed one — that is a spinner nobody "+
			"will ever close", depth)
	}
}
