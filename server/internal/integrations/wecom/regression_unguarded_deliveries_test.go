package wecom

// regression_unguarded_deliveries_test.go — four things the user receives that
// no test was actually watching.
//
// Every path below already had a test whose NAME promised it: an answer whose
// verdict never came "costs the bubble, not the answer"; a turn's task id
// picking the round an answer belongs to; the copy speaking the language of
// whoever reads it. Reverting the code under each of those left them green, so
// the promise in the name was the only thing there — and a name is what the
// next person reads instead of re-deriving the risk.
//
// The four are unrelated in mechanism and identical in consequence: the person
// who asked is left with something wrong and nothing on their screen says so.
// WeCom has no edit and no unsend, so there is no later correction; whatever
// lands is what they keep.
//
// Written against what arrives in the chat, never against how it gets there.
// Change the ack bookkeeping, the round matching or where a locale is read —
// none of that should touch these tests.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// ---- the answer whose verdict never came ----

// TestAnAnswerWhoseVerdictNeverArrivesIsStillSentToTheUser is the half of the
// lost-ack rule that costs a person their reply.
//
// A stream frame is written and the server's verdict decides everything: an
// accepted closing frame IS the delivery, a refused one sends the answer as a
// plain message instead. A verdict that never arrives looks like neither. The
// adapter has to treat it as the second, because treating it as the first
// throws the answer away: nothing is sent, the bubble sits on the screen still
// showing "正在读取 x.go" from the last refresh that landed, and the user is
// left waiting for a reply that has already been counted as delivered.
//
// The frame may well have landed, so this trades a possible duplicate for a
// possible silence. That is the right way round — a person who sees the answer
// twice is mildly annoyed; a person who never sees it has been ignored.
func TestAnAnswerWhoseVerdictNeverArrivesIsStillSentToTheUser(t *testing.T) {
	rig := newStreamRig(t)
	rig.ingest(t, "REQ-42") // the question, the bubble, and an ack for it

	// From here the server writes nothing back. Not a refusal — silence, which
	// is what a dropped ack frame or a socket that stalled one way looks like.
	sender := rig.senders.get(rig.inst.ID)
	sender.ackTimeout = 50 * time.Millisecond
	rig.conn.mu.Lock()
	rig.conn.silent = true
	rig.conn.mu.Unlock()

	const answer = "答案是 42"
	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneEvent(rig.session, answer))

	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != answer {
		t.Fatalf("the closing frame got no verdict and the chat received %v, want the answer %q as a plain message.\n"+
			"The user asked a question, watched a spinner, and never got a reply anywhere: not in the bubble, "+
			"which nothing will close now, and not in the chat. Nothing later revisits it.", got, answer)
	}
}

// ---- whose answer is this ----

// TestOneRunsAnswerDoesNotSealAnotherQuestionsBubble — a session can have two
// questions in flight, each with its own bubble, and the ONLY thing that says
// which one an answer belongs to is the task id the finished run publishes.
// The event carries it in the payload and leaves the envelope's own field
// empty, so an adapter that reads only the envelope sees every answer as
// unattributed and gives it to the oldest open round.
//
// What that does to a person: they asked twice. The second question's answer is
// painted into the first question's bubble and sealed there — under the first
// question, which is still running. Now the first run has no bubble left to
// finish into, the second asker's bubble spins on forever, and the one answer
// on screen is filed under the wrong question. A bubble is immutable once
// sealed, so none of it can be taken back.
func TestOneRunsAnswerDoesNotSealAnotherQuestionsBubble(t *testing.T) {
	rig, bus, _, clock := busRig(t)

	runA := uuidText(uuidOf(51))
	runB := uuidText(uuidOf(52))

	// The first question, and a run that speaks — which is what ties that
	// round to run A.
	rig.ingest(t, "REQ-A")
	bus.Publish(taskMessageEvent(runA, toolUse("Read", map[string]any{"file_path": "one.go"})))

	// The second question, two minutes later: its own round, its own bubble,
	// queued behind the run still going.
	clock.advance(2 * time.Minute)
	rig.ingest(t, "REQ-B")
	if depth := rig.streams.depth(); depth != 2 {
		t.Fatalf("setup: store depth %d, want a bubble open for each question", depth)
	}

	// Run B finishes. It names itself the only way chat:done ever does.
	const answer = "答案是 42"
	NewOutbound(boundQueries(rig.inst.ID), rig.senders, rig.streams, testLogger()).
		handleEvent(chatDoneFromRun(rig.session, runB, answer))

	for _, f := range streamViews(t, &rig.conn.recordingConn) {
		if f.Finish && f.ReqID == "REQ-A" {
			t.Fatalf("run B's answer %q was sealed into the FIRST question's bubble (req_id %s).\n"+
				"That question's own run is still going and now has no bubble to finish into, "+
				"the second asker is left with a spinner nothing will close, "+
				"and the answer on screen is filed under a question it does not answer.", f.Content, f.ReqID)
		}
	}
	if got := contentsOf(&rig.conn.recordingConn); len(got) != 1 || got[0] != answer {
		t.Fatalf("the chat received %v, want run B's answer %q delivered as a plain message — "+
			"it may not take another round's bubble, but it still has to reach the person who asked", got, answer)
	}
}

// ---- what language the reader reads ----

// TestALateFailureIsSaidInTheLanguageOfTheChatItLandsIn — a run that dies with
// no bubble left (a restart mid-run, a bubble the guard already closed) is
// announced from the binding row, and that notice is the ONLY account this
// adapter ever gives of a failed run. It has to be in the language of the chat
// it lands in, for the same reason the answer is: a person who cannot read the
// one sentence they were sent about a run that failed has been told nothing,
// and there is no second telling.
func TestALateFailureIsSaidInTheLanguageOfTheChatItLandsIn(t *testing.T) {
	rig := newStreamRig(t)
	rig.typing.bindings = &fakeBindings{
		binding: db.ChannelChatSessionBinding{
			InstallationID: rig.inst.ID,
			ChannelChatID:  rig.principalSender,
			ChatType:       "p2p",
		},
		install: db.ChannelInstallation{ID: rig.inst.ID, Status: string(InstallationActive)},
	}
	withEnglishSpeakingPrincipal(rig)

	failTheRun(rig)

	want := copyFor(LocaleEn).StreamFailed
	got := contentsOf(&rig.conn.recordingConn)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("the reader was told %v about a run that died, want their own language: %q.\n"+
			"This notice is the only word WeCom ever gets that a run failed; in a language they do not read it is silence.", got, want)
	}
}

// TestAnInboxCardSpeaksTheRecipientsOwnLanguage — the inbox push is the one
// surface whose reader is always known: it is addressed to a single Multica
// member by their binding row. Their profile language is a setting they chose
// themselves, and a card that ignores it is a notification the person cannot
// act on — the link text is how they know there is anything to open.
func TestAnInboxCardSpeaksTheRecipientsOwnLanguage(t *testing.T) {
	t.Setenv("WECOM_APP_URL", "https://example.com")

	inst := uuidOf(61)
	recipient := uuidOf(62)
	workspace := uuidOf(63)

	conn := &recordingConn{}
	senders := NewSendersRegistry()
	senders.log = testLogger()
	senders.set(inst, newWSSender(conn, testLogger()))

	q := profileQueries{
		fakeOutboundQueries: &fakeOutboundQueries{
			memberBind: db.ChannelUserBinding{
				InstallationID: inst,
				ChannelUserID:  "T-alex",
				MulticaUserID:  recipient,
			},
			install:   db.ChannelInstallation{ID: inst, Status: string(InstallationActive)},
			workspace: db.Workspace{Slug: "acme"},
		},
		wecomUser: "T-alex",
		user:      recipient,
		language:  "en",
	}

	NewOutbound(q, senders, nil, testLogger()).handleInboxNew(events.Event{
		Type: protocol.EventInboxNew,
		Payload: map[string]any{"item": map[string]any{
			"recipient_type": "member",
			"recipient_id":   uuidText(recipient),
			"workspace_id":   uuidText(workspace),
			"type":           "new_comment",
			"title":          "Deploy blocked",
			"issue_id":       "9194c058-e8a4-4c15-9c65-86d1784ba715",
		}},
	})

	got := contentsOf(conn)
	if len(got) != 1 {
		t.Fatalf("the member received %v, want exactly one card", got)
	}
	if !strings.Contains(got[0], copyFor(LocaleEn).InboxDetailLink) {
		t.Fatalf("the card reads %q, want it in the recipient's own profile language (%q).\n"+
			"This push goes to one named person whose language setting is known; a card they cannot read "+
			"is a notification they will not act on.", got[0], copyFor(LocaleEn).InboxDetailLink)
	}
}

// TestAFileThatCouldNotBeSentIsExplainedInTheReadersOwnLanguage — the agent
// said "it's attached" and the file did not go. The notice that follows is the
// only thing standing between the reader and looking for an attachment that
// will never appear, so it has to be in the language they were just answered
// in. Unreadable, it leaves them exactly where saying nothing would have.
func TestAFileThatCouldNotBeSentIsExplainedInTheReadersOwnLanguage(t *testing.T) {
	rig := newMediaRig(t)
	rig.attach("quarterly-review.pptx", "application/pdf", payload(1024))
	rig.objects.err = errors.New("object storage unreachable")

	q := profileQueries{
		fakeOutboundQueries: rig.q,
		wecomUser:           "T-alex",
		user:                uuidOf(33),
		language:            "en",
	}
	o := NewOutbound(q, rig.senders, rig.streams, testLogger(), WithAttachments(rig.objects))
	o.spawn = func(f func()) { f() }

	const answer = "It's attached."
	if err := o.processEvent(context.Background(), rig.answered(answer)); err != nil {
		t.Fatalf("processEvent: %v", err)
	}

	want := copyFor(LocaleEn).MediaSendFailed
	got := rig.textPosts()
	if len(got) != 2 || got[0] != answer {
		t.Fatalf("the chat received %v, want the answer followed by a word about the file", got)
	}
	if got[1] != want {
		t.Fatalf("the file failed and the reader was told %q, want their own language: %q.\n"+
			"They have just been told the file is attached; in a language they do not read, "+
			"this notice leaves them hunting for an attachment that is not coming.", got[1], want)
	}
}

// ---- scaffolding ----

// profileQueries is fakeOutboundQueries with one WeCom user actually bound to a
// Multica profile that has a language on it. The shared fake answers both
// language reads with "no row", which resolves every reader to the deployment
// default — and a default everywhere is a suite in which no locale selection
// site can be told from a hardcoded one.
type profileQueries struct {
	*fakeOutboundQueries
	wecomUser string      // the bot-scoped userid, which for a 1:1 is also the chatid
	user      pgtype.UUID // the Multica profile behind it
	language  string
}

func (q profileQueries) GetChannelUserBindingByUserID(_ context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error) {
	if arg.ChannelUserID == q.wecomUser {
		return db.ChannelUserBinding{MulticaUserID: q.user}, nil
	}
	return db.ChannelUserBinding{}, pgx.ErrNoRows
}

func (q profileQueries) GetUser(_ context.Context, id pgtype.UUID) (db.User, error) {
	if id == q.user {
		return db.User{ID: id, Language: pgtype.Text{String: q.language, Valid: true}}, nil
	}
	return db.User{}, pgx.ErrNoRows
}
