package wecom

// regression_fresh_session_behind_quote_test.go — "/new" is how a person tells
// the bot to forget the previous conversation and start over. In a busy room
// the way you ask about one specific message is to 引用 it and type your
// question underneath, so "quote a message + /new 重新分析" is the ordinary
// shape of the request, not an exotic one.
//
// The quote is rendered ABOVE the user's own line in the stored body, so
// anything that reads the command off the front of that body reads the quoted
// person's words instead. This file guards the outcome the user can see: the
// run that starts must actually be a fresh one. It drives a real
// aibot_msg_callback frame through the real engine.Router and the real wecom
// resolvers, and watches what reaches EnqueueChatTask — the same rig
// inbound_media_test.go uses.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// freshWatchingTasks records the fresh-session flag every chat run is queued
// with. That flag is the whole product behaviour of /new: with it false the
// agent resumes the old provider session and the user's "start over" was
// silently ignored.
type freshWatchingTasks struct {
	mu     sync.Mutex
	queued []bool
}

func (f *freshWatchingTasks) EnqueueChatTask(_ context.Context, _ db.ChatSession, _ pgtype.UUID, forceFresh bool) (db.AgentTaskQueue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued = append(f.queued, forceFresh)
	return db.AgentTaskQueue{}, nil
}

func (f *freshWatchingTasks) PromoteChannelChatTasksIfMediaReady(context.Context, pgtype.UUID) error {
	return nil
}

func (f *freshWatchingTasks) runs() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.queued...)
}

// groupQuotingFrame is somebody in a group room quoting a message and typing
// their own line underneath it — the frame WeCom pushes for 引用 + 提问.
func groupQuotingFrame(msgID, own, quoted string) frameEnvelope {
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "bot",
		"chattype": "group",
		"chatid":   "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": own},
		"quote": map[string]any{
			"msgtype": "text",
			"text":    map[string]any{"content": quoted},
		},
	})
	return frameEnvelope{Cmd: cmdMsgCallback, Body: body}
}

// freshSessionRig wires the real Router to the real wecom ResolverSet with the
// package's fakes, and hands back the socket to push a frame at plus the two
// recorders the assertions read: what was queued, and what was stored.
func freshSessionRig(t *testing.T) (*wecomChannel, *recordingConn, *freshWatchingTasks, *fakeSessionBinder) {
	t.Helper()
	tasks := &freshWatchingTasks{}
	binder := &fakeSessionBinder{sessionID: uuidOf(6)}

	router := engine.NewRouter(fakeIssueCreator{}, tasks, fakeSessionReader{}, engine.RouterConfig{Logger: testLogger()})
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: &installationResolver{store: &fakeInstallationLookup{inst: Installation{
			ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3), InstallerUserID: uuidOf(4),
			Status: InstallationActive, BotID: "bot",
		}}},
		Identity: &identityResolver{store: &fakeIdentityLookup{
			binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, member: true,
		}},
		Dedup:      &deduper{q: &fakeDedupQueries{claimToken: uuidOf(9)}},
		Session:    &sessionBinder{session: binder},
		Audit:      &auditor{q: &fakeAuditQueries{}},
		Media:      NewMediaResolver(&fakeMediaStorage{}, newFakeMediaLedger(nil), nil, nil, testLogger()),
		OriginType: originWecomChat,
	})
	t.Cleanup(func() { router.Drain(context.Background()) })

	c, conn, _ := testChannel(t, router.Handle)
	return c, conn, tasks, binder
}

// TestANewCommandBehindAQuoteStillStartsAFreshSession: someone quotes a
// colleague's number and writes "/new 重新分析这个数". They asked for a clean
// start; if the command is read off the stored body it lands on the quote
// block instead, the run resumes the old provider session, and the agent
// answers carrying all the context the person just asked it to drop. Nothing
// tells them it was ignored — the reply simply comes back wrong.
func TestANewCommandBehindAQuoteStillStartsAFreshSession(t *testing.T) {
	c, conn, tasks, binder := freshSessionRig(t)

	frame := groupQuotingFrame("msg-new-quoted", "/new 重新分析这个数", "Q3 毛利率 42.1%")
	if err := c.dispatchFrame(context.Background(), frame, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	runs := tasks.runs()
	if len(runs) != 1 {
		t.Fatalf("the message queued %d agent runs, want exactly 1 — the rig is not exercising the /new path", len(runs))
	}
	if !runs[0] {
		t.Errorf("the agent run was queued with force-fresh=false: the user quoted a message, typed %q, and got the old session back instead of the fresh one they asked for", "/new 重新分析这个数")
	}
	if body := binder.appended.Body; strings.Contains(body, "/new") {
		t.Errorf("stored body = %q — the /new directive was never stripped, so the agent is handed the raw slash command as if it were part of the question", body)
	}
}

// TestANewCommandWithNothingQuotedStartsAFreshSession is the positive control
// for the rig above: with no quote in front of it the same command already
// works today. It is here so that a future red run tells you which half broke
// — the wiring, or the quoted case specifically.
func TestANewCommandWithNothingQuotedStartsAFreshSession(t *testing.T) {
	c, conn, tasks, binder := freshSessionRig(t)

	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-new-plain",
		"aibotid":  "bot",
		"chattype": "group",
		"chatid":   "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": "/new 重新分析这个数"},
	})
	if err := c.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	runs := tasks.runs()
	if len(runs) != 1 || !runs[0] {
		t.Fatalf("an unquoted /new queued runs %v, want exactly one fresh run", runs)
	}
	if got := binder.appended.Body; got != "重新分析这个数" {
		t.Errorf("stored body = %q, want the question with the directive stripped", got)
	}
}

// TestAQuotedNewCommandDoesNotRestartSomebodyElsesSession is the negative
// control: the quoted message is another person's old text. Someone quoting a
// message that happens to begin with /new, and asking an ordinary question
// underneath, must not wipe the room's session. It passes today and has to
// keep passing — reading the command out of the quote is the failure mode a
// careless fix for the test above would introduce.
func TestAQuotedNewCommandDoesNotRestartSomebodyElsesSession(t *testing.T) {
	c, conn, tasks, _ := freshSessionRig(t)

	frame := groupQuotingFrame("msg-new-inside-quote", "这条我处理过了", "/new 重跑一遍")
	if err := c.dispatchFrame(context.Background(), frame, newWSSender(conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	runs := tasks.runs()
	if len(runs) != 1 {
		t.Fatalf("the message queued %d agent runs, want exactly 1", len(runs))
	}
	if runs[0] {
		t.Error("a /new inside a quote restarted the session: one person's old message must not throw away the room's context the moment somebody quotes it")
	}
}
