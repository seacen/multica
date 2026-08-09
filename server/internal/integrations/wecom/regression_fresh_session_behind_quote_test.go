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
// run that starts must actually be a fresh one, and the number they quoted
// must still be in the message the agent is handed.
//
// It drives a real aibot_msg_callback frame through the real engine.Router
// and the real wecom sessionBinder, and watches what reaches EnqueueChatTask.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---- the narrow doubles the Router needs ----

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

func (*freshWatchingTasks) PromoteChannelChatTasksIfMediaReady(context.Context, pgtype.UUID) error {
	return nil
}
func (*freshWatchingTasks) PromoteDeferredChannelIssueTask(context.Context, pgtype.UUID) error {
	return nil
}

func (f *freshWatchingTasks) runs() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.queued...)
}

type freshTestIssues struct{}

func (freshTestIssues) Create(context.Context, service.IssueCreateParams, service.IssueCreateOpts) (service.IssueCreateResult, error) {
	return service.IssueCreateResult{}, errors.New("no /issue in this test")
}
func (freshTestIssues) PublishAttachmentsChanged(context.Context, db.Issue, pgtype.UUID) {}

type freshTestSessions struct{}

func (freshTestSessions) GetChatSession(context.Context, pgtype.UUID) (db.ChatSession, error) {
	return db.ChatSession{}, nil
}
func (freshTestSessions) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return db.Workspace{}, nil
}

// The three resolvers in front of the binder are not what is under test, so
// they answer the same way every time: one active installation, one bound
// member, one fresh dedup claim.
type freshTestInstallations struct{}

func (freshTestInstallations) ResolveInstallation(context.Context, channel.InboundMessage) (engine.ResolvedInstallation, error) {
	return engine.ResolvedInstallation{
		ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3),
		InstallerUserID: uuidOf(4), Active: true,
		Platform: Installation{ID: uuidOf(1), BotID: "bot"},
	}, nil
}

type freshTestIdentities struct{}

func (freshTestIdentities) ResolveSender(context.Context, engine.ResolvedInstallation, channel.InboundMessage) (engine.ResolvedIdentity, error) {
	return engine.ResolvedIdentity{UserID: uuidOf(7)}, nil
}

type freshTestDedup struct{}

func (freshTestDedup) Claim(context.Context, pgtype.UUID, string) (pgtype.UUID, error) {
	return uuidOf(9), nil
}
func (freshTestDedup) Mark(context.Context, pgtype.UUID, string, pgtype.UUID) error    { return nil }
func (freshTestDedup) Release(context.Context, pgtype.UUID, string, pgtype.UUID) error { return nil }

type freshTestAudit struct{}

func (freshTestAudit) RecordDrop(context.Context, pgtype.UUID, channel.InboundMessage, engine.DropReason) error {
	return nil
}

// groupQuotingFrame is somebody in a group room quoting a message and typing
// their own line underneath it — the frame WeCom pushes for 引用 + 提问.
func groupQuotingFrame(msgID, own, quoted string) frameEnvelope {
	payload := map[string]any{
		"msgid":    msgID,
		"aibotid":  "bot",
		"chattype": "group",
		"chatid":   "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": own},
	}
	if quoted != "" {
		payload["quote"] = map[string]any{
			"msgtype": "text",
			"text":    map[string]any{"content": quoted},
		}
	}
	body, _ := json.Marshal(payload)
	return frameEnvelope{Cmd: cmdMsgCallback, Body: body}
}

// freshSessionRig wires the real Router to the real wecom sessionBinder, and
// hands back the socket to push a frame at plus the two recorders the
// assertions read: what was queued, and what was stored.
func freshSessionRig(t *testing.T) (*wecomChannel, *recordingConn, *freshWatchingTasks, *fakeSessionBinder) {
	t.Helper()
	tasks := &freshWatchingTasks{}
	binder := &fakeSessionBinder{sessID: uuidOf(6)}

	router := engine.NewRouter(freshTestIssues{}, tasks, freshTestSessions{}, engine.RouterConfig{Logger: slog.Default()})
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: freshTestInstallations{},
		Identity:     freshTestIdentities{},
		Dedup:        freshTestDedup{},
		Session:      &sessionBinder{session: binder},
		Audit:        freshTestAudit{},
		OriginType:   originWecomChat,
	})
	t.Cleanup(func() { router.Drain(context.Background()) })

	c := testChannel(router.Handle)
	conn := &recordingConn{}
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
	if err := c.dispatchFrame(context.Background(), frame, conn.autoAck(newWSSender(conn, nil)), slog.Default()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	runs := tasks.runs()
	if len(runs) != 1 {
		t.Fatalf("the message queued %d agent runs, want exactly 1 — the rig is not exercising the /new path", len(runs))
	}
	if !runs[0] {
		t.Errorf("the agent run was queued with force-fresh=false: the user quoted a message, typed %q, and got the old session back instead of the fresh one they asked for", "/new 重新分析这个数")
	}
	body := binder.appendIn.Body
	if strings.Contains(body, "/new") {
		t.Errorf("stored body = %q — the /new directive was never stripped, so the agent is handed the raw slash command as if it were part of the question", body)
	}
	if !strings.Contains(body, "Q3 毛利率 42.1%") {
		t.Errorf("stored body = %q — the quoted figure was dropped, so the fresh session was asked to re-analyse a number it has never been shown and has no earlier turn to find it in", body)
	}
}

// TestANewCommandWithNothingQuotedStartsAFreshSession is the positive control
// for the rig above: with no quote in front of it the same command already
// works today. It is here so that a future red run tells you which half broke
// — the wiring, or the quoted case specifically.
func TestANewCommandWithNothingQuotedStartsAFreshSession(t *testing.T) {
	c, conn, tasks, binder := freshSessionRig(t)

	frame := groupQuotingFrame("msg-new-plain", "/new 重新分析这个数", "")
	if err := c.dispatchFrame(context.Background(), frame, conn.autoAck(newWSSender(conn, nil)), slog.Default()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}

	runs := tasks.runs()
	if len(runs) != 1 || !runs[0] {
		t.Fatalf("an unquoted /new queued runs %v, want exactly one fresh run", runs)
	}
	if got := binder.appendIn.Body; got != "重新分析这个数" {
		t.Errorf("stored body = %q, want the question with the directive stripped", got)
	}
}

// TestAQuotedNewCommandDoesNotRestartSomebodyElsesSession is the negative
// control: the quoted message is another person's old text. Someone quoting a
// message that happens to begin with /new, and asking an ordinary question
// underneath, must not wipe the room's session. Reading the command out of
// the quote is the failure mode a careless fix for the test above would
// introduce.
func TestAQuotedNewCommandDoesNotRestartSomebodyElsesSession(t *testing.T) {
	c, conn, tasks, _ := freshSessionRig(t)

	frame := groupQuotingFrame("msg-new-inside-quote", "这条我处理过了", "/new 重跑一遍")
	if err := c.dispatchFrame(context.Background(), frame, conn.autoAck(newWSSender(conn, nil)), slog.Default()); err != nil {
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

// The binder is what hands CommandText to the shared classifier. With the
// stored body carrying a quote in front, passing that body instead of the
// user's own line is how /issue and /new stop being recognised at all.
func TestTheBinderPassesTheUnquotedLineAsTheCommandText(t *testing.T) {
	t.Parallel()
	fb := &fakeSessionBinder{sessID: uuidOf(6)}
	b := &sessionBinder{session: fb}

	if _, err := b.AppendMessage(context.Background(), engine.AppendParams{
		Message: channel.InboundMessage{
			Text:        "> 引用：Q3 毛利率 42.1%\n这个数对吗",
			CommandText: "这个数对吗",
		},
	}); err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}

	if got := fb.appendIn.CommandText; got != "这个数对吗" {
		t.Fatalf("CommandText = %q, want the sender's own line — a command parser reading this finds the quote block instead", got)
	}
	if got := fb.appendIn.Body; !strings.HasPrefix(got, "> ") {
		t.Fatalf("Body = %q, want the quote kept in what the agent reads", got)
	}
}

// A message with nothing quoted has no separate command line, and the binder
// must go on passing the body — this is what every non-quoting message on
// every existing deployment does.
func TestTheBinderStillPassesTheBodyWhenNothingWasQuoted(t *testing.T) {
	t.Parallel()
	fb := &fakeSessionBinder{sessID: uuidOf(6)}
	b := &sessionBinder{session: fb}

	if _, err := b.AppendMessage(context.Background(), engine.AppendParams{
		Message: channel.InboundMessage{Text: "在吗"},
	}); err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	if got := fb.appendIn.CommandText; got != "在吗" {
		t.Fatalf("CommandText = %q, want the body", got)
	}
}
