package wecom

// regression_issue_command_batch_test.go — a standalone /issue must never end
// up in front of the agent.
//
// WeCom is the one platform that answers /issue itself: the engine files the
// issue and the replier sends "✅ 已创建 #N", so the message carries
// SkipAgentRun and no agent run is triggered for it. That only holds if the
// command also stays OUT of the batch some other run hands to the agent. When
// it leaks in, the person who typed "/issue 修复登录" gets their confirmation
// and then, seconds later, a second answer from the agent reacting to the
// slash command itself — the exact clutter SkipAgentRun was added to prevent.
//
// These tests drive the real engine.Router through the real wecom frame path
// and watch what each run is given.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// chatTranscript stands in for the database on both sides of the question
// "what does the agent read": the append side that stores each inbound message
// as a user row, and the enqueue side that seals a batch of those rows to the
// task the agent will run.
//
// The seal mirrors LinkUnownedChannelChatMessagesToTask (queries/chat.sql),
// which EnqueueChatTask runs in the task's own transaction: every user row in
// the session that no task owns yet becomes part of this run's input. If a fix
// gives the engine a new way to say "this row is not agent input", teach that
// marker to sealForRun too — the assertion below is about the bodies the agent
// receives, not about how a row is kept out of them.
type chatTranscript struct {
	mu      sync.Mutex
	rows    []*transcriptRow
	batches [][]string
}

type transcriptRow struct {
	body string
	// taskID is chat_message.task_id: 0 stands for NULL, i.e. no run owns
	// this row yet and the next seal will take it.
	taskID int
	// kind is chat_message.message_kind. protocol.ChatMessageKindCommand marks
	// a message the engine answered itself; the seal skips those.
	kind string
}

// ---- the append side (engineSessionBinder) ----

func (tr *chatTranscript) EnsureSession(context.Context, engine.EnsureSessionInput) (pgtype.UUID, error) {
	return uuidOf(6), nil
}

func (tr *chatTranscript) AppendUserMessage(_ context.Context, in engine.AppendInput) (engine.AppendResult, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	// The real binder stamps chat_message.message_kind from in.MessageKind and
	// LinkUnownedChannelChatMessagesToTask refuses 'command' rows, so a row the
	// engine answered itself is never anybody's input. Mirrored here because
	// the assertion is about the bodies the agent receives, not about how a row
	// is kept out of them.
	tr.rows = append(tr.rows, &transcriptRow{body: in.Body, kind: in.MessageKind})
	// Same parse, off the same source, as the real binder: the stored body is
	// the agent-readable text, the command is read off the user's own line.
	source := in.CommandText
	if source == "" {
		source = in.Body
	}
	cmd, _ := engine.ParseIssueCommand(source)
	return engine.AppendResult{MessageID: uuidOf(5), IssueCommand: cmd}, nil
}

func (tr *chatTranscript) BindMediaRefs(context.Context, engine.BindMediaInput) error { return nil }

// ---- the seal side (engine.TaskEnqueuer) ----

func (tr *chatTranscript) EnqueueChatTask(context.Context, db.ChatSession, pgtype.UUID, bool) (db.AgentTaskQueue, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	taskID := len(tr.batches) + 1
	batch := []string{}
	for _, row := range tr.rows {
		if row.taskID != 0 || row.kind == protocol.ChatMessageKindCommand {
			continue
		}
		row.taskID = taskID
		batch = append(batch, row.body)
	}
	tr.batches = append(tr.batches, batch)
	return db.AgentTaskQueue{}, nil
}

func (tr *chatTranscript) PromoteChannelChatTasksIfMediaReady(context.Context, pgtype.UUID) error {
	return nil
}

// agentBatches is what every run so far was given to read, oldest first.
func (tr *chatTranscript) agentBatches() [][]string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	out := make([][]string, len(tr.batches))
	copy(out, tr.batches)
	return out
}

// issueRig wires the real Router and the real wecom resolvers over the
// transcript, with run batching armed on a window long enough that only the
// test decides when it expires (fireBatchWindow).
type issueRig struct {
	channel    *wecomChannel
	conn       *recordingConn
	router     *engine.Router
	transcript *chatTranscript
}

func newIssueRig(t *testing.T) *issueRig {
	t.Helper()
	transcript := &chatTranscript{}
	router := engine.NewRouter(fakeIssueCreator{}, transcript, fakeSessionReader{}, engine.RouterConfig{Logger: testLogger()})
	router.EnableRunBatching(time.Hour)
	router.Register(TypeWecom, engine.ResolverSet{
		Installation: &installationResolver{store: &fakeInstallationLookup{inst: Installation{
			ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3),
			Status: InstallationActive, BotID: "wb-1",
		}}},
		Identity: &identityResolver{store: &fakeIdentityLookup{
			binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, member: true,
		}},
		Dedup:      &deduper{q: &fakeDedupQueries{claimToken: uuidOf(9)}},
		Session:    &sessionBinder{session: transcript},
		Audit:      &auditor{q: &fakeAuditQueries{}},
		OriginType: originWecomChat,
	})
	c, conn, _ := testChannel(t, router.Handle)
	return &issueRig{channel: c, conn: conn, router: router, transcript: transcript}
}

// say pushes one text message off the wire, the way WeCom delivers it.
func (r *issueRig) say(t *testing.T, msgID, content string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  "wb-1",
		"chattype": "single",
		"chatid":   "T-alex",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "text",
		"text":     map[string]any{"content": content},
	})
	if err := r.channel.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, newWSSender(r.conn, nil), testLogger()); err != nil {
		t.Fatalf("dispatchFrame(%q): %v", content, err)
	}
}

// fireBatchWindow is the silence window expiring: Drain flushes whatever the
// debouncer still holds, on the same closure the timer would have run.
func (r *issueRig) fireBatchWindow(t *testing.T) {
	t.Helper()
	if !r.router.Drain(context.Background()) {
		t.Fatal("the debounced run never flushed")
	}
}

// assertAgentNeverSawTheCommand is the whole point of both tests: whatever the
// runs are handed, none of it is the slash command the engine already answered.
func assertAgentNeverSawTheCommand(t *testing.T, batches [][]string, command, question string) {
	t.Helper()
	sawQuestion := false
	for i, batch := range batches {
		for _, body := range batch {
			if body == question {
				sawQuestion = true
			}
			if !strings.Contains(body, command) {
				continue
			}
			t.Errorf("agent run #%d was handed the raw slash command %q (batch = %q).\n"+
				"The user has already been told \"✅ created #N\" for it, so the agent now answers that same command a second time — typically by explaining it does not know the command. Keeping the command out of every run is what SkipAgentRun is for.",
				i+1, body, batch)
		}
	}
	if !sawQuestion {
		t.Errorf("the user's actual question %q reached no agent run at all (batches = %q); suppressing the command must not swallow the conversation around it", question, batches)
	}
}

// TestAnIssueCommandTheAgentNeverRanForStaysOutOfItsBatch — the reported
// sequence: a question arms the 3s window, an /issue lands inside it and is
// answered by the engine alone, then the window from the question expires. The
// run that fires belongs to the question; the command must not ride along.
func TestAnIssueCommandTheAgentNeverRanForStaysOutOfItsBatch(t *testing.T) {
	const (
		question = "登录页面又报错了"
		command  = "/issue 修复登录报错"
	)
	rig := newIssueRig(t)

	rig.say(t, "MSGID-question", question) // t=0s: the window opens
	rig.say(t, "MSGID-issue", command)     // t=1s: filed and confirmed, no run
	rig.fireBatchWindow(t)                 // t=3s: the question's window expires

	batches := rig.transcript.agentBatches()
	if len(batches) != 1 {
		t.Fatalf("want exactly one agent run for the window, got %d (%q)", len(batches), batches)
	}
	assertAgentNeverSawTheCommand(t, batches, command, question)
}

// TestAnIssueCommandDoesNotRideAlongOnTheNextQuestion — the same leak without
// the timing race, and the more common shape of it: nothing is pending when
// the /issue arrives, so it simply sits in the session unowned until the
// person's next real question drags it into that run.
func TestAnIssueCommandDoesNotRideAlongOnTheNextQuestion(t *testing.T) {
	const (
		command  = "/issue 修复登录报错"
		question = "顺便看下昨天的报表"
	)
	rig := newIssueRig(t)

	rig.say(t, "MSGID-issue", command)     // filed and confirmed, no run
	rig.say(t, "MSGID-question", question) // a normal question, minutes later
	rig.fireBatchWindow(t)

	batches := rig.transcript.agentBatches()
	if len(batches) != 1 {
		t.Fatalf("want exactly one agent run for the question, got %d (%q)", len(batches), batches)
	}
	assertAgentNeverSawTheCommand(t, batches, command, question)
}
