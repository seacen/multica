package wecom

// outbox_direct_db_test.go — the duplicate reply, against a real database and
// the real reconciler.
//
// outbox_direct_test.go proves the adapter records a socket delivery. It
// cannot prove the thing the user actually complained about, because the
// suppression is not in Go: it is the NOT EXISTS in
// ListChannelOutboundReconcileCandidates, and whether a record written by
// RecordChannelOutboundDelivered satisfies it is a question only Postgres can
// answer. A record under a key the scan does not match looks identical from
// the call site and fixes nothing.
//
// So this test builds the shape the user was in — a WeCom-bound session, a
// completed run, an assistant reply — runs the real outbox.Reconciler over it
// twice, and asks the two questions in order: does it rescue a reply nobody
// recorded (it must, that is its job), and does it leave alone one that was
// delivered over the socket.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/integrations/channel/outbox"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func outboxTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("database not reachable: %v", err)
	}
	var migrated bool
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('public.channel_outbound_queue') IS NOT NULL
		   AND to_regclass('public.channel_outbound_reconcile_state') IS NOT NULL`).Scan(&migrated); err != nil || !migrated {
		pool.Close()
		t.Skip("channel outbound queue tables not present (database not migrated)")
	}
	t.Cleanup(pool.Close)
	return pool
}

// answeredTurn is one WeCom-bound session with a finished run and the reply it
// produced — the row shape the reconciler's candidate scan looks for.
type answeredTurn struct {
	mediaBindFixture
	sessionID pgtype.UUID
	taskID    pgtype.UUID
	chatID    string
}

// seedAnsweredTurn adds the chat half to the installation fixture: a session
// bound to a WeCom chat, a task that completed inside the reconciler's window,
// and the assistant message the builder renders the reply from.
//
// completedAt is in the future relative to the cursor row, which the caller
// then reaches with a Now override. It is not a trick: the cursor refuses to
// scan behind its own created_at, so on a database where the queue's epoch is
// "just now" there is no other way to put a task inside a window at all.
func seedAnsweredTurn(t *testing.T, pool *pgxpool.Pool, completedAt time.Time) answeredTurn {
	t.Helper()
	ctx := context.Background()
	f := answeredTurn{
		mediaBindFixture: seedMediaBindFixture(t, pool),
		chatID:           fmt.Sprintf("CHAT-%d", time.Now().UnixNano()),
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO chat_session (workspace_id, agent_id, creator_id, title)
		VALUES ($1, $2, $3, 'wecom outbox direct') RETURNING id`,
		f.workspaceID, f.agentID, f.userID).Scan(&f.sessionID); err != nil {
		t.Fatalf("create chat session: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_chat_session_binding
		    (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type)
		VALUES ($1, $2, 'wecom', $3, 'p2p')`,
		f.sessionID, f.installationID, f.chatID); err != nil {
		t.Fatalf("create chat binding: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, chat_session_id, status, completed_at)
		VALUES ($1, $2, 'completed', $3) RETURNING id`,
		f.agentID, f.sessionID, completedAt).Scan(&f.taskID); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO chat_message (chat_session_id, role, content, task_id)
		VALUES ($1, 'assistant', $2, $3)`,
		f.sessionID, "毛利率是 42.1%", f.taskID); err != nil {
		t.Fatalf("create assistant message: %v", err)
	}
	return f
}

// queuedRepliesFor counts rows the reconciler would have a worker deliver for
// this task — the second copy the user reads. Recorded deliveries are excluded
// because they are the opposite thing: a row that exists so nothing is sent.
func queuedRepliesFor(t *testing.T, pool *pgxpool.Pool, taskID pgtype.UUID) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*)::int FROM channel_outbound_queue
		WHERE source_id = $1 AND status = 'queued'`, util.UUIDToString(taskID)).Scan(&n); err != nil {
		t.Fatalf("count queued rows: %v", err)
	}
	return n
}

// TestTheReconcilerDoesNotResendAnAnswerTheBubbleAlreadyDelivered is the bug
// the user reported, in the order that makes the second half mean something.
//
// The first sweep is the control: with nothing recorded, the reconciler MUST
// rescue the reply, because that is the whole point of it and a test whose
// setup quietly stopped producing a candidate would otherwise pass by
// accident. The second sweep is the fix: the same task, the same window, one
// record of a delivery that went out over the socket, and no rescue.
func TestTheReconcilerDoesNotResendAnAnswerTheBubbleAlreadyDelivered(t *testing.T) {
	pool := outboxTestDB(t)
	ctx := context.Background()
	queries := db.New(pool)

	// The cursor is one row per channel type for the whole deployment. Start
	// from a known epoch so the window this test reasons about is the window
	// the reconciler uses.
	if _, err := pool.Exec(ctx, `DELETE FROM channel_outbound_reconcile_state WHERE channel_type = 'wecom'`); err != nil {
		t.Fatalf("reset reconcile cursor: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_outbound_reconcile_state WHERE channel_type = 'wecom'`)
	})

	epoch := time.Now()
	f := seedAnsweredTurn(t, pool, epoch.Add(60*time.Second))
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM channel_outbound_queue WHERE installation_id = $1`, f.installationID)
	})

	producer, err := outbox.NewProducer(channelTypeWecom, queries, nil, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	// One sweep per reconciler: Run closes its own done channel, so an
	// instance is single-use. Run blocks, and its first sweep happens before
	// it starts polling, so a short context is a complete pass rather than a
	// race with one.
	sweep := func() {
		r, err := outbox.NewReconciler(outbox.ReconcilerConfig{
			ChannelType: channelTypeWecom,
			Queries:     queries,
			Producer:    producer,
			Builder:     NewReconcilePayloadBuilder(queries),
			// Far enough past the task's completion to clear the settle delay
			// the reconciler keeps between the window's trailing edge and now.
			Now:       func() time.Time { return epoch.Add(3 * time.Minute) },
			PollEvery: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewReconciler: %v", err)
		}
		runCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		r.Run(runCtx)
	}

	sweep()
	if n := queuedRepliesFor(t, pool, f.taskID); n != 1 {
		t.Fatalf("the reconciler enqueued %d replies for a completed run nobody had delivered, want 1 — "+
			"this test's fixture is not a candidate, so the second half of it proves nothing", n)
	}

	// Now the same turn, delivered the way the bubble delivers it: over the
	// socket, with the delivery recorded instead of enqueued.
	if _, err := pool.Exec(ctx, `DELETE FROM channel_outbound_queue WHERE source_id = $1`, util.UUIDToString(f.taskID)); err != nil {
		t.Fatalf("clear the control's row: %v", err)
	}
	row, err := queries.RecordChannelOutboundDelivered(ctx, db.RecordChannelOutboundDeliveredParams{
		InstallationID: f.installationID,
		WorkspaceID:    f.workspaceID,
		ChannelType:    channelTypeWecom,
		ChatSessionID:  f.sessionID,
		SourceKind:     sourceKindChatDone,
		SourceID:       util.UUIDToString(f.taskID),
		TargetChatID:   f.chatID,
		TargetChatType: int16(chatTypeSingleInt),
		MsgType:        msgTypeMarkdown,
	})
	if err != nil {
		t.Fatalf("record delivery: %v", err)
	}
	if row.Status != "sent" {
		t.Fatalf("recorded row status = %q, want %q — a row left 'queued' is not a record of a delivery, it is one more copy waiting to be sent",
			row.Status, "sent")
	}
	if !row.SentAt.Valid {
		t.Error("recorded row has no sent_at, so the retention sweep cannot age it out with the delivered rows")
	}

	sweep()
	if n := queuedRepliesFor(t, pool, f.taskID); n != 0 {
		t.Fatalf("the reconciler enqueued %d replies for a run whose answer the bubble had already delivered: "+
			"the user reads the answer in the bubble and reads it again as an ordinary message about a minute later", n)
	}
}
