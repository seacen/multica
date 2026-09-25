package wecom

// invoke_permission_db_test.go — a bound workspace member who may NOT invoke
// the agent behind a WeCom bot gets nothing out of messaging it: no run, no
// stored turn, no issue.
//
// The web chat refuses that member up front (handler/chat.go, canInvokeAgent:
// a private agent runs only for its owner, with no admin bypass). The channel
// pipeline only checked the binding and workspace membership, so any member
// who bound their WeCom identity could run someone else's private agent on the
// owner's machine, with the owner's credentials.
//
// Driven over the real path: engine.Router, the real wecom ResolverSet, the
// real ChatSession and the real TaskService on Postgres. The only doubles are
// the issue creator and the replier, which record what reached them.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// seedBoundNonOwner adds a regular member to the fixture's workspace and binds
// their WeCom identity to the fixture's installation — everything the identity
// resolver asks for, and nothing that makes them the agent's owner.
func seedBoundNonOwner(t *testing.T, pool *pgxpool.Pool, f mediaBindFixture) (pgtype.UUID, string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	senderID := fmt.Sprintf("T-other-%d", suffix)
	var userID pgtype.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id`,
		"wecom non-owner", fmt.Sprintf("wecom-non-owner-%d@multica.test", suffix)).Scan(&userID); err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() {
		cleanup := context.Background()
		_, _ = pool.Exec(cleanup, `DELETE FROM "user" WHERE id = $1`, userID)
		// No foreign keys on the channel tables, so the workspace delete in
		// the base fixture's cleanup does not reach these.
		for _, table := range []string{"channel_user_binding", "channel_chat_session_binding", "channel_task_delivery", "channel_inbound_audit"} {
			_, _ = pool.Exec(cleanup, `DELETE FROM `+table+` WHERE installation_id = $1`, f.installationID)
		}
		_, _ = pool.Exec(cleanup, `DELETE FROM channel_installation WHERE id = $1`, f.installationID)
	})
	if _, err := pool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'member')`,
		f.workspaceID, userID); err != nil {
		t.Fatalf("create member: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO channel_user_binding (workspace_id, multica_user_id, installation_id, channel_type, channel_user_id)
		VALUES ($1, $2, $3, 'wecom', $4)`,
		f.workspaceID, userID, f.installationID, senderID); err != nil {
		t.Fatalf("create user binding: %v", err)
	}
	return userID, senderID
}

// wecomTextCallback builds an inbound text message through the real frame
// decoder. A group message carries the @-mention the way WeCom delivers it.
func wecomTextCallback(t *testing.T, botID, chatType, chatID, senderID, msgID, text string) channel.InboundMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"msgid":    msgID,
		"aibotid":  botID,
		"chattype": chatType,
		"chatid":   chatID,
		"from":     map[string]any{"userid": senderID},
		"msgtype":  "text",
		"text":     map[string]any{"content": text},
	})
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	var mc aibotMsgCallback
	if err := json.Unmarshal(raw, &mc); err != nil {
		t.Fatalf("decode callback: %v", err)
	}
	body, ok := mc.ownText()
	if !ok {
		t.Fatal("text callback is not routable; the fixture is wrong")
	}
	return channelMessageFromCallback(botID, "", mc, body, "req-invoke-1")
}

// recordingReplier keeps every verdict the Router handed the outbound side.
// Replies run on the Router's detached goroutine; Drain joins them.
type recordingReplier struct {
	mu       sync.Mutex
	outcomes []engine.Outcome
}

func (r *recordingReplier) Reply(_ context.Context, _ engine.ResolvedInstallation, _ channel.InboundMessage, res engine.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes = append(r.outcomes, res.Outcome)
}

func (r *recordingReplier) last() engine.Outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.outcomes) == 0 {
		return ""
	}
	return r.outcomes[len(r.outcomes)-1]
}

func TestInboundFromMemberWhoMayNotInvokeTheAgentStartsNothing(t *testing.T) {
	cases := []struct {
		name     string
		chatType string
		text     string
	}{
		{name: "direct message", chatType: "single", text: "run the deploy script"},
		// The group route is owned by the installer, who IS the agent owner
		// here. Judging the installer instead of the sender would let this
		// through.
		{name: "group mention", chatType: "group", text: "@Bot run the deploy script"},
		{name: "issue command", chatType: "single", text: "/issue run the deploy script"},
		{name: "new chat command", chatType: "single", text: "/new run the deploy script"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := mediaBindTestDB(t)
			f := seedMediaBindFixture(t, pool)
			_, otherSender := seedBoundNonOwner(t, pool, f)
			ctx := context.Background()
			queries := db.New(pool)

			issues := &recordingIssues{}
			replies := &recordingReplier{}
			router := engine.NewRouter(issues, service.NewTaskService(queries, pool, nil, events.New()), queries, engine.RouterConfig{
				Logger: testLogger(),
			})
			session := engine.NewChatSession(queries, pool, TypeWecom, engine.SessionTitles{
				Group: "群聊", Direct: "单聊", Fallback: "会话",
			})
			router.Register(TypeWecom, NewResolverSet(NewStore(queries), session, replies, nil))

			chatID := otherSender
			if tc.chatType == "group" {
				chatID = fmt.Sprintf("GROUP-%d", time.Now().UnixNano())
			}
			msgID := fmt.Sprintf("MSGID-INVOKE-%d", time.Now().UnixNano())
			if err := router.Handle(ctx, wecomTextCallback(t, f.botID, tc.chatType, chatID, otherSender, msgID, tc.text)); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			if !router.Drain(ctx) {
				t.Fatal("drain timed out")
			}

			var tasks, messages int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE agent_id = $1`, f.agentID).Scan(&tasks); err != nil {
				t.Fatalf("count tasks: %v", err)
			}
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM chat_message cm
				JOIN chat_session cs ON cs.id = cm.chat_session_id
				WHERE cs.workspace_id = $1`, f.workspaceID).Scan(&messages); err != nil {
				t.Fatalf("count chat messages: %v", err)
			}
			if tasks != 0 {
				t.Errorf("tasks enqueued for the private agent = %d, want 0 — a member who is not its owner ran it", tasks)
			}
			if messages != 0 {
				t.Errorf("chat messages stored = %d, want 0 — the denied turn landed in the owner's chat and the next run would read it", messages)
			}
			if n := len(issues.filed()); n != 0 {
				t.Errorf("issues created = %d, want 0 — /issue assigned the private agent on behalf of a member who may not invoke it", n)
			}
			// Refused, not silently lost: the replier is told, and the drop
			// audit says why.
			if got := replies.last(); got != engine.OutcomeInvokeDenied {
				t.Errorf("outcome handed to the replier = %q, want %q", got, engine.OutcomeInvokeDenied)
			}
			var reason string
			if err := pool.QueryRow(ctx, `
				SELECT drop_reason FROM channel_inbound_audit
				WHERE installation_id = $1 AND channel_message_id = $2`, f.installationID, msgID).Scan(&reason); err != nil {
				t.Fatalf("load drop audit: %v", err)
			}
			if reason != string(engine.DropReasonInvokeDenied) {
				t.Errorf("drop audit reason = %q, want %q", reason, engine.DropReasonInvokeDenied)
			}
		})
	}
}

// TestInboundFromAgentOwnerStillRuns keeps the test above honest: the same rig,
// with the owner as the sender, does enqueue a run.
func TestInboundFromAgentOwnerStillRuns(t *testing.T) {
	pool := mediaBindTestDB(t)
	f := seedMediaBindFixture(t, pool)
	seedBoundNonOwner(t, pool, f)
	ctx := context.Background()
	queries := db.New(pool)

	router := engine.NewRouter(&recordingIssues{}, service.NewTaskService(queries, pool, nil, events.New()), queries, engine.RouterConfig{
		Logger: testLogger(),
	})
	session := engine.NewChatSession(queries, pool, TypeWecom, engine.SessionTitles{
		Group: "群聊", Direct: "单聊", Fallback: "会话",
	})
	router.Register(TypeWecom, NewResolverSet(NewStore(queries), session, &recordingReplier{}, nil))

	msgID := fmt.Sprintf("MSGID-OWNER-%d", time.Now().UnixNano())
	if err := router.Handle(ctx, wecomTextCallback(t, f.botID, "single", f.senderID, f.senderID, msgID, "run the deploy script")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if !router.Drain(ctx) {
		t.Fatal("drain timed out")
	}
	var tasks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE agent_id = $1`, f.agentID).Scan(&tasks); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if tasks != 1 {
		t.Fatalf("tasks enqueued for the owner's own message = %d, want 1", tasks)
	}
}
