package wecom

// installation_swap_test.go — swapping the bot on an agent must not leave the
// previous bot's identities behind (#6547).
//
// UpsertChannelInstallation conflicts on (workspace_id, agent_id, channel_type),
// so pointing an agent at a different bot REUSES the same installation row and
// the same installation_id. Everything keyed on that id therefore survives a
// change that made all of it meaningless: WeCom anonymises aibot userids per
// (bot, user), which is the premise the whole binding flow rests on, so a
// channel_user_binding written under bot X names somebody bot Y has never heard
// of. The visible symptom is an inbox push addressed to a bot-X userid over bot
// Y's connection — the message goes nowhere and the user is never told.
//
// Driven through InstallationService.Upsert, the only write path to a wecom row
// and the one #6581's scan-code install worker also binds through, against a
// real migrated database like installation_reclaim_test.go; skips when none is
// reachable.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	wcSwapBotOld = "bot_swap_old"
	wcSwapBotNew = "bot_swap_new"

	// The colleague who bound their account by messaging the OLD bot. The
	// userid is anonymised per (bot, user), so it is meaningless to the new one.
	wcSwapChannelUser = "wmOldBotAnonymisedUserId"
	wcSwapChatID      = "wrOldBotChatId"

	// A drop_reason only this file writes. The swap DETACHES audit rows rather
	// than deleting them, so they outlive their installation_id and cannot be
	// cleaned up by joining back to it — without a marker of their own they
	// accumulate across runs and make the detach assertion pass on its own
	// leftovers.
	wcSwapAuditReason = "swap_test_marker"
)

func cleanSwap(ctx context.Context, pool *pgxpool.Pool) {
	apps := []string{wcSwapBotOld, wcSwapBotNew}
	_, _ = pool.Exec(ctx, `
DELETE FROM channel_user_binding WHERE installation_id IN
    (SELECT id FROM channel_installation WHERE config->>'app_id' = ANY($1))`, apps)
	_, _ = pool.Exec(ctx, `
DELETE FROM channel_chat_session_binding WHERE installation_id IN
    (SELECT id FROM channel_installation WHERE config->>'app_id' = ANY($1))`, apps)
	_, _ = pool.Exec(ctx, `
DELETE FROM channel_binding_token WHERE installation_id IN
    (SELECT id FROM channel_installation WHERE config->>'app_id' = ANY($1))`, apps)
	_, _ = pool.Exec(ctx, `
DELETE FROM channel_inbound_message_dedup WHERE installation_id IN
    (SELECT id FROM channel_installation WHERE config->>'app_id' = ANY($1))`, apps)
	// Detached rows have no installation_id left to join on, so they are
	// cleaned by this file's own marker instead.
	_, _ = pool.Exec(ctx, `DELETE FROM channel_inbound_audit WHERE drop_reason = $1`, wcSwapAuditReason)
	_, _ = pool.Exec(ctx, `DELETE FROM channel_installation WHERE config->>'app_id' = ANY($1)`, apps)
	_, _ = pool.Exec(ctx, `DELETE FROM workspace WHERE id = $1`, wcRclWS)
}

func setupSwap(t *testing.T) (context.Context, *pgxpool.Pool, *InstallationService) {
	t.Helper()
	pool := reclaimTestDB(t)
	ctx := context.Background()
	cleanSwap(ctx, pool)
	seedReclaimOwners(t, ctx, pool)
	t.Cleanup(func() { cleanSwap(ctx, pool) })

	box, err := secretbox.New(make([]byte, secretbox.KeySize))
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	svc, err := NewInstallationService(db.New(pool), pool, box, WithCredentialProbe(&fakeProbe{}))
	if err != nil {
		t.Fatalf("NewInstallationService: %v", err)
	}
	return ctx, pool, svc
}

// seedBindingsFor writes the rows a member accumulates by using the bot: an
// identity binding (their anonymised userid), a chat-session binding (where the
// conversation lives), a pending link token, an inbound dedup entry and an
// audit row. Every one of them is keyed on installation_id.
func seedBindingsFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, installID string) {
	t.Helper()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	exec(`INSERT INTO channel_user_binding
(workspace_id, multica_user_id, installation_id, channel_type, channel_user_id)
VALUES ($1, $2, $3, 'wecom', $4)`, wcRclWS, wcRclInstall, installID, wcSwapChannelUser)
	exec(`INSERT INTO channel_chat_session_binding
(chat_session_id, installation_id, channel_type, channel_chat_id, chat_type)
VALUES (gen_random_uuid(), $1, 'wecom', $2, 'p2p')`, installID, wcSwapChatID)
	exec(`INSERT INTO channel_binding_token
(token_hash, workspace_id, installation_id, channel_type, channel_user_id, expires_at)
VALUES ('swap-token-hash', $1, $2, 'wecom', $3, now() + interval '10 minutes')`,
		wcRclWS, installID, wcSwapChannelUser)
	exec(`INSERT INTO channel_inbound_message_dedup (installation_id, message_id)
VALUES ($1, 'swap-msg-1')`, installID)
	exec(`INSERT INTO channel_inbound_audit
(installation_id, channel_type, event_type, drop_reason)
VALUES ($1, 'wecom', 'msg', $2)`, installID, wcSwapAuditReason)
}

func countBy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table, installID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM `+table+` WHERE installation_id = $1`, installID).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func installIDOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, app string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx,
		`SELECT id::text FROM channel_installation WHERE channel_type = 'wecom' AND config->>'app_id' = $1`,
		app).Scan(&id); err != nil {
		t.Fatalf("read installation id for %s: %v", app, err)
	}
	return id
}

// The admin points the agent at a different bot. Every identity the previous
// bot accumulated has to go, because none of it names anybody the new bot can
// address.
func TestUpsert_SwappingTheBotClearsThePreviousBotsBindings(t *testing.T) {
	ctx, pool, svc := setupSwap(t)

	first, err := svc.Upsert(ctx, params(wcSwapBotOld, wcRclAgentA))
	if err != nil {
		t.Fatalf("connecting the old bot: %v", err)
	}
	oldID := installIDOf(t, ctx, pool, wcSwapBotOld)
	// A colleague used the old bot: bound their account, held a conversation.
	seedBindingsFor(t, ctx, pool, oldID)

	// The swap: same workspace, same agent, a different bot.
	second, err := svc.Upsert(ctx, params(wcSwapBotNew, wcRclAgentA))
	if err != nil {
		t.Fatalf("swapping to the new bot: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("the swap started a new installation row (%v -> %v); this test's premise is gone "+
			"and the bug it guards cannot happen the way it is described", first.ID, second.ID)
	}
	newID := installIDOf(t, ctx, pool, wcSwapBotNew)
	if newID != oldID {
		t.Fatalf("installation id changed %s -> %s; premise gone", oldID, newID)
	}

	if n := countBy(t, ctx, pool, "channel_user_binding", oldID); n != 0 {
		t.Errorf("%d user binding(s) written under the OLD bot survived the swap — the new bot would "+
			"push inbox messages to an old-bot anonymised userid over its own connection, and they "+
			"reach nobody", n)
	}
	if n := countBy(t, ctx, pool, "channel_chat_session_binding", oldID); n != 0 {
		t.Errorf("%d chat-session binding(s) from the OLD bot survived — replies would be addressed to "+
			"a chat id the new bot cannot post into", n)
	}
	if n := countBy(t, ctx, pool, "channel_binding_token", oldID); n != 0 {
		t.Errorf("%d pending binding token(s) from the OLD bot survived — redeeming one would bind a "+
			"user to a userid the new bot never issued", n)
	}
	if n := countBy(t, ctx, pool, "channel_inbound_message_dedup", oldID); n != 0 {
		t.Errorf("%d inbound dedup row(s) from the OLD bot survived — a new-bot message id that collides "+
			"would be silently swallowed as a redelivery", n)
	}

	// The audit trail keeps its rows and only loses the reference: losing the
	// record of what arrived is worse than a dangling installation_id.
	var audits int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM channel_inbound_audit WHERE installation_id IS NULL AND drop_reason = $1`,
		wcSwapAuditReason).Scan(&audits); err != nil {
		t.Fatalf("count detached audit: %v", err)
	}
	if audits != 1 {
		t.Errorf("detached audit rows = %d, want the row kept with a NULL installation_id — an operator "+
			"loses the record of what arrived otherwise", audits)
	}
}

// Rotating the secret on the SAME bot is not a swap. The anonymised userids are
// a property of the bot, not of its credentials, so every binding stays — and
// clearing them here would silently unbind everybody who uses the bot.
func TestUpsert_RotatingTheSameBotClearsNothing(t *testing.T) {
	ctx, pool, svc := setupSwap(t)

	if _, err := svc.Upsert(ctx, params(wcSwapBotOld, wcRclAgentA)); err != nil {
		t.Fatalf("connecting the bot: %v", err)
	}
	id := installIDOf(t, ctx, pool, wcSwapBotOld)
	seedBindingsFor(t, ctx, pool, id)

	p := params(wcSwapBotOld, wcRclAgentA)
	p.Secret = "rotated-secret"
	if _, err := svc.Upsert(ctx, p); err != nil {
		t.Fatalf("rotating the secret: %v", err)
	}

	if n := countBy(t, ctx, pool, "channel_user_binding", id); n != 1 {
		t.Errorf("user bindings after a plain secret rotation = %d, want 1 — a rotation that unbinds "+
			"everybody makes every member re-link before the bot answers them again", n)
	}
	if n := countBy(t, ctx, pool, "channel_chat_session_binding", id); n != 1 {
		t.Errorf("chat-session bindings after a plain secret rotation = %d, want 1 — the running "+
			"conversation would lose its thread", n)
	}
	if n := countBy(t, ctx, pool, "channel_inbound_message_dedup", id); n != 1 {
		t.Errorf("dedup rows after a plain secret rotation = %d, want 1 — a redelivery arriving across "+
			"the rotation would be processed twice", n)
	}
	if n := countBy(t, ctx, pool, "channel_inbound_audit", id); n != 1 {
		t.Errorf("audit rows still attached after a plain secret rotation = %d, want 1", n)
	}
}

// A FIRST install on an agent that has no installation at all must not try to
// clear anything: there is no previous bot, and `carried` is the zero value.
func TestUpsert_FirstInstallClearsNothing(t *testing.T) {
	ctx, pool, svc := setupSwap(t)

	// Agent B is untouched by the other tests in this file.
	if _, err := svc.Upsert(ctx, params(wcSwapBotNew, wcRclAgentB)); err != nil {
		t.Fatalf("first install on a fresh agent: %v", err)
	}
	id := installIDOf(t, ctx, pool, wcSwapBotNew)
	if n := countBy(t, ctx, pool, "channel_user_binding", id); n != 0 {
		t.Errorf("a first install found %d bindings; the fixture is wrong", n)
	}
}
