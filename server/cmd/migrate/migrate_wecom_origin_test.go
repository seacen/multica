package main

// migrate_wecom_origin_test.go — guards the one failure that takes the whole
// backend down rather than degrading something: DingTalk's migration 259
// rebuilds issue_origin_type_check, and a rebuild restates the entire allowed
// list. That list was written before this fork's 'wecom_chat' existed, so on a
// deployment carrying real WeCom-created issues, 260's VALIDATE finds rows the
// narrowed constraint forbids and fails. The migration runner stops at the
// first failure, so migration 261 — which puts 'wecom_chat' back — is never
// reached, and the server refuses to start on every restart afterwards.
//
// This is not hypothetical: it took the self-hosted deployment down on
// 2026-08-06, on a database with two wecom_chat issues.
//
// The test drives the REAL migration files and the REAL production hook map,
// so it fails if either the hook registration or the constraint list drifts.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dingtalkOriginVersion         = "259_issue_origin_dingtalk_chat"
	dingtalkOriginValidateVersion = "260_issue_origin_dingtalk_chat_validate"
	wecomOriginVersion            = "261_issue_origin_wecom_chat"
	wecomOriginValidateVersion    = "262_issue_origin_wecom_chat_validate"
)

// originMigrationFiles is the four-file run in the order the runner sees them.
func originMigrationFiles() []string {
	out := make([]string, 0, 4)
	for _, v := range []string{
		dingtalkOriginVersion, dingtalkOriginValidateVersion,
		wecomOriginVersion, wecomOriginValidateVersion,
	} {
		out = append(out, "../../migrations/"+v+".up.sql")
	}
	return out
}

// newOriginFixture builds a private schema holding just enough of `issue` for
// the constraint, seeds one wecom_chat row, and returns a pool whose sessions
// resolve unqualified `issue` to it. The pool matters: the hook runs on the
// pool and names the table unqualified, exactly as it does in production.
func newOriginFixture(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	admin := openTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := fmt.Sprintf("migrate_wecom_origin_%d_%d", time.Now().UnixNano(), rand.Uint32())
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schemaIdent); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(c, "DROP SCHEMA IF EXISTS "+schemaIdent+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	})

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://multica:multica@localhost:5432/multica?sslmode=disable"
	}
	sep := "?"
	if strings.Contains(dbURL, "?") {
		sep = "&"
	}
	pool, err := pgxpool.New(ctx, dbURL+sep+"search_path="+url.QueryEscape(schema))
	if err != nil {
		t.Fatalf("open scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `CREATE TABLE issue (
		id BIGSERIAL PRIMARY KEY,
		origin_type TEXT
	)`); err != nil {
		t.Fatalf("create issue stand-in: %v", err)
	}
	// The pre-DingTalk constraint, as any deployment running the WeCom adapter
	// already has it.
	if _, err := pool.Exec(ctx, `ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
		CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'wecom_chat'))`); err != nil {
		t.Fatalf("install pre-dingtalk constraint: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO issue (origin_type) VALUES ('wecom_chat'), ('agent_create')`); err != nil {
		t.Fatalf("seed a wecom-created issue: %v", err)
	}
	return pool, schema
}

func originRunOptions(schema string, hooks map[string]preMigrationHook) runOptions {
	return runOptions{
		Direction:             "up",
		Files:                 originMigrationFiles(),
		SchemaMigrationsTable: schema + ".schema_migrations",
		AdvisoryLockKey:       int64(rand.Uint64()&0x7fffffffffffffff) | 1,
		Hooks:                 hooks,
	}
}

// TestDingTalkOriginValidateStopsTheServerWithoutTheWecomHook is the failure
// itself: with no hook, the run dies inside 260 and never reaches 261.
func TestDingTalkOriginValidateStopsTheServerWithoutTheWecomHook(t *testing.T) {
	pool, schema := newOriginFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := runMigrations(ctx, pool, originRunOptions(schema, nil))
	if err == nil {
		t.Fatal("migration run succeeded without the hook; the ordering trap this guards is gone — " +
			"if DingTalk's 259 now carries wecom_chat, delete the hook and this test together")
	}
	if !strings.Contains(err.Error(), dingtalkOriginValidateVersion) {
		t.Fatalf("run failed somewhere unexpected: %v", err)
	}
	// And the damage is what makes it fatal rather than cosmetic: 261 never ran.
	assertMigrationRecorded(t, pool, schema, wecomOriginVersion, false)
}

// TestWecomOriginHookLetsTheDingTalkValidatePass is the fix: the production
// hook map carries the run through to the end, with both origin types allowed.
func TestWecomOriginHookLetsTheDingTalkValidatePass(t *testing.T) {
	pool, schema := newOriginFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if preMigrationHooks[dingtalkOriginValidateVersion] == nil {
		t.Fatalf("production hook is not registered for %s", dingtalkOriginValidateVersion)
	}
	if err := runMigrations(ctx, pool, originRunOptions(schema, preMigrationHooks)); err != nil {
		t.Fatalf("migration run with the production hooks: %v", err)
	}

	for _, v := range []string{
		dingtalkOriginVersion, dingtalkOriginValidateVersion,
		wecomOriginVersion, wecomOriginValidateVersion,
	} {
		assertMigrationRecorded(t, pool, schema, v, true)
	}

	var def string
	var validated bool
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(c.oid), c.convalidated
		FROM pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
		WHERE n.nspname = $1 AND c.conname = 'issue_origin_type_check'
	`, schema).Scan(&def, &validated); err != nil {
		t.Fatalf("read final constraint: %v", err)
	}
	if !validated {
		t.Fatalf("constraint left NOT VALID: %s", def)
	}
	for _, want := range []string{"'dingtalk_chat'", "'wecom_chat'", "'lark_chat'", "'slack_chat'", "'agent_create'", "'autopilot'", "'quick_create'"} {
		if !strings.Contains(def, want) {
			t.Fatalf("final constraint dropped %s: %s", want, def)
		}
	}

	// The rows both adapters can write must still be insertable.
	for _, origin := range []string{"wecom_chat", "dingtalk_chat"} {
		if _, err := pool.Exec(ctx, `INSERT INTO issue (origin_type) VALUES ($1)`, origin); err != nil {
			t.Fatalf("insert %s issue after migration: %v", origin, err)
		}
	}
}

func assertMigrationRecorded(t *testing.T, pool *pgxpool.Pool, schema, version string, want bool) {
	t.Helper()
	var got bool
	if err := pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM "+pgx.Identifier{schema, "schema_migrations"}.Sanitize()+" WHERE version = $1)",
		version).Scan(&got); err != nil {
		t.Fatalf("read schema_migrations for %s: %v", version, err)
	}
	if got != want {
		t.Fatalf("migration %s recorded = %v, want %v", version, got, want)
	}
}
