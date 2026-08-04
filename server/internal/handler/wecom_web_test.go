package handler

// wecom_web_test.go — the four HTTP surfaces the WeCom smart-bot integration
// exposes: list, BYO install, revoke, and the binding-token redeem. Two things
// are worth pinning here. The install endpoints must never echo the secret
// back and must refuse an agent from another workspace. And redeem's failure
// modes have to keep their distinct status codes: the bind page shows a
// different message for each, and collapsing them into a generic 400 is how a
// user ends up re-clicking a token that will never work.
//
// Everything above the redeem tests needs the shared Postgres fixture (see
// TestMain — the whole package no-ops without a reachable database). The
// redeem tests drive a fake redeemer and touch no tables.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/wecom"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

// ---- redeem: no database needed ----

type fakeWecomRedeemer struct {
	result wecom.RedeemedBindingToken
	err    error

	gotToken string
	gotUser  pgtype.UUID
	calls    int
}

func (f *fakeWecomRedeemer) RedeemAndBind(_ context.Context, raw string, userID pgtype.UUID) (wecom.RedeemedBindingToken, error) {
	f.calls++
	f.gotToken, f.gotUser = raw, userID
	return f.result, f.err
}

// redeemRequest posts a redeem body. userID empty means an unauthenticated
// caller.
func redeemRequest(t *testing.T, h *Handler, userID string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if s, ok := body.(string); ok {
		req = httptest.NewRequest(http.MethodPost, "/api/wecom/binding/redeem", strings.NewReader(s))
	} else {
		buf, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		req = httptest.NewRequest(http.MethodPost, "/api/wecom/binding/redeem", strings.NewReader(string(buf)))
	}
	req.Header.Set("Content-Type", "application/json")
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}
	rec := httptest.NewRecorder()
	h.RedeemWecomBindingToken(rec, req)
	return rec
}

const redeemTestUser = "33333333-3333-3333-3333-333333333333"

// TestRedeemWecomBindingTokenStatusPerFailureMode is the whole point of the
// endpoint's error switch: three different things the user has to be told
// apart, plus the catch-all that must not leak internals.
func TestRedeemWecomBindingTokenStatusPerFailureMode(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{"token unknown, consumed or expired", wecom.ErrBindingTokenInvalid, http.StatusGone},
		{"this WeCom id belongs to another account", wecom.ErrBindingAlreadyAssigned, http.StatusConflict},
		{"redeemer is not in the workspace", wecom.ErrBindingNotWorkspaceMember, http.StatusForbidden},
		{"anything else", errors.New("connection refused"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeWecomRedeemer{err: c.err}
			h := &Handler{WecomBindingTokens: fake}

			rec := redeemRequest(t, h, redeemTestUser, RedeemWecomBindingTokenRequest{Token: "raw-token"})
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, c.wantStatus, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "connection refused") {
				t.Error("an internal error must not be echoed to the client")
			}
		})
	}
}

// TestRedeemWecomBindingTokenBindsTheSessionUser is the security property: the
// account bound is the one holding the session, never anything carried by the
// token or the request body. A stolen link binds the thief's own account.
func TestRedeemWecomBindingTokenBindsTheSessionUser(t *testing.T) {
	fake := &fakeWecomRedeemer{result: wecom.RedeemedBindingToken{
		WorkspaceID:    parseUUID("11111111-1111-1111-1111-111111111111"),
		InstallationID: parseUUID("22222222-2222-2222-2222-222222222222"),
		WecomUserID:    "T-alex",
	}}
	h := &Handler{WecomBindingTokens: fake}

	rec := redeemRequest(t, h, redeemTestUser, map[string]any{
		"token": "raw-token",
		// A caller trying to bind someone else's account:
		"user_id":       "44444444-4444-4444-4444-444444444444",
		"wecom_user_id": "T-someone-else",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
	}
	if fake.gotToken != "raw-token" {
		t.Errorf("redeemed token %q", fake.gotToken)
	}
	if uuidToString(fake.gotUser) != redeemTestUser {
		t.Errorf("bound user = %s, want the session's %s", uuidToString(fake.gotUser), redeemTestUser)
	}

	var out RedeemWecomBindingTokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.WorkspaceID != "11111111-1111-1111-1111-111111111111" ||
		out.InstallationID != "22222222-2222-2222-2222-222222222222" ||
		out.WecomUserID != "T-alex" {
		t.Errorf("response = %+v", out)
	}
}

// TestRedeemWecomBindingTokenRejectsBeforeTouchingTheService — every one of
// these must be turned away without a redeem attempt.
func TestRedeemWecomBindingTokenRejectsBeforeTouchingTheService(t *testing.T) {
	cases := []struct {
		name       string
		configured bool
		userID     string
		body       any
		wantStatus int
	}{
		{"integration not configured", false, redeemTestUser, RedeemWecomBindingTokenRequest{Token: "t"}, http.StatusServiceUnavailable},
		{"not signed in", true, "", RedeemWecomBindingTokenRequest{Token: "t"}, http.StatusUnauthorized},
		{"body is not json", true, redeemTestUser, "{not json", http.StatusBadRequest},
		{"no token", true, redeemTestUser, RedeemWecomBindingTokenRequest{}, http.StatusBadRequest},
		{"user id is not a uuid", true, "not-a-uuid", RedeemWecomBindingTokenRequest{Token: "t"}, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeWecomRedeemer{}
			h := &Handler{}
			if c.configured {
				h.WecomBindingTokens = fake
			}
			rec := redeemRequest(t, h, c.userID, c.body)
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, c.wantStatus, rec.Body.String())
			}
			if fake.calls != 0 {
				t.Error("the redeem service must not be reached")
			}
		})
	}
}

// ---- list / install / revoke, unconfigured ----

// TestWecomEndpointsWhenTheIntegrationIsOff — list degrades to an empty,
// flagged response so the Settings tab can render "not configured"; the two
// write endpoints refuse outright.
func TestWecomEndpointsWhenTheIntegrationIsOff(t *testing.T) {
	h := &Handler{}

	rec := httptest.NewRecorder()
	req := withURLParam(httptest.NewRequest(http.MethodGet, "/api/workspaces/x/wecom/installations", nil),
		"id", "11111111-1111-1111-1111-111111111111")
	h.ListWecomInstallations(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", rec.Code)
	}
	var listed struct {
		Installations    []WecomInstallationResponse `json:"installations"`
		Configured       bool                        `json:"configured"`
		InstallSupported bool                        `json:"install_supported"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if listed.Configured || listed.InstallSupported {
		t.Errorf("an unconfigured deployment must report configured=false: %+v", listed)
	}
	if listed.Installations == nil {
		t.Error("installations must be an empty array, not null — the UI maps over it")
	}

	rec = httptest.NewRecorder()
	h.RegisterWecomBYO(rec, withURLParam(newWecomInstallRequest("wb-1", "s3cret"), "id", "11111111-1111-1111-1111-111111111111"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("install status = %d, want 503", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = withURLParam(httptest.NewRequest(http.MethodDelete, "/api/workspaces/x/wecom/installations/y", nil),
		"id", "11111111-1111-1111-1111-111111111111")
	h.RevokeWecomInstallation(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("revoke status = %d, want 503", rec.Code)
	}
}

func newWecomInstallRequest(botID, secret string) *http.Request {
	body, _ := json.Marshal(RegisterWecomBYORequest{BotID: botID, Secret: secret})
	req := httptest.NewRequest(http.MethodPost, "/api/workspaces/x/wecom/install/byo", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", testUserID)
	return req
}

// ---- list / install / revoke, against the shared fixture ----

// wecomTestHandler wires the shared test handler for wecom and restores it
// afterwards, so the rest of the suite keeps seeing the integration as off.
func wecomTestHandler(t *testing.T) *Handler {
	t.Helper()
	if testHandler == nil {
		t.Skip("no database fixture")
	}
	key := make([]byte, secretbox.KeySize)
	for i := range key {
		key[i] = byte(i + 7)
	}
	box, err := secretbox.New(key)
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	creds, err := wecom.NewSecretboxCredentialsResolver(box)
	if err != nil {
		t.Fatalf("credentials resolver: %v", err)
	}

	prevStore, prevCreds, prevRouter := testHandler.WecomStore, testHandler.WecomCredentials, testHandler.ChannelRouter
	testHandler.WecomStore = wecom.NewStore(testHandler.Queries)
	testHandler.WecomCredentials = creds
	if testHandler.ChannelRouter == nil {
		testHandler.ChannelRouter = &engine.Router{}
	}
	t.Cleanup(func() {
		testHandler.WecomStore, testHandler.WecomCredentials, testHandler.ChannelRouter = prevStore, prevCreds, prevRouter
	})
	return testHandler
}

func decodeWecomInstallation(t *testing.T, body []byte) WecomInstallationResponse {
	t.Helper()
	var out WecomInstallationResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode installation: %v", err)
	}
	return out
}

// TestWecomInstallListRevokeRoundTrip walks the three endpoints the Settings
// tab drives, in the order an operator drives them.
func TestWecomInstallListRevokeRoundTrip(t *testing.T) {
	h := wecomTestHandler(t)
	agentID := createHandlerTestAgent(t, "WeCom Round Trip Agent", []byte(`{}`))
	botID := "wb-roundtrip-" + agentID[:8]
	t.Cleanup(func() {
		testPool.Exec(context.Background(),
			`DELETE FROM channel_installation WHERE channel_type = 'wecom' AND agent_id = $1`, agentID)
	})

	// install
	rec := httptest.NewRecorder()
	req := newWecomInstallRequest(botID, "s3cret-plaintext")
	req.URL.RawQuery = "agent_id=" + agentID
	h.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))
	if rec.Code != http.StatusOK {
		t.Fatalf("install status = %d, body %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "s3cret-plaintext") {
		t.Fatal("the install response echoed the secret back")
	}
	created := decodeWecomInstallation(t, rec.Body.Bytes())
	if created.BotID != botID || created.Status != string(wecom.InstallationActive) {
		t.Fatalf("created = %+v", created)
	}
	if created.AgentID != agentID || created.WorkspaceID != testWorkspaceID {
		t.Fatalf("created scoped to %+v", created)
	}

	// the stored row must not hold the plaintext secret
	var config []byte
	if err := testPool.QueryRow(context.Background(),
		`SELECT config FROM channel_installation WHERE id = $1`, created.ID).Scan(&config); err != nil {
		t.Fatalf("read stored config: %v", err)
	}
	if strings.Contains(string(config), "s3cret-plaintext") {
		t.Fatal("the secret was stored in the clear")
	}

	// list
	rec = httptest.NewRecorder()
	listReq := withURLParam(httptest.NewRequest(http.MethodGet, "/api/workspaces/x/wecom/installations", nil), "id", testWorkspaceID)
	listReq.Header.Set("X-User-ID", testUserID)
	h.ListWecomInstallations(rec, listReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, body %s", rec.Code, rec.Body.String())
	}
	var listed struct {
		Installations []WecomInstallationResponse `json:"installations"`
		Configured    bool                        `json:"configured"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if !listed.Configured {
		t.Error("a wired deployment must report configured=true")
	}
	found := false
	for _, inst := range listed.Installations {
		if inst.ID == created.ID {
			found = true
			if inst.BotID != botID {
				t.Errorf("listed bot id = %q", inst.BotID)
			}
		}
	}
	if !found {
		t.Fatalf("the new installation is missing from the list: %+v", listed.Installations)
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Error("the list response must carry no secret field at all")
	}

	// revoke
	rec = httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/api/workspaces/x/wecom/installations/y", nil)
	delReq.Header.Set("X-User-ID", testUserID)
	delReq = withURLParams(delReq, "id", testWorkspaceID, "installationId", created.ID)
	h.RevokeWecomInstallation(rec, delReq)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d, body %s", rec.Code, rec.Body.String())
	}

	var status string
	if err := testPool.QueryRow(context.Background(),
		`SELECT status FROM channel_installation WHERE id = $1`, created.ID).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != string(wecom.InstallationRevoked) {
		t.Fatalf("status after revoke = %q, want revoked (the row must survive so a re-install reuses it)", status)
	}
}

// TestWecomInstallRejectsAnAgentOutsideTheWorkspace — the workspace in the URL
// is authorised by the router middleware; the agent in the query string is not,
// so this handler has to check it itself.
func TestWecomInstallRejectsAnAgentOutsideTheWorkspace(t *testing.T) {
	h := wecomTestHandler(t)

	otherWorkspace := "55555555-5555-5555-5555-555555555555"
	agentID := createHandlerTestAgent(t, "WeCom Foreign Agent", []byte(`{}`))

	rec := httptest.NewRecorder()
	req := newWecomInstallRequest("wb-foreign", "s")
	req.URL.RawQuery = "agent_id=" + agentID
	h.RegisterWecomBYO(rec, withURLParam(req, "id", otherWorkspace))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for an agent in another workspace (body %s)", rec.Code, rec.Body.String())
	}
}

// TestWecomInstallRequiresItsInputs — the argument checks, each with its own
// status so the UI can say what is missing.
func TestWecomInstallRequiresItsInputs(t *testing.T) {
	h := wecomTestHandler(t)
	agentID := createHandlerTestAgent(t, "WeCom Input Check Agent", []byte(`{}`))

	cases := []struct {
		name       string
		query      string
		botID      string
		secret     string
		wantStatus int
	}{
		{"no agent_id", "", "wb-1", "s", http.StatusBadRequest},
		{"agent_id is not a uuid", "agent_id=nope", "wb-1", "s", http.StatusBadRequest},
		{"no bot id", "agent_id=" + agentID, "", "s", http.StatusBadRequest},
		{"no secret", "agent_id=" + agentID, "wb-1", "", http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := newWecomInstallRequest(c.botID, c.secret)
			req.URL.RawQuery = c.query
			h.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, c.wantStatus, rec.Body.String())
			}
		})
	}
}

// TestWecomRevokeRefusesAnotherWorkspacesInstallation — GetInWorkspace is the
// tenancy gate; without it an admin could revoke by guessing a uuid.
func TestWecomRevokeRefusesAnotherWorkspacesInstallation(t *testing.T) {
	h := wecomTestHandler(t)
	agentID := createHandlerTestAgent(t, "WeCom Tenancy Agent", []byte(`{}`))
	botID := "wb-tenancy-" + agentID[:8]
	t.Cleanup(func() {
		testPool.Exec(context.Background(),
			`DELETE FROM channel_installation WHERE channel_type = 'wecom' AND agent_id = $1`, agentID)
	})

	rec := httptest.NewRecorder()
	req := newWecomInstallRequest(botID, "s")
	req.URL.RawQuery = "agent_id=" + agentID
	h.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))
	if rec.Code != http.StatusOK {
		t.Fatalf("install status = %d, body %s", rec.Code, rec.Body.String())
	}
	created := decodeWecomInstallation(t, rec.Body.Bytes())

	rec = httptest.NewRecorder()
	delReq := httptest.NewRequest(http.MethodDelete, "/api/workspaces/x/wecom/installations/y", nil)
	delReq.Header.Set("X-User-ID", testUserID)
	delReq = withURLParams(delReq,
		"id", "55555555-5555-5555-5555-555555555555",
		"installationId", created.ID)
	h.RevokeWecomInstallation(rec, delReq)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when the installation belongs to another workspace", rec.Code)
	}

	var status string
	if err := testPool.QueryRow(context.Background(),
		`SELECT status FROM channel_installation WHERE id = $1`, created.ID).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != string(wecom.InstallationActive) {
		t.Fatalf("the installation was revoked across a workspace boundary (status %q)", status)
	}
}

// TestWecomEndpointsWithoutAnEncryptionKey pins the deliberate fallback: the
// install service is only built from the real secretbox resolver, so a
// deployment without the key reports 503 instead of writing a row it could
// never decrypt.
func TestWecomEndpointsWithoutAnEncryptionKey(t *testing.T) {
	if testHandler == nil {
		t.Skip("no database fixture")
	}
	prevStore, prevCreds, prevRouter := testHandler.WecomStore, testHandler.WecomCredentials, testHandler.ChannelRouter
	testHandler.WecomStore = wecom.NewStore(testHandler.Queries)
	testHandler.WecomCredentials = stubWecomCredentials{}
	if testHandler.ChannelRouter == nil {
		testHandler.ChannelRouter = &engine.Router{}
	}
	t.Cleanup(func() {
		testHandler.WecomStore, testHandler.WecomCredentials, testHandler.ChannelRouter = prevStore, prevCreds, prevRouter
	})

	rec := httptest.NewRecorder()
	req := newWecomInstallRequest("wb-1", "s")
	req.URL.RawQuery = "agent_id=" + createHandlerTestAgent(t, "WeCom No Key Agent", []byte(`{}`))
	testHandler.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("install status = %d, want 503 without a usable encryption key", rec.Code)
	}
}

type stubWecomCredentials struct{}

func (stubWecomCredentials) Credentials(wecom.Installation) (wecom.InstallationCredentials, error) {
	return wecom.InstallationCredentials{}, errors.New("stub")
}

// TestWecomInstallSameBotOnASecondAgentConflicts pins the failure an admin is
// most likely to hit second: the bot is already connected, and they try it on
// another agent because that is the obvious way to give a second agent a WeCom
// presence.
//
// UpsertChannelInstallation conflicts on (workspace_id, agent_id, channel_type),
// so this request misses ON CONFLICT and trips
// idx_channel_installation_type_appid — UNIQUE on (channel_type, app_id) —
// instead. The handler used to return err.Error() verbatim as a 400, so what
// reached the toast was `duplicate key value violates unique constraint
// "idx_channel_installation_type_appid"`. It has to be a 409 that says where
// the bot is.
func TestWecomInstallSameBotOnASecondAgentConflicts(t *testing.T) {
	h := wecomTestHandler(t)
	firstAgent := createHandlerTestAgent(t, "WeCom Bot Holder", []byte(`{}`))
	secondAgent := createHandlerTestAgent(t, "WeCom Bot Claimant", []byte(`{}`))
	botID := "wb-conflict-" + firstAgent[:8]
	t.Cleanup(func() {
		testPool.Exec(context.Background(),
			`DELETE FROM channel_installation WHERE channel_type = 'wecom' AND agent_id = ANY($1)`,
			[]string{firstAgent, secondAgent})
	})

	rec := httptest.NewRecorder()
	req := newWecomInstallRequest(botID, "s3cret-plaintext")
	req.URL.RawQuery = "agent_id=" + firstAgent
	h.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))
	if rec.Code != http.StatusOK {
		t.Fatalf("first install status = %d, body %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = newWecomInstallRequest(botID, "s3cret-plaintext")
	req.URL.RawQuery = "agent_id=" + secondAgent
	h.RegisterWecomBYO(rec, withURLParam(req, "id", testWorkspaceID))

	if rec.Code != http.StatusConflict {
		t.Fatalf("second install status = %d, want 409; body %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "duplicate key") || strings.Contains(body, "idx_channel_installation") {
		t.Fatalf("the raw Postgres error reached the caller: %s", body)
	}
	if !strings.Contains(body, "another agent in this workspace") {
		t.Fatalf("the message does not say where the bot is: %s", body)
	}
	// The sentence is the fallback; the code is what a localized client
	// renders. Without it the settings tab can only toast the server's
	// English at an admin who set their profile to something else.
	var decoded struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if decoded.Code != "wecom_bot_owned_by_same_workspace" {
		t.Fatalf("code = %q, want a stable identifier the UI can localize", decoded.Code)
	}
}
