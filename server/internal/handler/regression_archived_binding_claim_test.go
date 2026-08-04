package handler

// regression_archived_binding_claim_test.go — guards the join the claim
// handler's binding lookup never makes: the row it reads to describe the room
// is not the handle the answer is delivered on, and the two can disagree.
//
// A WeCom group message binds a chat_session to the room, and the typing
// indicator captures that room's chatid in memory at the same moment. The
// in-memory handle is what the reply is written to; the binding row is only
// what the claim reads. Archiving the session (PATCH /archive) deletes the
// binding row in the same transaction and cancels nothing, and ClaimAgentTask
// does not look at chat_session.status — so a turn queued a moment earlier is
// still handed out, and the binding read behind the `berr == nil` guard in
// buildClaimedTaskResponse now fails. Both chat_channel_type and chat_type come
// back empty, with no log line, which is indistinguishable from a web chat:
// the brief renders no `## Conversation Channel` block, AudienceOf("","")
// resolves to Direct, and the agent is told "a user is chatting with you
// directly". It answers for an audience of one — freely, by name, as if nobody
// else were reading — and the live handle posts that answer into the group room
// the message came from. The person who archived the session sees nothing; the
// room sees everything.
//
// Nothing else covers it. daemon_claim_channel_type_test.go seeds the binding
// and claims immediately, and its unbound-session case is a session that never
// had a binding — not one that lost it between enqueue and claim. That file's
// TestClaim_BoundSessionReportsRoomShape is the control for this test: the same
// session, unarchived, reports wecom/group.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// archiveChatSessionViaAPI archives the session through the endpoint the owner's
// "Archive" click hits, so the binding is severed exactly the way production
// severs it (one transaction, no task cancellation).
func archiveChatSessionViaAPI(t *testing.T, sessionID string) {
	t.Helper()
	req := newRequest("PATCH", "/api/chat/sessions/"+sessionID+"/archive", map[string]any{"archived": true})
	req = withURLParam(req, "sessionId", sessionID)
	req = withChatTestWorkspaceCtx(t, req)
	w := httptest.NewRecorder()

	testHandler.SetChatSessionArchived(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("SetChatSessionArchived: expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

// claimChatChannelFieldsIfAny is claimChatChannelFields for a case where
// handing out no task at all is a legitimate answer: if the queue declines to
// serve a task whose session was archived, there is no run to misdescribe.
func claimChatChannelFieldsIfAny(t *testing.T, runtimeID string) (claimedChatChannel, bool) {
	t.Helper()
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest("POST", "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "claim-archived-binding")
	req = withURLParam(req, "runtimeId", runtimeID)

	testHandler.ClaimTaskByRuntime(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ClaimTaskByRuntime: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Task *claimedChatChannel `json:"task"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode claim response: %v", err)
	}
	if resp.Task == nil {
		return claimedChatChannel{}, false
	}
	return *resp.Task, true
}

// A turn whose reply still lands in a WeCom group room must never be handed to
// the agent as a private 1:1. Archiving the session between the message
// arriving and the daemon claiming it is the window where those two facts come
// apart: the row that says "group room" is gone, the handle that delivers into
// that room is not.
//
// Either outcome keeps the promise — hand out nothing, or hand out a task that
// still names the room. What must not happen is a task that runs with the room
// erased from the brief while the reply goes there anyway.
func TestClaim_ArchivedGroupSessionIsNotHandedOutAsAPrivateWebChat(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, runtimeID, _ := setupDirectChatSession(t, ctx, "wecom group chat archived mid-flight")

	// The state an inbound WeCom group message leaves behind: the session is
	// bound to the room (EnsureSession) and a turn is waiting to be claimed.
	seedChannelBindingOfChatType(t, ctx, agentID, sessionID, "wecom", "group", "msg-1", "msg-1")
	insertChannelChatTask(t, ctx, agentID, runtimeID, sessionID)
	requeueTaskForClaim(t, ctx, sessionID)

	// The owner archives before the daemon gets there. The room is still open
	// and the adapter still holds its handle; only the binding row is gone.
	archiveChatSessionViaAPI(t, sessionID)

	claimed, handedOut := claimChatChannelFieldsIfAny(t, runtimeID)
	if !handedOut {
		// No task, no answer, nothing to misdirect.
		return
	}
	if claimed.ChannelType != "wecom" || claimed.ChatType != "group" {
		t.Errorf("claim reported chat_channel_type=%q chat_type=%q, want %q/%q — "+
			"the reply for this turn is still delivered into the WeCom group room the message came from, "+
			"but the brief built from these fields tells the agent it is chatting privately with one person "+
			"in the Multica web app, so it answers a room of many as if it were alone with one",
			claimed.ChannelType, claimed.ChatType, "wecom", "group")
	}
}
