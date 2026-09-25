package service

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// memberMayInvokeAgent decides whether a specific member may invoke the agent
// under the invocation-permission model (MUL-3963). It mirrors
// handler.canInvokeAgent with a member effective user: the owner always may, a
// private agent admits nobody else (no admin bypass), and a public_to agent
// admits a workspace member on a workspace target or the named user on a member
// target. Team targets are inert.
//
// A failed allow-list read is returned rather than folded into a denial, so a
// caller that answers the member can tell "not allowed" from "could not check".
// A failed membership read counts as "not a member", as it does in the handler.
func memberMayInvokeAgent(ctx context.Context, q *db.Queries, agent db.Agent, memberUserID, workspaceID pgtype.UUID) (bool, error) {
	userID := util.UUIDToString(memberUserID)
	if userID == "" {
		return false, nil
	}
	if util.UUIDToString(agent.OwnerID) == userID {
		return true, nil
	}
	if agent.PermissionMode != "public_to" {
		return false, nil
	}
	targets, err := q.ListAgentInvocationTargets(ctx, agent.ID)
	if err != nil {
		return false, err
	}
	isWorkspaceMember := false
	if _, err := q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID:      memberUserID,
		WorkspaceID: workspaceID,
	}); err == nil {
		isWorkspaceMember = true
	}
	for _, t := range targets {
		switch t.TargetType {
		case "workspace":
			if isWorkspaceMember {
				return true, nil
			}
		case "member":
			if util.UUIDToString(t.TargetID) == userID {
				return true, nil
			}
		}
	}
	return false, nil
}
