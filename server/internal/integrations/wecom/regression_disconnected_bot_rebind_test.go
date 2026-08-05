package wecom

// regression_disconnected_bot_rebind_test.go — guards the one thing an admin
// cannot talk their way out of: a WeCom bot that has been disconnected and can
// then never be connected to anything, anywhere, again.
//
// Disconnect (Revoke) only flips status to 'revoked'. The row stays, and with it
// the (channel_type, config->>'app_id') value that idx_channel_installation_type_appid
// holds unique — unconditionally, with no WHERE status = 'active' — so a revoked
// row goes on occupying its bot's routing slot. Upsert then goes straight at
// UpsertChannelInstallation, whose ON CONFLICT key is (workspace_id, agent_id,
// channel_type): a DIFFERENT agent misses that key entirely and trips the index
// instead. botOwnerConflictErr reads GetChannelInstallationOwnerByAppID to name
// the holder, but never runs ReclaimDeadChannelInstallationByAppID first, which
// that query's own contract requires ("meant to be read only after
// ReclaimDeadChannelInstallationByAppID has removed every DEAD owner, so a
// returned row is a live active owner") — so the dead owner is reported as a live
// one, confidently and wrongly.
//
// What the admin sees: "this bot is already connected to another agent in this
// workspace — disconnect it there first, then connect it here", about a row that
// shows a grey Revoked badge. The settings tab gates its Disconnect button on
// `canManage && isActive` (wecom-tab.tsx), so a revoked row draws no control at
// all. The product offers no way to free the bot, and no error message can be
// acted on. The bot is stranded.
//
// slack/install.go reclaims dead and orphaned owners in front of its upsert, and
// lark/channel_store.go does the same; wecom is the only adapter that never calls
// it. The tests below drive InstallationService — the only write path to a wecom
// channel_installation row — against an in-memory stand-in for that table which
// enforces the two unique constraints migration 124 actually creates. Nothing
// else in the tree covers this: handler/wecom_web_test.go seeds an ACTIVE owner
// and asserts the 409 that case should still get, and never revokes anything.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---- the stand-in for channel_installation -------------------------------

// installedBot is one channel_installation row, holding only the columns this
// path reads or writes.
type installedBot struct {
	id          pgtype.UUID
	workspaceID pgtype.UUID
	agentID     pgtype.UUID
	channelType string
	config      []byte
	status      string
	installerID pgtype.UUID
}

// appID is the routing key the unique index is built on: config->>'app_id'.
func (r installedBot) appID() string {
	var cfg struct {
		AppID string `json:"app_id"`
	}
	if err := json.Unmarshal(r.config, &cfg); err != nil {
		return ""
	}
	return cfg.AppID
}

// agentRecord stands in for the agent row the reclaim predicate and the owner
// lookup consult. Presence in channelTable.agents means the agent still exists;
// archived is reversible and deliberately does NOT free the bot.
type agentRecord struct{ archived bool }

// channelTable is an in-memory channel_installation plus the two unique
// constraints migration 124 puts on it: UNIQUE(workspace_id, agent_id,
// channel_type) on the table itself, and the functional unique index
// idx_channel_installation_type_appid on (channel_type, (config->>'app_id')).
// That second one is unconditional in the migration, which is the whole story
// here — a revoked row keeps its bot's slot.
//
// It also keeps what the reclaim statement's predicate needs (does the owning
// workspace still exist, does the owning agent still exist) and a count of the
// chat-session bindings hanging off each installation, since a binding minted
// under one agent must not survive the bot moving to another.
//
// It answers by sqlc query name, so a statement it has not been taught surfaces
// as an error naming that query instead of a silent wrong answer.
type channelTable struct {
	rows []installedBot

	workspaces   map[pgtype.UUID]bool
	agents       map[pgtype.UUID]agentRecord
	chatSessions map[pgtype.UUID]int

	nextID byte
}

func newChannelTable() *channelTable {
	return &channelTable{
		workspaces:   map[pgtype.UUID]bool{},
		agents:       map[pgtype.UUID]agentRecord{},
		chatSessions: map[pgtype.UUID]int{},
		nextID:       0x90,
	}
}

func (tb *channelTable) addWorkspace(id pgtype.UUID) { tb.workspaces[id] = true }

func (tb *channelTable) addAgent(id pgtype.UUID, archived bool) {
	tb.agents[id] = agentRecord{archived: archived}
}

func (tb *channelTable) mintID() pgtype.UUID {
	tb.nextID++
	return pgtype.UUID{Bytes: [16]byte{tb.nextID}, Valid: true}
}

func (tb *channelTable) indexByAppID(channelType, appID string) int {
	if appID == "" {
		return -1
	}
	for i, r := range tb.rows {
		if r.channelType == channelType && r.appID() == appID {
			return i
		}
	}
	return -1
}

func (tb *channelTable) indexByAgent(workspaceID, agentID pgtype.UUID, channelType string) int {
	for i, r := range tb.rows {
		if r.workspaceID == workspaceID && r.agentID == agentID && r.channelType == channelType {
			return i
		}
	}
	return -1
}

func (tb *channelTable) indexByID(id pgtype.UUID) int {
	for i, r := range tb.rows {
		if r.id == id {
			return i
		}
	}
	return -1
}

// appIDTakenBy reports the row (other than skip) already holding candidate's
// (channel_type, app_id) — idx_channel_installation_type_appid.
func (tb *channelTable) appIDTakenBy(candidate installedBot, skip int) int {
	appID := candidate.appID()
	if appID == "" {
		return -1 // JSON null; Postgres allows many NULLs in a unique index
	}
	for i, r := range tb.rows {
		if i == skip {
			continue
		}
		if r.channelType == candidate.channelType && r.appID() == appID {
			return i
		}
	}
	return -1
}

// agentSlotTakenBy reports the row (other than skip) already holding
// candidate's (workspace_id, agent_id, channel_type) — the table UNIQUE.
func (tb *channelTable) agentSlotTakenBy(candidate installedBot, skip int) int {
	for i, r := range tb.rows {
		if i == skip {
			continue
		}
		if r.workspaceID == candidate.workspaceID && r.agentID == candidate.agentID && r.channelType == candidate.channelType {
			return i
		}
	}
	return -1
}

func uniqueViolation(constraint string) error {
	return &pgconn.PgError{
		Code:           "23505",
		Message:        fmt.Sprintf("duplicate key value violates unique constraint %q", constraint),
		ConstraintName: constraint,
	}
}

// ---- pgx.Row plumbing ----------------------------------------------------

// scanRow adapts a closure to pgx.Row.
type scanRow func(dest ...any) error

func (f scanRow) Scan(dest ...any) error { return f(dest...) }

// failedRow is a pgx.Row that only ever reports err — pgx.ErrNoRows, a unique
// violation, or a complaint from the rig itself.
type failedRow struct{ err error }

func (r failedRow) Scan(...any) error { return r.err }

func putUUID(dest any, v pgtype.UUID) error {
	p, ok := dest.(*pgtype.UUID)
	if !ok {
		return fmt.Errorf("regression rig: expected *pgtype.UUID destination, got %T", dest)
	}
	*p = v
	return nil
}

func putString(dest any, v string) error {
	p, ok := dest.(*string)
	if !ok {
		return fmt.Errorf("regression rig: expected *string destination, got %T", dest)
	}
	*p = v
	return nil
}

func putBytes(dest any, v []byte) error {
	p, ok := dest.(*[]byte)
	if !ok {
		return fmt.Errorf("regression rig: expected *[]byte destination, got %T", dest)
	}
	*p = v
	return nil
}

// installationRow serves a full channel_installation RETURNING/SELECT list. The
// lease and timestamp columns are left at their zero values; no caller on this
// path reads them.
func installationRow(r installedBot) pgx.Row {
	return scanRow(func(dest ...any) error {
		if len(dest) != 12 {
			return fmt.Errorf("regression rig: channel_installation has 12 columns, statement scanned %d", len(dest))
		}
		for _, step := range []struct {
			dest any
			put  func(any) error
		}{
			{dest[0], func(d any) error { return putUUID(d, r.id) }},
			{dest[1], func(d any) error { return putUUID(d, r.workspaceID) }},
			{dest[2], func(d any) error { return putUUID(d, r.agentID) }},
			{dest[3], func(d any) error { return putString(d, r.channelType) }},
			{dest[4], func(d any) error { return putBytes(d, r.config) }},
			{dest[5], func(d any) error { return putString(d, r.status) }},
			{dest[8], func(d any) error { return putUUID(d, r.installerID) }},
		} {
			if err := step.put(step.dest); err != nil {
				return err
			}
		}
		return nil
	})
}

// ---- statement handlers --------------------------------------------------

// queryNameOf reads the sqlc name off the head of a generated statement
// ("-- name: Foo :one"), so the rig dispatches on the query rather than on
// fragments of SQL text, and names the query it does not know.
func queryNameOf(sql string) string {
	head := sql
	if i := strings.IndexByte(head, '\n'); i >= 0 {
		head = head[:i]
	}
	head = strings.TrimSpace(head)
	const marker = "-- name:"
	if !strings.HasPrefix(head, marker) {
		return ""
	}
	name := strings.TrimSpace(strings.TrimPrefix(head, marker))
	if i := strings.IndexByte(name, ' '); i >= 0 {
		name = name[:i]
	}
	return name
}

func (tb *channelTable) QueryRow(_ context.Context, sql string, args ...interface{}) pgx.Row {
	switch name := queryNameOf(sql); name {
	case "UpsertChannelInstallation":
		return tb.upsertByAgent(args)
	case "UpsertChannelInstallationByAppID":
		return tb.upsertByAppID(args)
	case "ReclaimDeadChannelInstallationByAppID":
		return tb.reclaimDead(args)
	case "GetChannelInstallationOwnerByAppID":
		return tb.ownerByAppID(args)
	case "GetChannelInstallationByAppID":
		if len(args) != 2 {
			return failedRow{fmt.Errorf("regression rig: GetChannelInstallationByAppID takes 2 args, got %d", len(args))}
		}
		channelType, _ := args[0].(string)
		appID, _ := args[1].(string)
		if i := tb.indexByAppID(channelType, appID); i >= 0 {
			return installationRow(tb.rows[i])
		}
		return failedRow{pgx.ErrNoRows}
	case "GetChannelInstallation", "GetChannelInstallationInWorkspace":
		id, ok := args[0].(pgtype.UUID)
		if !ok {
			return failedRow{fmt.Errorf("regression rig: %s expected a uuid id, got %T", name, args[0])}
		}
		i := tb.indexByID(id)
		if i < 0 {
			return failedRow{pgx.ErrNoRows}
		}
		if name == "GetChannelInstallationInWorkspace" {
			if ws, ok := args[1].(pgtype.UUID); ok && tb.rows[i].workspaceID != ws {
				return failedRow{pgx.ErrNoRows}
			}
		}
		return installationRow(tb.rows[i])
	default:
		return failedRow{fmt.Errorf("regression rig: query %q is not modelled by this test's channel_installation stand-in; teach channelTable about it", name)}
	}
}

// upsertByAgent is UpsertChannelInstallation: ON CONFLICT (workspace_id,
// agent_id, channel_type). A row for another agent is NOT its conflict target,
// so it reaches the app_id unique index and fails there.
func (tb *channelTable) upsertByAgent(args []interface{}) pgx.Row {
	next, err := upsertCandidate(args)
	if err != nil {
		return failedRow{err}
	}
	if i := tb.indexByAgent(next.workspaceID, next.agentID, next.channelType); i >= 0 {
		next.id = tb.rows[i].id // the same agent reconnecting updates its row in place
		if tb.appIDTakenBy(next, i) >= 0 {
			return failedRow{uniqueViolation("idx_channel_installation_type_appid")}
		}
		tb.rows[i] = next
		return installationRow(next)
	}
	if tb.appIDTakenBy(next, -1) >= 0 {
		return failedRow{uniqueViolation("idx_channel_installation_type_appid")}
	}
	next.id = tb.mintID()
	tb.rows = append(tb.rows, next)
	return installationRow(next)
}

// upsertByAppID is UpsertChannelInstallationByAppID: ON CONFLICT
// (channel_type, (config->>'app_id')) ... WHERE workspace_id matches, so a row
// owned by another workspace updates nothing and returns no row at all.
func (tb *channelTable) upsertByAppID(args []interface{}) pgx.Row {
	next, err := upsertCandidate(args)
	if err != nil {
		return failedRow{err}
	}
	if i := tb.indexByAppID(next.channelType, next.appID()); i >= 0 {
		if tb.rows[i].workspaceID != next.workspaceID {
			return failedRow{pgx.ErrNoRows}
		}
		next.id = tb.rows[i].id
		if tb.agentSlotTakenBy(next, i) >= 0 {
			return failedRow{uniqueViolation("channel_installation_workspace_id_agent_id_channel_type_key")}
		}
		tb.rows[i] = next
		return installationRow(next)
	}
	if tb.agentSlotTakenBy(next, -1) >= 0 {
		return failedRow{uniqueViolation("channel_installation_workspace_id_agent_id_channel_type_key")}
	}
	next.id = tb.mintID()
	tb.rows = append(tb.rows, next)
	return installationRow(next)
}

// upsertCandidate reads the five INSERT values both upserts share.
func upsertCandidate(args []interface{}) (installedBot, error) {
	if len(args) != 5 {
		return installedBot{}, fmt.Errorf("regression rig: channel_installation upsert takes 5 args, got %d", len(args))
	}
	workspaceID, ok1 := args[0].(pgtype.UUID)
	agentID, ok2 := args[1].(pgtype.UUID)
	channelType, ok3 := args[2].(string)
	config, ok4 := args[3].([]byte)
	installerID, ok5 := args[4].(pgtype.UUID)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 {
		return installedBot{}, fmt.Errorf("regression rig: unexpected upsert arg types: %T %T %T %T %T",
			args[0], args[1], args[2], args[3], args[4])
	}
	return installedBot{
		workspaceID: workspaceID,
		agentID:     agentID,
		channelType: channelType,
		config:      config,
		status:      "active", // the upsert forces status back to 'active'
		installerID: installerID,
	}, nil
}

// reclaimDead is ReclaimDeadChannelInstallationByAppID: it removes a DEAD owner
// of the slot — a revoked row held by anyone other than the caller's own
// (workspace, agent) pair, or an orphan whose workspace or agent no longer
// exists — along with that installation's dependent rows, and reports
// pgx.ErrNoRows when nothing was dead. A live owner is left alone, archived
// agent included, so the caller still gets an accurate conflict.
func (tb *channelTable) reclaimDead(args []interface{}) pgx.Row {
	if len(args) != 4 {
		return failedRow{fmt.Errorf("regression rig: reclaim takes 4 args, got %d", len(args))}
	}
	channelType, ok1 := args[0].(string)
	appID, ok2 := args[1].(string)
	callerWorkspace, ok3 := args[2].(pgtype.UUID)
	callerAgent, ok4 := args[3].(pgtype.UUID)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return failedRow{fmt.Errorf("regression rig: unexpected reclaim arg types: %T %T %T %T",
			args[0], args[1], args[2], args[3])}
	}
	i := tb.indexByAppID(channelType, appID)
	if i < 0 {
		return failedRow{pgx.ErrNoRows}
	}
	owner := tb.rows[i]
	revokedElsewhere := owner.status == "revoked" &&
		!(owner.workspaceID == callerWorkspace && owner.agentID == callerAgent)
	_, agentAlive := tb.agents[owner.agentID]
	orphaned := !tb.workspaces[owner.workspaceID] || !agentAlive
	if !revokedElsewhere && !orphaned {
		return failedRow{pgx.ErrNoRows}
	}
	tb.rows = append(tb.rows[:i], tb.rows[i+1:]...)
	delete(tb.chatSessions, owner.id) // the CTE clears every dependent of the removed row
	return scanRow(func(dest ...any) error {
		if len(dest) != 1 {
			return fmt.Errorf("regression rig: reclaim returns 1 column, statement scanned %d", len(dest))
		}
		return putUUID(dest[0], owner.id)
	})
}

// ownerByAppID is GetChannelInstallationOwnerByAppID. Its JOIN on agent drops a
// row whose agent no longer exists, so a hard-deleted owner reads as no owner.
func (tb *channelTable) ownerByAppID(args []interface{}) pgx.Row {
	if len(args) != 2 {
		return failedRow{fmt.Errorf("regression rig: owner lookup takes 2 args, got %d", len(args))}
	}
	channelType, _ := args[0].(string)
	appID, _ := args[1].(string)
	i := tb.indexByAppID(channelType, appID)
	if i < 0 {
		return failedRow{pgx.ErrNoRows}
	}
	owner := tb.rows[i]
	agent, alive := tb.agents[owner.agentID]
	if !alive {
		return failedRow{pgx.ErrNoRows}
	}
	return scanRow(func(dest ...any) error {
		if len(dest) != 3 {
			return fmt.Errorf("regression rig: owner lookup returns 3 columns, statement scanned %d", len(dest))
		}
		if err := putUUID(dest[0], owner.workspaceID); err != nil {
			return err
		}
		if err := putUUID(dest[1], owner.agentID); err != nil {
			return err
		}
		archivedAt, ok := dest[2].(*pgtype.Timestamptz)
		if !ok {
			return fmt.Errorf("regression rig: expected *pgtype.Timestamptz for agent_archived_at, got %T", dest[2])
		}
		*archivedAt = pgtype.Timestamptz{Time: time.Unix(1_700_000_000, 0), Valid: agent.archived}
		return nil
	})
}

func (tb *channelTable) Exec(_ context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	switch name := queryNameOf(sql); name {
	case "SetChannelInstallationStatus":
		if len(args) != 2 {
			return pgconn.CommandTag{}, fmt.Errorf("regression rig: SetChannelInstallationStatus takes 2 args, got %d", len(args))
		}
		id, ok1 := args[0].(pgtype.UUID)
		status, ok2 := args[1].(string)
		if !ok1 || !ok2 {
			return pgconn.CommandTag{}, fmt.Errorf("regression rig: unexpected status arg types: %T %T", args[0], args[1])
		}
		if i := tb.indexByID(id); i >= 0 {
			tb.rows[i].status = status
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		return pgconn.NewCommandTag("UPDATE 0"), nil
	case "DeleteChannelChatSessionBindingsByInstallation":
		if id, ok := args[0].(pgtype.UUID); ok {
			delete(tb.chatSessions, id)
		}
		return pgconn.NewCommandTag("DELETE 1"), nil
	case "DeleteChannelUserBindingsByInstallation",
		"DeleteChannelBindingTokensByInstallation",
		"NullChannelInboundAuditInstallationID":
		// Modelled as no-ops: this rig only tracks chat-session bindings.
		return pgconn.NewCommandTag("DELETE 0"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("regression rig: statement %q is not modelled by this test's channel_installation stand-in; teach channelTable about it", name)
	}
}

func (tb *channelTable) Query(_ context.Context, sql string, _ ...interface{}) (pgx.Rows, error) {
	return nil, fmt.Errorf("regression rig: multi-row query %q is not modelled by this test's channel_installation stand-in", queryNameOf(sql))
}

// newRebindRig wires a real InstallationService — the only write path to a
// wecom channel_installation row, and the one the HTTP install endpoint uses —
// onto the stand-in table.
func newRebindRig(t *testing.T) (*InstallationService, *channelTable) {
	t.Helper()
	key := make([]byte, secretbox.KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	box, err := secretbox.New(key)
	if err != nil {
		t.Fatalf("secretbox.New: %v", err)
	}
	table := newChannelTable()
	svc, err := NewInstallationService(db.New(table), box)
	if err != nil {
		t.Fatalf("NewInstallationService: %v", err)
	}
	return svc, table
}

// ---- the tests -----------------------------------------------------------

// TestADisconnectedBotCanBeConnectedToAnotherAgent — an admin moves a bot from
// one agent to another the only way the product offers: disconnect it there,
// connect it here. Today the second step is refused, and the sentence it is
// refused with tells the admin to do the very thing they just did. The revoked
// row draws no Disconnect button, no other screen touches installations, and
// nothing expires — so the bot cannot be connected to anything ever again, and
// the only remaining move is a new bot on the WeChat Work console.
func TestADisconnectedBotCanBeConnectedToAnotherAgent(t *testing.T) {
	ctx := context.Background()
	svc, table := newRebindRig(t)

	const botID = "wecom-smart-bot-42"
	workspace, agentA, agentB, admin := uuidOf(1), uuidOf(2), uuidOf(3), uuidOf(4)
	table.addWorkspace(workspace)
	table.addAgent(agentA, false)
	table.addAgent(agentB, false)

	// The admin pastes the bot id and its long-connection secret on agent A.
	onA, err := svc.Upsert(ctx, InstallationParams{
		WorkspaceID:     workspace,
		AgentID:         agentA,
		InstallerUserID: admin,
		BotID:           botID,
		Secret:          "long-connection-secret",
	})
	if err != nil {
		t.Fatalf("connecting the bot to the first agent: %v", err)
	}

	// People talk to the bot for a while, so chat sessions accumulate bound to
	// this installation and to agent A behind it.
	table.chatSessions[onA.ID] = 3

	// They decide the bot belongs to agent B instead and press Disconnect. The
	// row survives as a revoked audit record; the settings tab now shows a grey
	// Revoked badge with no button beside it.
	if err := svc.Revoke(ctx, onA.ID); err != nil {
		t.Fatalf("disconnecting the bot from the first agent: %v", err)
	}

	// The second half of the move, which is the whole reason they disconnected.
	onB, err := svc.Upsert(ctx, InstallationParams{
		WorkspaceID:     workspace,
		AgentID:         agentB,
		InstallerUserID: admin,
		BotID:           botID,
		Secret:          "long-connection-secret",
	})
	if err != nil {
		t.Fatalf("a bot that was disconnected from one agent could not be connected to another: %v\n"+
			"the disconnected installation is still in channel_installation holding this bot's\n"+
			"(channel_type, app_id) slot — Revoke only flips status, and the unique index has no\n"+
			"status predicate — so the install trips idx_channel_installation_type_appid, and the\n"+
			"conflict is classified without the dead-owner reclaim slack and lark both run first.\n"+
			"a revoked owner is therefore named as a live one, and the admin is told to disconnect\n"+
			"the bot where it is already disconnected. the settings tab draws Disconnect only on an\n"+
			"active row (canManage && isActive), so there is nothing left to click: this bot cannot\n"+
			"be connected to any agent again, in this workspace or any other.", err)
	}
	if onB.AgentID != agentB {
		t.Fatalf("the bot was connected to the wrong agent: got agent %v, want %v", onB.AgentID, agentB)
	}
	if onB.Status != InstallationActive {
		t.Fatalf("the bot connected to the second agent but is not active: status %q", onB.Status)
	}
	if onB.BotID != botID {
		t.Fatalf("the installation on the second agent carries bot id %q, want %q", onB.BotID, botID)
	}

	// The bot now answers as agent B, so no chat session bound under agent A may
	// still be live: the next message in one of those chats would otherwise be
	// routed by a binding onto an installation that is gone or has changed hands,
	// and the person in that chat would get either silence or the wrong agent.
	if n := table.chatSessions[onA.ID]; n != 0 {
		t.Fatalf("the bot moved to another agent but %d chat-session binding(s) still point at the\n"+
			"installation it left: those conversations keep routing to the agent that no longer\n"+
			"owns the bot.", n)
	}
}

// TestABotConnectedSomewhereLiveIsStillRefused — the opposite failure, and the
// reason freeing a revoked slot has to be narrow. A bot that is genuinely in use
// somewhere must not be silently stolen by whoever pastes its credentials next:
// the agent it is connected to would go quiet with nothing on screen to say why.
// Both cases here are recoverable by a person, and the message has to point at
// the recovery.
func TestABotConnectedSomewhereLiveIsStillRefused(t *testing.T) {
	const botID = "wecom-smart-bot-77"

	t.Run("another agent in the same workspace", func(t *testing.T) {
		ctx := context.Background()
		svc, table := newRebindRig(t)
		workspace, agentA, agentB, admin := uuidOf(1), uuidOf(2), uuidOf(3), uuidOf(4)
		table.addWorkspace(workspace)
		table.addAgent(agentA, false)
		table.addAgent(agentB, false)

		if _, err := svc.Upsert(ctx, InstallationParams{
			WorkspaceID: workspace, AgentID: agentA, InstallerUserID: admin,
			BotID: botID, Secret: "long-connection-secret",
		}); err != nil {
			t.Fatalf("connecting the bot to the first agent: %v", err)
		}

		_, err := svc.Upsert(ctx, InstallationParams{
			WorkspaceID: workspace, AgentID: agentB, InstallerUserID: admin,
			BotID: botID, Secret: "long-connection-secret",
		})
		if !errors.Is(err, ErrBotOwnedBySameWorkspace) {
			t.Fatalf("connecting a bot that is live on another agent should be refused with the\n"+
				"sentence that sends the admin to that agent's Disconnect button; got %v", err)
		}
	})

	t.Run("an agent that has been archived", func(t *testing.T) {
		ctx := context.Background()
		svc, table := newRebindRig(t)
		workspace, archivedAgent, agentB, admin := uuidOf(1), uuidOf(2), uuidOf(3), uuidOf(4)
		table.addWorkspace(workspace)
		table.addAgent(archivedAgent, false)
		table.addAgent(agentB, false)

		if _, err := svc.Upsert(ctx, InstallationParams{
			WorkspaceID: workspace, AgentID: archivedAgent, InstallerUserID: admin,
			BotID: botID, Secret: "long-connection-secret",
		}); err != nil {
			t.Fatalf("connecting the bot to the first agent: %v", err)
		}
		// Archiving is reversible and does not disconnect anything, so the bot is
		// still owned — but the agent has dropped out of the agent list, which is
		// exactly when the admin needs to be told where it went.
		table.addAgent(archivedAgent, true)

		_, err := svc.Upsert(ctx, InstallationParams{
			WorkspaceID: workspace, AgentID: agentB, InstallerUserID: admin,
			BotID: botID, Secret: "long-connection-secret",
		})
		if !errors.Is(err, ErrBotOwnedByArchivedAgent) {
			t.Fatalf("connecting a bot held by an archived agent should be refused with the sentence\n"+
				"that names the archive — the agent is not in the list, so nothing else on screen\n"+
				"explains where the bot is; got %v", err)
		}
	})
}

// TestReconnectingTheSameAgentKeepsItsInstallation — the everyday case: an admin
// disconnects a bot and reconnects it to the same agent, usually to paste a
// rotated secret. That has to land on the same installation row, because every
// member's account link and every bound chat session hangs off its id. If a fix
// for the takeover case starts deleting revoked rows indiscriminately, this
// reconnect quietly unbinds the whole workspace and everyone is asked to link
// their account again.
func TestReconnectingTheSameAgentKeepsItsInstallation(t *testing.T) {
	ctx := context.Background()
	svc, table := newRebindRig(t)

	const botID = "wecom-smart-bot-13"
	workspace, agent, admin := uuidOf(1), uuidOf(2), uuidOf(4)
	table.addWorkspace(workspace)
	table.addAgent(agent, false)

	first, err := svc.Upsert(ctx, InstallationParams{
		WorkspaceID: workspace, AgentID: agent, InstallerUserID: admin,
		BotID: botID, Secret: "long-connection-secret",
	})
	if err != nil {
		t.Fatalf("connecting the bot: %v", err)
	}
	table.chatSessions[first.ID] = 2

	if err := svc.Revoke(ctx, first.ID); err != nil {
		t.Fatalf("disconnecting the bot: %v", err)
	}

	again, err := svc.Upsert(ctx, InstallationParams{
		WorkspaceID: workspace, AgentID: agent, InstallerUserID: admin,
		BotID: botID, Secret: "rotated-secret",
	})
	if err != nil {
		t.Fatalf("reconnecting the same bot to the same agent: %v", err)
	}
	if again.ID != first.ID {
		t.Fatalf("reconnecting the same bot to the same agent started a new installation\n"+
			"(%v, was %v): every account link and bound chat session hangs off the old id, so\n"+
			"the workspace has to link itself to the bot all over again.", again.ID, first.ID)
	}
	if again.Status != InstallationActive {
		t.Fatalf("the reconnected installation is not active: status %q", again.Status)
	}
	if n := table.chatSessions[first.ID]; n != 2 {
		t.Fatalf("reconnecting the same agent dropped %d of its chat-session bindings: those\n"+
			"conversations lose their thread with the bot for no reason the user can see.", 2-n)
	}
}
