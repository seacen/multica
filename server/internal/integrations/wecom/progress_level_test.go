package wecom

// progress_level_test.go — who gets to watch the run work.
//
// The bubble is a chat message. In a group everyone in the room reads it, and
// in a one-to-one with a colleague the person reading it is not the person the
// bot belongs to. So the step list has exactly one audience — the principal,
// in their own chat — and everybody else gets the bubble and the answer with
// nothing in between. Most of this file is the negative half.

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ---- scaffolding ----

// fakeIdentities answers the one question the tier decision asks: which
// Multica user is behind this WeCom sender id. Locked because the ingest race
// test drives OnIngested from several goroutines at once.
type fakeIdentities struct {
	mu            sync.Mutex
	byChannelUser map[string]pgtype.UUID
	err           error
	calls         int
}

func (f *fakeIdentities) GetChannelUserBindingByUserID(_ context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return db.ChannelUserBinding{}, f.err
	}
	id, ok := f.byChannelUser[arg.ChannelUserID]
	if !ok {
		return db.ChannelUserBinding{}, pgx.ErrNoRows
	}
	return db.ChannelUserBinding{MulticaUserID: id}, nil
}

// bind points a WeCom sender id at a Multica user.
func (f *fakeIdentities) bind(channelUserID string, userID pgtype.UUID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byChannelUser[channelUserID] = userID
}

// fail makes every lookup answer with err.
func (f *fakeIdentities) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeIdentities) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeIdentities) IsWorkspaceMember(context.Context, pgtype.UUID, pgtype.UUID) (bool, error) {
	return true, nil
}

// groupInbound is the same envelope as streamInbound, sent into a room.
func groupInbound(reqID, chatID, senderID string) channel.InboundMessage {
	mc := aibotMsgCallback{MsgID: "MSGID-G", ChatID: chatID, ChatType: "group"}
	mc.From.UserID = senderID
	mc.MsgType = "text"
	mc.Text.Content = "帮我查一下"
	return channelMessageFromCallback("BOT-1", mc, mc.Text.Content, reqID)
}

// levelRig is a stream rig whose task lookups are answered, so a transcript
// event reaches the bubble.
func levelRig(t *testing.T) *streamRig {
	t.Helper()
	rig := newStreamRig(t)
	rig.typing.tasks = &fakeTasks{session: rig.session}
	return rig
}

// feedToolCall plays one tool call of a run into whatever bubble is open.
func feedToolCall(rig *streamRig, tool string, input map[string]any) {
	rig.typing.handleTaskMessage(taskMessageEvent(chatTaskID(), toolUse(tool, input)))
}

// ---- the two tiers ----

// TestThePrincipalsOwnChatSeesTheSteps is the positive half: the person the
// bot belongs to, asking in their own chat, watches it work.
func TestThePrincipalsOwnChatSeesTheSteps(t *testing.T) {
	rig := levelRig(t)
	rig.ingest(t, "REQ-42")

	feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})

	frames := streamViews(t, &rig.conn.recordingConn)
	if len(frames) != 2 {
		t.Fatalf("want the opening frame and a step, got %d frames", len(frames))
	}
	if !strings.Contains(frames[1].Content, "正在读取") {
		t.Errorf("content = %q, want the step in it", frames[1].Content)
	}
}

// TestAGroupChatGetsTheBubbleAndNothingElse — a room is not an audience for
// the middle of a run. One person opened the bubble; everyone else is reading
// it.
func TestAGroupChatGetsTheBubbleAndNothingElse(t *testing.T) {
	rig := levelRig(t)
	rig.ingestMessage(t, groupInbound("REQ-42", "CHAT-room", rig.principalSender))

	feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})

	if frames := streamViews(t, &rig.conn.recordingConn); len(frames) != 1 {
		t.Fatalf("the group bubble took %d frames, want only the one that opened it: %+v", len(frames), frames[1:])
	}
}

// TestAColleaguesOwnChatGetsTheBubbleAndNothingElse — a one-to-one is private,
// but it is somebody else's private. The run is still the principal's.
func TestAColleaguesOwnChatGetsTheBubbleAndNothingElse(t *testing.T) {
	rig := levelRig(t)
	rig.identities.bind("T-dana", uuidOf(31)) // bound, but not the principal
	rig.ingestMessage(t, streamInbound("REQ-42", "T-dana"))

	feedToolCall(rig, "Bash", map[string]any{"command": "psql -h db.internal"})

	if frames := streamViews(t, &rig.conn.recordingConn); len(frames) != 1 {
		t.Fatalf("a colleague's bubble took %d frames, want only the one that opened it: %+v", len(frames), frames[1:])
	}
}

// TestAnUnreadableBindingSaysNo — the lookup is the only thing that can prove
// the sender is the principal, so a lookup that failed proves nothing.
func TestAnUnreadableBindingSaysNo(t *testing.T) {
	rig := levelRig(t)
	rig.identities.fail(context.DeadlineExceeded)
	rig.ingest(t, "REQ-42")

	feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})

	if frames := streamViews(t, &rig.conn.recordingConn); len(frames) != 1 {
		t.Fatalf("an unproven sender took %d frames, want only the opening one", len(frames))
	}
}

// TestNoIdentityLookupSaysNo — a deployment that never wired the lookup gets
// the closed tier, not the open one.
func TestNoIdentityLookupSaysNo(t *testing.T) {
	rig := levelRig(t)
	rig.typing.identities = nil
	rig.ingest(t, "REQ-42")

	feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})

	if frames := streamViews(t, &rig.conn.recordingConn); len(frames) != 1 {
		t.Fatalf("an unconfigured deployment took %d frames, want only the opening one", len(frames))
	}
}

// TestTheClosedTierCostsNoLookupsAtAll — the tier is decided once, at ingest,
// and a closed bubble stops before the task row, not after it.
func TestTheClosedTierCostsNoLookupsAtAll(t *testing.T) {
	rig := levelRig(t)
	tasks := &fakeTasks{session: rig.session}
	rig.typing.tasks = tasks
	rig.ingestMessage(t, groupInbound("REQ-42", "CHAT-room", rig.principalSender))

	for i := 0; i < 5; i++ {
		feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})
	}
	if rig.identities.count() != 0 {
		t.Errorf("read the binding %d times for a group, want none — the room settles it", rig.identities.count())
	}
	if tasks.count() != 1 {
		t.Errorf("read the task row %d times for a bubble that shows nothing", tasks.count())
	}
}

// TestTheTierIsDecidedOncePerTurn — the binding is read at ingest and never
// again, however many tool calls the run makes.
func TestTheTierIsDecidedOncePerTurn(t *testing.T) {
	rig := levelRig(t)
	rig.identities.bind("T-dana", uuidOf(31))
	rig.ingestMessage(t, streamInbound("REQ-42", "T-dana"))

	for i := 0; i < 5; i++ {
		feedToolCall(rig, "Read", map[string]any{"file_path": "/srv/app/handler.go"})
	}
	if rig.identities.count() != 1 {
		t.Errorf("read the binding %d times, want exactly one at ingest", rig.identities.count())
	}
}

// ---- who the principal is ----

// TestTheInstallerIsThePrincipalByDefault states the default rule in one
// place: whoever installed the bot is whose run this is.
func TestTheInstallerIsThePrincipalByDefault(t *testing.T) {
	inst := engine.ResolvedInstallation{InstallerUserID: uuidOf(9), Platform: Installation{}}
	if got := principalOf(inst); got != uuidOf(9) {
		t.Errorf("principal = %v, want the installer", got)
	}
}

// TestAConfiguredPrincipalOverridesTheInstaller — one person installs the bot,
// another one uses it. The config field is how that is said.
func TestAConfiguredPrincipalOverridesTheInstaller(t *testing.T) {
	inst := engine.ResolvedInstallation{
		InstallerUserID: uuidOf(9),
		Platform:        Installation{PrincipalUserID: uuidText(uuidOf(31))},
	}
	if got := principalOf(inst); got != uuidOf(31) {
		t.Errorf("principal = %v, want the configured user", got)
	}
}

// TestAnUnreadablePrincipalFallsBackToTheInstaller — a typo in the config must
// not hand the detail to nobody, and must not hand it to everybody either.
func TestAnUnreadablePrincipalFallsBackToTheInstaller(t *testing.T) {
	inst := engine.ResolvedInstallation{
		InstallerUserID: uuidOf(9),
		Platform:        Installation{PrincipalUserID: "not-a-uuid"},
	}
	if got := principalOf(inst); got != uuidOf(9) {
		t.Errorf("principal = %v, want the installer", got)
	}
}

// TestTheConfiguredPrincipalSurvivesARoundTrip — the override is stored next
// to the locale and has to come back out of the JSONB the same way.
func TestTheConfiguredPrincipalSurvivesARoundTrip(t *testing.T) {
	raw, err := encodeInstallConfig(Installation{BotID: "BOT-1", PrincipalUserID: uuidText(uuidOf(31))})
	if err != nil {
		t.Fatalf("encodeInstallConfig: %v", err)
	}
	back, err := installationFromRow(db.ChannelInstallation{Config: raw})
	if err != nil {
		t.Fatalf("installationFromRow: %v", err)
	}
	if back.PrincipalUserID != uuidText(uuidOf(31)) {
		t.Errorf("principal_user_id = %q, want it back as written", back.PrincipalUserID)
	}
}

// TestTheConfiguredPrincipalDecidesTheTier ties the config field to what the
// user sees: the installer stops seeing the detail, the named user starts.
func TestTheConfiguredPrincipalDecidesTheTier(t *testing.T) {
	installer := levelRig(t)
	installer.inst.Platform = Installation{Locale: string(LocaleZhHans), PrincipalUserID: uuidText(uuidOf(31))}
	installer.identities.bind("T-dana", uuidOf(31))
	installer.ingest(t, "REQ-42")
	feedToolCall(installer, "Read", map[string]any{"file_path": "/srv/app/handler.go"})
	if frames := streamViews(t, &installer.conn.recordingConn); len(frames) != 1 {
		t.Fatalf("the installer took %d frames, want only the opening one now someone else is the principal", len(frames))
	}

	named := levelRig(t)
	named.inst.Platform = Installation{Locale: string(LocaleZhHans), PrincipalUserID: uuidText(uuidOf(31))}
	named.identities.bind("T-dana", uuidOf(31))
	named.ingestMessage(t, streamInbound("REQ-43", "T-dana"))
	feedToolCall(named, "Read", map[string]any{"file_path": "/srv/app/handler.go"})
	if frames := streamViews(t, &named.conn.recordingConn); len(frames) != 2 {
		t.Fatalf("the configured principal took %d frames, want the opening one and a step", len(frames))
	}
}

// ---- the narrowing is in one place ----

// TestNothingRendersOutsideTheDetailTier is the rule a future tool type cannot
// slip past: line() is where a step becomes words, and outside the detail tier
// it produces none, whatever the kind.
func TestNothingRendersOutsideTheDetailTier(t *testing.T) {
	kinds := []progressKind{
		progressRaw, progressRead, progressEdit, progressCommand, progressSearch,
		progressWeb, progressSubtask, progressPlan, progressService, progressTool,
		progressError, progressThinking,
	}
	for _, k := range kinds {
		step := progressStep{kind: k, arg: "x", arg2: "y"}
		if got := step.line(copyFor(LocaleZhHans), progressLevelNone); got != "" {
			t.Errorf("kind %d rendered %q outside the detail tier, want nothing", k, got)
		}
		if got := step.line(copyFor(LocaleZhHans), progressLevelDetail); got == "" {
			t.Errorf("kind %d rendered nothing in the detail tier", k)
		}
	}
}
