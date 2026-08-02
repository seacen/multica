package wecom

// resolvers_test.go — the five resolvers the engine.Router calls for every
// inbound wecom message. Each one is a translation between the normalized
// envelope and the shared channel_* tables, and each mistranslation has a
// user-visible shape: a message routed to the wrong workspace, a departed
// colleague still reaching the agent, a redelivered frame answered twice, two
// group chats sharing one session.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// wecomInbound builds the envelope the Router hands the resolvers: a
// cross-platform message whose Raw carries the wecom-side fields.
func wecomInbound(botID, chatID, senderID string, chatType channel.ChatType) channel.InboundMessage {
	wm := InboundMessage{
		BotID:        botID,
		MsgID:        "MSGID-001",
		MsgType:      "text",
		ChatID:       chatID,
		SenderUserID: senderID,
		Content:      "你好",
	}
	if chatType == channel.ChatTypeGroup {
		wm.ChatType = "group"
	} else {
		wm.ChatType = "single"
	}
	raw, _ := json.Marshal(wm)
	return channel.InboundMessage{
		EventID:   wm.MsgID,
		MessageID: wm.MsgID,
		Type:      channel.MsgTypeText,
		Text:      wm.Content,
		Source: channel.Source{
			ChannelType: TypeWecom,
			ChatID:      chatID,
			ChatType:    chatType,
			SenderID:    senderID,
		},
		Raw: raw,
	}
}

// ---- the set itself ----

func TestNewResolverSetIsComplete(t *testing.T) {
	set := NewResolverSet(nil, nil, nil)
	if set.Installation == nil || set.Identity == nil || set.Dedup == nil || set.Session == nil || set.Audit == nil {
		t.Fatal("all five required resolvers must be populated")
	}
	if set.OriginType != "wecom_chat" {
		t.Errorf("OriginType = %q, want wecom_chat", set.OriginType)
	}
	if set.Replier != nil {
		t.Error("a nil replier must leave Replier nil, not a typed-nil interface the Router would call")
	}
	if set.Typing != nil {
		t.Error("wecom has no typing indicator; Typing must stay nil")
	}
	if set.Media != nil {
		t.Error("wecom declares CapText only; Media must stay nil")
	}

	set = NewResolverSet(nil, nil, NewOutboundReplier(OutboundReplierConfig{}))
	if set.Replier == nil {
		t.Error("a real replier must reach ResolverSet.Replier")
	}
}

// ---- installation routing ----

type fakeInstallationLookup struct {
	inst    Installation
	err     error
	askedBy string
}

func (f *fakeInstallationLookup) GetInstallationByBotID(_ context.Context, botID string) (Installation, error) {
	f.askedBy = botID
	return f.inst, f.err
}

// TestResolveInstallationRoutesOnTheBotID: the socket knows which bot it is,
// stamps it into Raw, and this lookup is the whole routing decision.
func TestResolveInstallationRoutesOnTheBotID(t *testing.T) {
	store := &fakeInstallationLookup{inst: Installation{
		ID:              uuidOf(1),
		WorkspaceID:     uuidOf(2),
		AgentID:         uuidOf(3),
		InstallerUserID: uuidOf(4),
		Status:          InstallationActive,
		BotID:           "wb-1",
		Locale:          "en",
	}}
	r := &installationResolver{store: store}

	got, err := r.ResolveInstallation(context.Background(), wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P))
	if err != nil {
		t.Fatalf("ResolveInstallation: %v", err)
	}
	if store.askedBy != "wb-1" {
		t.Errorf("looked up bot %q, want the one on the message", store.askedBy)
	}
	if got.ID != uuidOf(1) || got.WorkspaceID != uuidOf(2) || got.AgentID != uuidOf(3) || got.InstallerUserID != uuidOf(4) {
		t.Errorf("resolved identity = %+v", got)
	}
	if !got.Active {
		t.Error("an active installation must resolve Active")
	}
	inst, ok := got.Platform.(Installation)
	if !ok {
		t.Fatalf("Platform = %T, want the wecom Installation (the replier reads the locale off it)", got.Platform)
	}
	if inst.Locale != "en" {
		t.Errorf("Platform.Locale = %q", inst.Locale)
	}
}

// TestResolveInstallationMarksARevokedRowInactive — a revoked install must
// not be reported Active, or a disconnected bot keeps answering.
func TestResolveInstallationMarksARevokedRowInactive(t *testing.T) {
	store := &fakeInstallationLookup{inst: Installation{ID: uuidOf(1), Status: InstallationRevoked}}
	r := &installationResolver{store: store}

	got, err := r.ResolveInstallation(context.Background(), wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P))
	if err != nil {
		t.Fatalf("ResolveInstallation: %v", err)
	}
	if got.Active {
		t.Error("a revoked installation must resolve Active=false")
	}
}

// TestResolveInstallationNotFoundPaths — the Router only treats
// ErrInstallationNotFound as "drop quietly"; anything else it logs as infra.
func TestResolveInstallationNotFoundPaths(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("connection refused")

	cases := []struct {
		name  string
		store installationLookup
		msg   channel.InboundMessage
		want  error
	}{
		{
			name:  "no row for this bot",
			store: &fakeInstallationLookup{err: pgx.ErrNoRows},
			msg:   wecomInbound("wb-unknown", "T-alex", "T-alex", channel.ChatTypeP2P),
			want:  engine.ErrInstallationNotFound,
		},
		{
			name:  "no bot id on the message",
			store: &fakeInstallationLookup{},
			msg:   wecomInbound("", "T-alex", "T-alex", channel.ChatTypeP2P),
			want:  engine.ErrInstallationNotFound,
		},
		{
			name:  "the database is down",
			store: &fakeInstallationLookup{err: boom},
			msg:   wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P),
			want:  boom,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &installationResolver{store: c.store}
			_, err := r.ResolveInstallation(ctx, c.msg)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// TestResolveInstallationRejectsAnUndecodableRaw — an empty Raw is a bug in
// the read loop, not a routing miss; it must not be swallowed as not-found.
func TestResolveInstallationRejectsAnUndecodableRaw(t *testing.T) {
	r := &installationResolver{store: &fakeInstallationLookup{}}
	_, err := r.ResolveInstallation(context.Background(), channel.InboundMessage{})
	if err == nil {
		t.Fatal("an empty Raw must error")
	}
	if errors.Is(err, engine.ErrInstallationNotFound) {
		t.Error("a decode failure must not masquerade as a routing miss")
	}
}

// ---- identity ----

type fakeIdentityLookup struct {
	binding    db.ChannelUserBinding
	bindingErr error
	member     bool
	memberErr  error

	askedInstallation pgtype.UUID
	askedUser         string
	memberWorkspace   pgtype.UUID
	memberUser        pgtype.UUID
}

func (f *fakeIdentityLookup) GetChannelUserBindingByUserID(_ context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error) {
	f.askedInstallation, f.askedUser = arg.InstallationID, arg.ChannelUserID
	return f.binding, f.bindingErr
}

func (f *fakeIdentityLookup) IsWorkspaceMember(_ context.Context, workspaceID, userID pgtype.UUID) (bool, error) {
	f.memberWorkspace, f.memberUser = workspaceID, userID
	return f.member, f.memberErr
}

func resolvedInstallation() engine.ResolvedInstallation {
	return engine.ResolvedInstallation{ID: uuidOf(1), WorkspaceID: uuidOf(2), Active: true}
}

// TestResolveSenderReturnsTheBoundUser — the happy path, plus the fact that
// the binding is looked up per installation (an aibot userid means nothing
// outside the bot that issued it).
func TestResolveSenderReturnsTheBoundUser(t *testing.T) {
	store := &fakeIdentityLookup{
		binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)},
		member:  true,
	}
	r := &identityResolver{store: store}

	got, err := r.ResolveSender(context.Background(), resolvedInstallation(), wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P))
	if err != nil {
		t.Fatalf("ResolveSender: %v", err)
	}
	if got.UserID != uuidOf(7) {
		t.Errorf("UserID = %v, want the bound user", got.UserID)
	}
	if store.askedInstallation != uuidOf(1) || store.askedUser != "T-alex" {
		t.Errorf("binding looked up by (%v, %q), want (installation, aibot userid)", store.askedInstallation, store.askedUser)
	}
	if store.memberWorkspace != uuidOf(2) || store.memberUser != uuidOf(7) {
		t.Errorf("membership checked for (%v, %v)", store.memberWorkspace, store.memberUser)
	}
}

// TestResolveSenderUnbound — a first-time sender is not an error, it is the
// binding prompt. The Router keys that off this exact sentinel.
func TestResolveSenderUnbound(t *testing.T) {
	cases := []struct {
		name   string
		store  identityLookup
		sender string
	}{
		{"never bound", &fakeIdentityLookup{bindingErr: pgx.ErrNoRows}, "T-alex"},
		{"no sender id on the frame", &fakeIdentityLookup{}, ""},
		{"whitespace sender id", &fakeIdentityLookup{}, "   "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &identityResolver{store: c.store}
			_, err := r.ResolveSender(context.Background(), resolvedInstallation(),
				wecomInbound("wb-1", "T-alex", c.sender, channel.ChatTypeP2P))
			if !errors.Is(err, engine.ErrSenderUnbound) {
				t.Fatalf("err = %v, want ErrSenderUnbound", err)
			}
		})
	}
}

// TestResolveSenderRechecksMembership is the security-relevant one: the
// binding row survives a departure, so the row's existence is not permission.
func TestResolveSenderRechecksMembership(t *testing.T) {
	store := &fakeIdentityLookup{
		binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)},
		member:  false, // they left the workspace, the binding row did not
	}
	r := &identityResolver{store: store}

	_, err := r.ResolveSender(context.Background(), resolvedInstallation(), wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P))
	if !errors.Is(err, engine.ErrSenderNotMember) {
		t.Fatalf("err = %v, want ErrSenderNotMember — a departed member's binding must not route", err)
	}
}

// TestResolveSenderSurfacesInfraErrors — a failed membership query must not
// read as "not a member" (that would silently drop real traffic).
func TestResolveSenderSurfacesInfraErrors(t *testing.T) {
	boom := errors.New("connection refused")
	for _, c := range []struct {
		name  string
		store *fakeIdentityLookup
	}{
		{"binding lookup fails", &fakeIdentityLookup{bindingErr: boom}},
		{"membership lookup fails", &fakeIdentityLookup{binding: db.ChannelUserBinding{MulticaUserID: uuidOf(7)}, memberErr: boom}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := &identityResolver{store: c.store}
			_, err := r.ResolveSender(context.Background(), resolvedInstallation(), wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P))
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want the infra error surfaced", err)
			}
		})
	}
}

// ---- dedup ----

type fakeDedupQueries struct {
	claimErr   error
	claimToken pgtype.UUID
	markErr    error
	releaseErr error

	claimArg   db.ClaimChannelInboundDedupParams
	markArg    db.MarkChannelInboundDedupProcessedParams
	releaseArg db.ReleaseChannelInboundDedupParams
}

func (f *fakeDedupQueries) ClaimChannelInboundDedup(_ context.Context, arg db.ClaimChannelInboundDedupParams) (db.ChannelInboundMessageDedup, error) {
	f.claimArg = arg
	return db.ChannelInboundMessageDedup{ClaimToken: f.claimToken}, f.claimErr
}

func (f *fakeDedupQueries) MarkChannelInboundDedupProcessed(_ context.Context, arg db.MarkChannelInboundDedupProcessedParams) (int64, error) {
	f.markArg = arg
	return 1, f.markErr
}

func (f *fakeDedupQueries) ReleaseChannelInboundDedup(_ context.Context, arg db.ReleaseChannelInboundDedupParams) (int64, error) {
	f.releaseArg = arg
	return 1, f.releaseErr
}

// TestDedupClaimKeyIsInstallationPlusMsgID — msgids are only unique within a
// bot, so the installation has to be part of the key.
func TestDedupClaimKeyIsInstallationPlusMsgID(t *testing.T) {
	q := &fakeDedupQueries{claimToken: uuidOf(9)}
	d := &deduper{q: q}

	token, err := d.Claim(context.Background(), uuidOf(1), "MSGID-001")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if token != uuidOf(9) {
		t.Errorf("claim token = %v, want the row's", token)
	}
	if q.claimArg.InstallationID != uuidOf(1) || q.claimArg.MessageID != "MSGID-001" {
		t.Errorf("claim key = %+v", q.claimArg)
	}
}

// TestDedupClaimTranslatesNoRowsToDuplicate — the shared query returns no row
// when someone else already owns the message; the Router needs the sentinel.
func TestDedupClaimTranslatesNoRowsToDuplicate(t *testing.T) {
	d := &deduper{q: &fakeDedupQueries{claimErr: pgx.ErrNoRows}}
	if _, err := d.Claim(context.Background(), uuidOf(1), "MSGID-001"); !errors.Is(err, engine.ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}

	boom := errors.New("connection refused")
	d = &deduper{q: &fakeDedupQueries{claimErr: boom}}
	if _, err := d.Claim(context.Background(), uuidOf(1), "MSGID-001"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the infra error (a dead DB is not a duplicate)", err)
	}
}

// TestDedupMarkAndReleaseAreFencedOnTheClaimToken — without the token a late
// worker could mark someone else's claim processed.
func TestDedupMarkAndReleaseAreFencedOnTheClaimToken(t *testing.T) {
	q := &fakeDedupQueries{}
	d := &deduper{q: q}
	ctx := context.Background()

	if err := d.Mark(ctx, uuidOf(1), "MSGID-001", uuidOf(9)); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if q.markArg.InstallationID != uuidOf(1) || q.markArg.MessageID != "MSGID-001" || q.markArg.ClaimToken != uuidOf(9) {
		t.Errorf("mark args = %+v", q.markArg)
	}

	if err := d.Release(ctx, uuidOf(1), "MSGID-001", uuidOf(9)); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if q.releaseArg.InstallationID != uuidOf(1) || q.releaseArg.MessageID != "MSGID-001" || q.releaseArg.ClaimToken != uuidOf(9) {
		t.Errorf("release args = %+v", q.releaseArg)
	}
}

// TestNewInboundDeduperIsTheSameTwoPhaseSurface — the read loop's receipt path
// gets its at-most-once guarantee from the same table as the Router.
func TestNewInboundDeduperIsTheSameTwoPhaseSurface(t *testing.T) {
	var d engine.Deduper = NewInboundDeduper(NewStore(nil))
	if _, ok := d.(*deduper); !ok {
		t.Fatalf("NewInboundDeduper returned %T, want the shared deduper", d)
	}
}

// ---- session binding ----

type fakeSessionBinder struct {
	ensured   engine.EnsureSessionInput
	appended  engine.AppendInput
	sessionID pgtype.UUID
	ensureErr error
	appendErr error
}

func (f *fakeSessionBinder) EnsureSession(_ context.Context, in engine.EnsureSessionInput) (pgtype.UUID, error) {
	f.ensured = in
	return f.sessionID, f.ensureErr
}

func (f *fakeSessionBinder) AppendUserMessage(_ context.Context, in engine.AppendInput) (engine.AppendResult, error) {
	f.appended = in
	return engine.AppendResult{MessageID: uuidOf(5)}, f.appendErr
}

// TestSessionIsolationKey — one session per person in a DM, one per room in a
// group. aibot has no thread concept, so the chat id is the whole key.
func TestSessionIsolationKey(t *testing.T) {
	cases := []struct {
		name     string
		chatID   string
		chatType channel.ChatType
	}{
		{"a DM keys on the userid", "T-alex", channel.ChatTypeP2P},
		{"a group keys on the room", "wrOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO", channel.ChatTypeGroup},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeSessionBinder{sessionID: uuidOf(6)}
			b := &sessionBinder{session: fake}

			got, err := b.EnsureSession(context.Background(), engine.EnsureSessionParams{
				Installation: engine.ResolvedInstallation{ID: uuidOf(1), WorkspaceID: uuidOf(2), AgentID: uuidOf(3)},
				Sender:       uuidOf(7),
				Message:      wecomInbound("wb-1", c.chatID, "T-alex", c.chatType),
			})
			if err != nil {
				t.Fatalf("EnsureSession: %v", err)
			}
			if got != uuidOf(6) {
				t.Errorf("session id = %v", got)
			}
			if fake.ensured.BindingKey != c.chatID {
				t.Errorf("BindingKey = %q, want the chat id %q", fake.ensured.BindingKey, c.chatID)
			}
			if fake.ensured.ChatType != c.chatType {
				t.Errorf("ChatType = %q", fake.ensured.ChatType)
			}
			if fake.ensured.WorkspaceID != uuidOf(2) || fake.ensured.AgentID != uuidOf(3) || fake.ensured.InstallationID != uuidOf(1) {
				t.Errorf("session scoped to %+v", fake.ensured)
			}
			if fake.ensured.Sender != uuidOf(7) {
				t.Errorf("Sender = %v", fake.ensured.Sender)
			}
		})
	}
}

// TestTwoGroupsDoNotShareASession — the isolation statement, not just the
// field mapping.
func TestTwoGroupsDoNotShareASession(t *testing.T) {
	key := func(chatID string) string {
		fake := &fakeSessionBinder{}
		b := &sessionBinder{session: fake}
		_, _ = b.EnsureSession(context.Background(), engine.EnsureSessionParams{
			Installation: engine.ResolvedInstallation{ID: uuidOf(1)},
			Message:      wecomInbound("wb-1", chatID, "T-alex", channel.ChatTypeGroup),
		})
		return fake.ensured.BindingKey
	}
	if key("room-a") == key("room-b") {
		t.Fatal("two rooms must derive distinct session keys")
	}
}

// TestAppendCarriesTheDedupFence — the append runs the dedup Mark inside its
// own transaction, so the claim token has to travel with it.
func TestAppendCarriesTheDedupFence(t *testing.T) {
	fake := &fakeSessionBinder{}
	b := &sessionBinder{session: fake}

	res, err := b.AppendMessage(context.Background(), engine.AppendParams{
		SessionID:      uuidOf(6),
		Sender:         uuidOf(7),
		InstallationID: uuidOf(1),
		Message:        wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P),
		ClaimToken:     uuidOf(9),
	})
	if err != nil {
		t.Fatalf("AppendMessage: %v", err)
	}
	if res.MessageID != uuidOf(5) {
		t.Errorf("MessageID = %v", res.MessageID)
	}
	in := fake.appended
	if in.SessionID != uuidOf(6) || in.Sender != uuidOf(7) || in.InstallationID != uuidOf(1) || in.ClaimToken != uuidOf(9) {
		t.Errorf("append input = %+v", in)
	}
	if in.Body != "你好" || in.CommandText != "你好" {
		t.Errorf("body/command = %q/%q — wecom has no enrichment, they are the same text", in.Body, in.CommandText)
	}
	if in.MessageID != "MSGID-001" {
		t.Errorf("platform MessageID = %q", in.MessageID)
	}
}

// TestBindMediaIsANoOp — wecom registers no MediaResolver, so this exists
// only to satisfy the interface. If it ever starts doing work, the CapText
// declaration is a lie.
func TestBindMediaIsANoOp(t *testing.T) {
	b := &sessionBinder{session: &fakeSessionBinder{}}
	if err := b.BindMedia(context.Background(), engine.BindMediaParams{}); err != nil {
		t.Fatalf("BindMedia: %v", err)
	}
}

// ---- audit ----

type fakeAuditQueries struct {
	arg db.RecordChannelInboundDropParams
	err error
}

func (f *fakeAuditQueries) RecordChannelInboundDrop(_ context.Context, arg db.RecordChannelInboundDropParams) error {
	f.arg = arg
	return f.err
}

// TestRecordDropKeepsTheRawWecomType — the audit row is the only trace a
// dropped message leaves, so it has to carry enough to explain the drop.
func TestRecordDropKeepsTheRawWecomType(t *testing.T) {
	q := &fakeAuditQueries{}
	a := &auditor{q: q}

	msg := wecomInbound("wb-1", "T-alex", "T-alex", channel.ChatTypeP2P)
	if err := a.RecordDrop(context.Background(), uuidOf(1), msg, engine.DropReasonUnboundUser); err != nil {
		t.Fatalf("RecordDrop: %v", err)
	}
	if q.arg.InstallationID != uuidOf(1) {
		t.Errorf("InstallationID = %v", q.arg.InstallationID)
	}
	if q.arg.ChannelType != "wecom" {
		t.Errorf("ChannelType = %q", q.arg.ChannelType)
	}
	if q.arg.EventType != "text" {
		t.Errorf("EventType = %q, want the raw wecom msgtype", q.arg.EventType)
	}
	if q.arg.DropReason != string(engine.DropReasonUnboundUser) {
		t.Errorf("DropReason = %q", q.arg.DropReason)
	}
	if q.arg.ChannelChatID.String != "T-alex" || !q.arg.ChannelChatID.Valid {
		t.Errorf("ChannelChatID = %+v", q.arg.ChannelChatID)
	}
	if q.arg.ChannelMessageID.String != "MSGID-001" || q.arg.ChannelEventID.String != "MSGID-001" {
		t.Errorf("message/event id = %+v / %+v", q.arg.ChannelMessageID, q.arg.ChannelEventID)
	}
}

// TestRecordDropWithoutAnInstallation — a message that never resolved to an
// installation still gets an audit row, with NULLs rather than empty strings
// (the shared query uses sqlc.narg on those columns).
func TestRecordDropWithoutAnInstallation(t *testing.T) {
	q := &fakeAuditQueries{}
	a := &auditor{q: q}

	if err := a.RecordDrop(context.Background(), pgtype.UUID{}, channel.InboundMessage{}, engine.DropReasonInvalidEvent); err != nil {
		t.Fatalf("RecordDrop: %v", err)
	}
	if q.arg.InstallationID.Valid {
		t.Error("an unresolved installation must be NULL, not the zero uuid")
	}
	if q.arg.ChannelChatID.Valid || q.arg.ChannelEventID.Valid || q.arg.ChannelMessageID.Valid {
		t.Errorf("empty ids must be NULL: %+v", q.arg)
	}
	if q.arg.EventType != "" {
		t.Errorf("EventType = %q, want empty when Raw does not decode", q.arg.EventType)
	}
}

func TestTextOrNull(t *testing.T) {
	if got := textOrNull(""); got.Valid {
		t.Error("an empty string must map to NULL")
	}
	got := textOrNull("x")
	if !got.Valid || got.String != "x" {
		t.Errorf("textOrNull(\"x\") = %+v", got)
	}
}
