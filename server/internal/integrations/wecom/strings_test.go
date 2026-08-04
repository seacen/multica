package wecom

// strings_test.go — the bot's copy is chosen per READER: a bound sender gets
// their own Multica profile language, and a reader nobody can name gets the
// Chinese default. Our own tenant must keep reading Chinese without touching
// anything, and a colleague whose profile says English must get English
// everywhere the bot speaks, not just in the one place someone remembered to
// wire up.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// fakeLanguages is a languageLookup with one bound user.
type fakeLanguages struct {
	senderID string
	userID   pgtype.UUID
	language string
}

func (f fakeLanguages) GetChannelUserBindingByUserID(_ context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error) {
	if arg.ChannelUserID == f.senderID {
		return db.ChannelUserBinding{MulticaUserID: f.userID}, nil
	}
	return db.ChannelUserBinding{}, pgx.ErrNoRows
}

func (f fakeLanguages) GetUser(_ context.Context, id pgtype.UUID) (db.User, error) {
	if id == f.userID {
		return db.User{ID: id, Language: pgtype.Text{String: f.language, Valid: true}}, nil
	}
	return db.User{}, pgx.ErrNoRows
}

// TestEmptyLanguageStaysChinese — absence is not a choice. A profile that
// never set a language, and every reader with no profile at all, reads the
// Chinese default.
func TestEmptyLanguageStaysChinese(t *testing.T) {
	for _, in := range []string{"", "   "} {
		if got := resolveLocale(in); got != LocaleZhHans {
			t.Fatalf("resolveLocale(%q) = %q, want the Chinese default", in, got)
		}
	}
	if !strings.Contains(copyFor(DefaultLocale).AgentOffline, "智能体") {
		t.Fatal("the default pack must still be the Chinese copy")
	}
}

// TestAnyChosenNonChineseLanguageReadsEnglish — the profile validates to
// en / zh-Hans / ko / ja and there are two packs. A user who picked ko or ja
// picked "not Chinese", and English is the lingua-franca pack for them.
func TestAnyChosenNonChineseLanguageReadsEnglish(t *testing.T) {
	for _, in := range []string{"en", "EN", "en-US", " en-GB ", "ko", "ja", "fr-CA"} {
		if got := resolveLocale(in); got != LocaleEn {
			t.Fatalf("resolveLocale(%q) = %q, want %q", in, got, LocaleEn)
		}
	}
	for _, in := range []string{"zh-Hans", "zh", "ZH-hant"} {
		if got := resolveLocale(in); got != LocaleZhHans {
			t.Fatalf("resolveLocale(%q) = %q, want %q", in, got, LocaleZhHans)
		}
	}
}

// TestLocaleForSenderReadsTheProfile — the whole chain: channel user id →
// binding → user → language. And every missing link lands on the default.
func TestLocaleForSenderReadsTheProfile(t *testing.T) {
	instID := uuidOf(3)
	langs := fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}

	if got := localeForSender(context.Background(), langs, instID, "T-alex"); got != LocaleEn {
		t.Fatalf("bound sender resolved %q, want their English profile", got)
	}
	if got := localeForSender(context.Background(), langs, instID, "T-stranger"); got != DefaultLocale {
		t.Fatalf("unbound sender resolved %q, want the default", got)
	}
	if got := localeForSender(context.Background(), nil, instID, "T-alex"); got != DefaultLocale {
		t.Fatalf("nil lookup resolved %q, want the default", got)
	}
	if got := localeForSender(context.Background(), langs, instID, ""); got != DefaultLocale {
		t.Fatalf("empty sender resolved %q, want the default", got)
	}
}

// TestLocaleForChatIsPersonalOnlyInPrivate — a group is many people with no
// shared profile; only a 1:1 chat's receipts follow a profile.
func TestLocaleForIsPersonalOnlyInPrivate(t *testing.T) {
	instID := uuidOf(3)
	langs := fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}

	if got := localeFor(context.Background(), langs, instID, chatTypeSingleInt, "T-alex"); got != LocaleEn {
		t.Fatalf("1:1 chat resolved %q, want the member's English", got)
	}
	if got := localeFor(context.Background(), langs, instID, chatTypeGroupInt, "R-room"); got != DefaultLocale {
		t.Fatalf("group chat resolved %q, want the default", got)
	}
}

// TestEveryLocaleFillsEveryString — a half-translated pack would send an
// empty message, which is worse than the wrong language.
func TestEveryLocaleFillsEveryString(t *testing.T) {
	for name, pack := range copyPacks {
		if pack.AgentOffline == "" || pack.AgentArchived == "" || pack.UnsupportedMsgType == "" ||
			pack.BindingPromptPrefix == "" || pack.BindingPending == "" ||
			pack.IssueCreatedPrefix == "" || pack.InboxDetailLink == "" || pack.InboxTypeFallback == "" ||
			pack.StreamMerged == "" {
			t.Fatalf("locale %q has an empty string field", name)
		}
		if len(pack.InboxTypeLabels) == 0 {
			t.Fatalf("locale %q has no inbox type labels", name)
		}
	}
	// The two packs must cover the same notification kinds, or an English
	// reader silently falls back for types Chinese names.
	zh, en := copyPacks[LocaleZhHans], copyPacks[LocaleEn]
	for k := range zh.InboxTypeLabels {
		if _, ok := en.InboxTypeLabels[k]; !ok {
			t.Fatalf("inbox type %q is named in zh-Hans but not en", k)
		}
	}
	for k := range en.InboxTypeLabels {
		if _, ok := zh.InboxTypeLabels[k]; !ok {
			t.Fatalf("inbox type %q is named in en but not zh-Hans", k)
		}
	}
}

// TestInstallConfigCarriesNoLocale — the installation-level language is gone.
// The config must not write the key, and a legacy row that still carries one
// must decode without error (the value is simply ignored).
func TestInstallConfigCarriesNoLocale(t *testing.T) {
	raw, err := encodeInstallConfig(Installation{BotID: "bot"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var asMap map[string]any
	if err := json.Unmarshal(raw, &asMap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, has := asMap["locale"]; has {
		t.Fatal("the config still writes a locale key; the language follows the reader's profile now")
	}

	legacy, err := json.Marshal(map[string]any{"app_id": "bot", "bot_id": "bot", "locale": "en"})
	if err != nil {
		t.Fatalf("marshal legacy row: %v", err)
	}
	var old installConfig
	if err := json.Unmarshal(legacy, &old); err != nil {
		t.Fatalf("a legacy row carrying the removed key must still decode: %v", err)
	}
}

// TestReplierFollowsTheSendersProfileLanguage — the outcome notices speak the
// language the asker chose in their own Multica settings.
func TestReplierFollowsTheSendersProfileLanguage(t *testing.T) {
	reg := NewSendersRegistry()
	inst := engine.ResolvedInstallation{
		ID:          uuidOf(3),
		WorkspaceID: uuidOf(4),
		Platform:    Installation{BotID: "bot"},
	}
	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	r := NewOutboundReplier(OutboundReplierConfig{
		Senders:   reg,
		Languages: fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"},
		AppURL:    "https://multica.example",
		Logger:    testLogger(),
	})
	msg := channel.InboundMessage{
		Source: channel.Source{SenderID: "T-alex", ChatID: "T-alex", ChatType: channel.ChatTypeP2P},
	}

	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeAgentOffline})
	r.Reply(context.Background(), inst, msg, engine.Result{
		Outcome:         engine.OutcomeIngested,
		IssueID:         pgtype.UUID{Bytes: [16]byte{7}, Valid: true},
		IssueIdentifier: "ACME-12",
		IssueTitle:      "Login 500",
	})

	got := contentsOf(conn)
	if len(got) != 2 {
		t.Fatalf("want two notices, got %v", got)
	}
	if !strings.Contains(got[0], "offline") {
		t.Fatalf("offline notice was not in English: %q", got[0])
	}
	if !strings.Contains(got[1], "Created") {
		t.Fatalf("issue confirmation was not in English: %q", got[1])
	}
}

// TestReplierDefaultsToChineseForAnUnboundSender — the shape every stranger is
// in, which notably includes everyone the binding prompt is for.
func TestReplierDefaultsToChineseForAnUnboundSender(t *testing.T) {
	reg := NewSendersRegistry()
	inst := engine.ResolvedInstallation{
		ID:          uuidOf(3),
		WorkspaceID: uuidOf(4),
		Platform:    Installation{BotID: "bot"},
	}
	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	r := NewOutboundReplier(OutboundReplierConfig{Senders: reg, Logger: testLogger()})

	r.Reply(context.Background(), inst, channel.InboundMessage{
		Source: channel.Source{SenderID: "T-alex", ChatID: "T-alex", ChatType: channel.ChatTypeP2P},
	}, engine.Result{Outcome: engine.OutcomeAgentArchived})

	got := contentsOf(conn)
	if len(got) != 1 || !strings.Contains(got[0], "已归档") {
		t.Fatalf("want the Chinese archived notice, got %v", got)
	}
}

// TestUnsupportedMsgTypeNoticeFollowsTheSendersLanguage — the read loop's
// receipt is sent outside the Replier, so it needs its own wiring.
func TestUnsupportedMsgTypeNoticeFollowsTheSendersLanguage(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error { return nil })
	c.languages = fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}
	sender := newWSSender(conn, testLogger())

	if err := c.dispatchFrame(context.Background(), mediaFrame("location", "msg-en"), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	got := contentsOf(conn)
	if len(got) != 1 || !strings.Contains(got[0], "text") {
		t.Fatalf("receipt was not in English: %v", got)
	}
}

// TestPlaceholdersFollowTheSendersLanguage closes the loop on the one string
// category that reaches the MODEL rather than a person: an English-profile
// sender's photo must be stored as "[Image]", not "[图片]", or the copy
// setting steers the agent's answer language through its prompt.
func TestPlaceholdersFollowTheSendersLanguage(t *testing.T) {
	handled := make(chan channel.InboundMessage, 1)
	c, _, _ := testChannel(t, func(_ context.Context, msg channel.InboundMessage) error {
		handled <- msg
		return nil
	})
	c.languages = fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}
	conn := &ackingConn{}
	sender := newWSSender(conn, testLogger())
	conn.mu.Lock()
	conn.sender = sender
	conn.mu.Unlock()

	body, _ := json.Marshal(map[string]any{
		"msgid":    "msg-img",
		"aibotid":  "bot",
		"chattype": "single",
		"from":     map[string]any{"userid": "T-alex"},
		"msgtype":  "image",
		"image":    map[string]any{"url": "https://example.com/i.jpg", "aeskey": "k"},
	})
	if err := c.dispatchFrame(context.Background(), frameEnvelope{Cmd: cmdMsgCallback, Body: body}, sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	select {
	case msg := <-handled:
		if msg.Text != copyPacks[LocaleEn].MediaImage {
			t.Fatalf("stored body = %q, want the English placeholder %q", msg.Text, copyPacks[LocaleEn].MediaImage)
		}
	default:
		t.Fatal("the image message never reached the handler")
	}
}

type stubCredentials struct{}

func (stubCredentials) Credentials(inst Installation) (InstallationCredentials, error) {
	return InstallationCredentials{BotID: inst.BotID, Secret: "s"}, nil
}

// TestFactoryWiresTheLanguageLookup closes the loop from ChannelDeps to the
// running channel: the read loop can only speak a sender's language if the
// factory handed it the lookup.
func TestFactoryWiresTheLanguageLookup(t *testing.T) {
	raw, err := encodeInstallConfig(Installation{BotID: "bot"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	langs := fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}
	factory := newWecomFactory(ChannelDeps{Credentials: stubCredentials{}, Languages: langs, Logger: testLogger()})
	built, err := factory(channel.Config{ID: uuidOf(5), Raw: raw})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if built.(*wecomChannel).languages == nil {
		t.Fatal("the channel was built without the language lookup")
	}
}

// TestInboxCardFollowsTheLocale — the type label and the link's anchor text.
func TestInboxCardFollowsTheLocale(t *testing.T) {
	t.Setenv("WECOM_APP_URL", "https://example.com")
	item := map[string]any{"type": "status_changed", "title": "Login 500", "issue_id": "iid"}

	en := buildInboxMarkdown(item, "ws-uuid", "acme", copyFor(LocaleEn))
	if !strings.Contains(en, "**[Status changed] Login 500**") {
		t.Fatalf("English card kept a Chinese type label: %q", en)
	}
	if !strings.Contains(en, "[View details](") {
		t.Fatalf("English card kept the Chinese link text: %q", en)
	}

	zh := buildInboxMarkdown(item, "ws-uuid", "acme", copyFor(LocaleZhHans))
	if !strings.Contains(zh, "**[状态变更] Login 500**") || !strings.Contains(zh, "[查看详情](") {
		t.Fatalf("Chinese card changed: %q", zh)
	}
}
