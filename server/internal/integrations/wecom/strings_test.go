package wecom

// strings_test.go — the bot's copy is now chosen per installation. Our own
// tenant must keep reading Chinese without touching anything, and an
// installation that asks for English must get English everywhere the bot
// speaks, not just in the one place someone remembered to wire up.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// TestDefaultLocaleStaysChinese — no config change may switch our own tenant.
func TestDefaultLocaleStaysChinese(t *testing.T) {
	for _, in := range []string{"", "   ", "fr-CA", "klingon"} {
		if got := resolveLocale(in); got != LocaleZhHans {
			t.Fatalf("resolveLocale(%q) = %q, want the Chinese default", in, got)
		}
	}
	if !strings.Contains(copyFor(DefaultLocale).AgentOffline, "智能体") {
		t.Fatal("the default pack must still be the Chinese copy")
	}
}

// TestEnglishLocaleIsRecognised across the spellings a config might carry.
func TestEnglishLocaleIsRecognised(t *testing.T) {
	for _, in := range []string{"en", "EN", "en-US", " en-GB "} {
		if got := resolveLocale(in); got != LocaleEn {
			t.Fatalf("resolveLocale(%q) = %q, want %q", in, got, LocaleEn)
		}
	}
}

// TestEveryLocaleFillsEveryString — a half-translated pack would send an
// empty message, which is worse than the wrong language.
func TestEveryLocaleFillsEveryString(t *testing.T) {
	for name, pack := range copyPacks {
		if pack.AgentOffline == "" || pack.AgentArchived == "" || pack.UnsupportedMsgType == "" ||
			pack.BindingPromptPrefix == "" || pack.BindingPending == "" ||
			pack.IssueCreatedPrefix == "" || pack.InboxDetailLink == "" || pack.InboxTypeFallback == "" ||
			pack.StreamQueued == "" {
			t.Fatalf("locale %q has an empty string field", name)
		}
		if len(pack.InboxTypeLabels) == 0 {
			t.Fatalf("locale %q has no inbox type labels", name)
		}
	}
	// The two packs must cover the same notification kinds, or an English
	// installation silently falls back for types Chinese names.
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

// TestInstallConfigCarriesLocale — the field round-trips through the JSONB
// column, and a row without it reads as the default.
func TestInstallConfigCarriesLocale(t *testing.T) {
	raw, err := encodeInstallConfig(Installation{BotID: "bot", Locale: "en"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var cfg installConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if cfg.Locale != "en" {
		t.Fatalf("locale did not survive the config round trip: %q", cfg.Locale)
	}

	legacy, err := json.Marshal(map[string]any{"app_id": "bot", "bot_id": "bot"})
	if err != nil {
		t.Fatalf("marshal legacy row: %v", err)
	}
	var old installConfig
	if err := json.Unmarshal(legacy, &old); err != nil {
		t.Fatalf("decode legacy: %v", err)
	}
	if resolveLocale(old.Locale) != DefaultLocale {
		t.Fatal("an installation row written before this field must read as the default")
	}
}

// TestReplierFollowsTheInstallationLocale — the outcome notices.
func TestReplierFollowsTheInstallationLocale(t *testing.T) {
	reg := NewSendersRegistry()
	inst := engine.ResolvedInstallation{
		ID:          uuidOf(3),
		WorkspaceID: uuidOf(4),
		Platform:    Installation{BotID: "bot", Locale: "en"},
	}
	conn := &recordingConn{}
	reg.set(inst.ID, newWSSender(conn, testLogger()))
	r := NewOutboundReplier(OutboundReplierConfig{
		Senders: reg,
		AppURL:  "https://multica.example",
		Logger:  testLogger(),
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

// TestReplierDefaultsToChineseWithoutALocale — the shape our own tenant is in.
func TestReplierDefaultsToChineseWithoutALocale(t *testing.T) {
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

// TestUnsupportedMsgTypeNoticeFollowsTheLocale — the read loop's receipt is
// sent outside the Replier, so it needs its own wiring.
func TestUnsupportedMsgTypeNoticeFollowsTheLocale(t *testing.T) {
	c, conn, _ := testChannel(t, func(context.Context, channel.InboundMessage) error { return nil })
	c.locale = LocaleEn
	sender := newWSSender(conn, testLogger())

	if err := c.dispatchFrame(context.Background(), mediaFrame("voice", "msg-en"), sender, testLogger()); err != nil {
		t.Fatalf("dispatchFrame: %v", err)
	}
	got := contentsOf(conn)
	if len(got) != 1 || !strings.Contains(got[0], "text") {
		t.Fatalf("receipt was not in English: %v", got)
	}
}

// TestFactoryReadsTheLocaleFromInstallationConfig closes the loop from the
// stored JSONB to the running channel.
func TestFactoryReadsTheLocaleFromInstallationConfig(t *testing.T) {
	raw, err := encodeInstallConfig(Installation{BotID: "bot", Locale: "en-US"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	factory := newWecomFactory(ChannelDeps{Credentials: stubCredentials{}, Logger: testLogger()})
	built, err := factory(channel.Config{ID: uuidOf(5), Raw: raw})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if got := built.(*wecomChannel).locale; got != LocaleEn {
		t.Fatalf("channel locale = %q, want %q", got, LocaleEn)
	}
}

type stubCredentials struct{}

func (stubCredentials) Credentials(inst Installation) (InstallationCredentials, error) {
	return InstallationCredentials{BotID: inst.BotID, Secret: "s"}, nil
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
