package wecom

// locale_wiring_test.go — every surface the adapter speaks on reads the copy
// pack, not a literal compiled into the file that happens to send it.
//
// Localising one surface and not the rest produces the worst possible outcome:
// a colleague whose Multica profile says English gets an English notice, then
// a Chinese binding prompt and a Chinese inbox card around it. The tests below
// drive the REAL entry points — OutboundReplier.Reply, wecomChannel
// .dispatchFrame, Outbound.tryDeliverInbox — once per language and assert on
// what came out of the socket, so putting any of those literals back fails
// here rather than passing quietly.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// localeTestUserID is the Multica user every bound sender in this file
// resolves to. Deliberately not mustTestUUID's installation id: a lookup that
// confuses the two must not pass.
var localeTestUserID = pgtype.UUID{Bytes: [16]byte{77}, Valid: true}

// fakeLanguages is a languageLookup holding one bound person: their WeCom
// userid, the Multica user it resolves to, and that profile's language.
// Anyone else is unbound, which is what a real first-time sender is.
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

// languagesFor builds a lookup for one asker reading in the given language.
func languagesFor(language string) fakeLanguages {
	return fakeLanguages{senderID: "T-asker", userID: localeTestUserID, language: language}
}

// sentMarkdown returns the content of the i-th aibot_send_msg frame.
func sentMarkdown(t *testing.T, conn *recordingConn, i int) string {
	t.Helper()
	body := conn.sendBody(t, i)
	md, ok := body["markdown"].(map[string]any)
	if !ok {
		t.Fatalf("frame %d has no markdown body: %#v", i, body)
	}
	content, _ := md["content"].(string)
	return content
}

// localeCases is the pair every surface below is driven with. The expected
// text is read off the packs rather than spelled out again: the assertion is
// that the SURFACE consults the pack, and duplicating the wording here would
// only give it a second place to drift from.
var localeCases = []struct {
	name     string
	language string
	locale   Locale
}{
	{"english profile", "en", LocaleEn},
	{"chinese profile", "zh-Hans", LocaleZhHans},
}

// ---- surface 1: replier.go ----

func TestReplierNoticesReadTheAskersLanguage(t *testing.T) {
	t.Parallel()
	for _, tc := range localeCases {
		t.Run(tc.name, func(t *testing.T) {
			want := copyPacks[tc.locale]

			for _, outcome := range []struct {
				name string
				res  engine.Result
				want string
			}{
				{"offline", engine.Result{Outcome: engine.OutcomeAgentOffline}, want.AgentOffline},
				{"archived", engine.Result{Outcome: engine.OutcomeAgentArchived}, want.AgentArchived},
				{
					"issue created",
					engine.Result{
						Outcome:         engine.OutcomeIngested,
						IssueID:         pgtype.UUID{Bytes: [16]byte{4}, Valid: true},
						IssueIdentifier: "MUL-42",
						IssueTitle:      "Login is broken",
					},
					want.issueCreated("MUL-42", "Login is broken"),
				},
				{
					"issue duplicate",
					engine.Result{
						Outcome:         engine.OutcomeIngested,
						IssueID:         pgtype.UUID{Bytes: [16]byte{4}, Valid: true},
						IssueIdentifier: "MUL-7",
						IssueTitle:      "Login is broken",
						IssueDuplicate:  true,
					},
					want.issueDuplicate("MUL-7", "Login is broken"),
				},
			} {
				t.Run(outcome.name, func(t *testing.T) {
					reg := newSendersRegistry()
					inst := engine.ResolvedInstallation{ID: mustTestUUID(t)}
					conn := &recordingConn{}
					reg.set(inst.ID, conn.autoAck(newWSSender(conn, nil)))
					r := NewOutboundReplier(OutboundReplierConfig{
						Senders:   reg,
						Languages: languagesFor(tc.language),
						AppURL:    "https://multica.example",
					})
					// A 1:1 chat, so the destination IS the asker and their
					// own profile decides the language.
					msg := channel.InboundMessage{Source: channel.Source{
						ChatID:   "T-asker",
						ChatType: channel.ChatTypeP2P,
						SenderID: "T-asker",
					}}
					r.Reply(context.Background(), inst, msg, outcome.res)

					if got := sentMarkdown(t, conn, 0); got != outcome.want {
						t.Fatalf("%s notice = %q, want the %s copy %q", outcome.name, got, tc.locale, outcome.want)
					}
				})
			}
		})
	}
}

// TestReplierGroupNoticeReadsTheRoomNotTheMember — a room has many readers and
// no shared profile, so it reads the deployment default. This is the guard on
// the fallback: it must be deploymentLocale(), not the triggering member's
// personal setting.
func TestReplierGroupNoticeReadsTheRoomNotTheMember(t *testing.T) {
	t.Parallel()
	reg := newSendersRegistry()
	inst := engine.ResolvedInstallation{ID: mustTestUUID(t)}
	conn := &recordingConn{}
	reg.set(inst.ID, conn.autoAck(newWSSender(conn, nil)))
	r := NewOutboundReplier(OutboundReplierConfig{
		Senders: reg,
		// The member who spoke reads English...
		Languages: languagesFor("en"),
		AppURL:    "https://multica.example",
	})
	msg := channel.InboundMessage{Source: channel.Source{
		ChatID:   "GROUP_CHAT",
		ChatType: channel.ChatTypeGroup,
		SenderID: "T-asker",
	}}
	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeAgentOffline})

	// ...and the room still reads the deployment default, in front of
	// everybody else in it.
	if got, want := sentMarkdown(t, conn, 0), copyFor(deploymentLocale()).AgentOffline; got != want {
		t.Fatalf("group notice = %q, want the room's language %q", got, want)
	}
}

// ---- surface 2: welcome.go ----

// welcomeRigWithLanguages is welcomeRig plus a language lookup, since the
// greeting's whole point here is which language it comes out in.
func welcomeRigWithLanguages(t *testing.T, lookup *fakeWelcomeLookup, minter binder, langs languageLookup) (*wecomChannel, *recordingConn, *wsSender) {
	t.Helper()
	c, conn, sender := welcomeRig(t, lookup, minter)
	c.languages = langs
	return c, conn, sender
}

func TestWelcomeGreetingReadsTheOpenersLanguage(t *testing.T) {
	t.Parallel()
	for _, tc := range localeCases {
		t.Run(tc.name, func(t *testing.T) {
			// Already linked: the greeting says hello and offers no link.
			lookup := &fakeWelcomeLookup{binding: db.ChannelUserBinding{}}
			c, conn, sender := welcomeRigWithLanguages(t, lookup, nil, languagesFor(tc.language))
			c.handleEnterChat(context.Background(), enterChatFrame(t, "REQ-1", "single", "T-asker"), sender, slog.Default())

			if got, want := welcomeSaid(t, conn), copyPacks[tc.locale].WelcomeBound; got != want {
				t.Fatalf("greeting = %q, want the %s copy %q", got, tc.locale, want)
			}
		})
	}
}

// TestWelcomeUnboundGreetingReadsTheDeploymentLanguage — the unbound greeting
// is prefix + URL + suffix, and all three parts have to come from one pack or
// the sentence around the link is half in each language. A person opening the
// bot for the first time is unbound by definition, so there is no profile to
// read: the deployment default is the whole answer, and it is what the
// language knob has to reach.
func TestWelcomeUnboundGreetingReadsTheDeploymentLanguage(t *testing.T) {
	// Not parallel: it moves the deployment locale, which every reader with
	// no profile reads. Restored before it returns, inside the serial phase,
	// so no parallel test ever observes it moved.
	restoreLocale(t, LocaleEn)

	lookup := &fakeWelcomeLookup{bindingErr: pgx.ErrNoRows}
	minter := &countingBinder{raw: "RAW_TOKEN"}
	c, conn, sender := welcomeRigWithLanguages(t, lookup, minter, languagesFor("en"))
	c.handleEnterChat(context.Background(), enterChatFrame(t, "REQ-1", "single", "T-nobody"), sender, slog.Default())

	got := welcomeSaid(t, conn)
	want := copyPacks[LocaleEn]
	if !strings.HasPrefix(got, want.WelcomeUnboundPrefix) || !strings.HasSuffix(got, want.WelcomeUnboundSuffix) {
		t.Fatalf("unbound greeting = %q, want it wrapped in the English pack's prefix/suffix", got)
	}
	if !strings.Contains(got, "RAW_TOKEN") {
		t.Fatalf("unbound greeting = %q, want the bind link inside it", got)
	}
}

// TestWelcomeReusedTokenSaysSoRatherThanPrintingADeadLink — Mint returns
// Reused with Raw EMPTY when the throttle suppressed it, so interpolating
// token.Raw produces a greeting whose link ends in "?token=" and redeems
// nothing. The pack has copy for exactly this case.
func TestWelcomeReusedTokenSaysSoRatherThanPrintingADeadLink(t *testing.T) {
	t.Parallel()
	lookup := &fakeWelcomeLookup{bindingErr: pgx.ErrNoRows}
	c, conn, sender := welcomeRigWithLanguages(t, lookup, fakeBinder{reused: true}, languagesFor("en"))
	c.handleEnterChat(context.Background(), enterChatFrame(t, "REQ-1", "single", "T-nobody"), sender, slog.Default())

	got := welcomeSaid(t, conn)
	if strings.Contains(got, "?token=") {
		t.Fatalf("greeting = %q, want no link at all when there is no raw token to put in one", got)
	}
	if got != copyFor(deploymentLocale()).WelcomeUnboundPending {
		t.Fatalf("greeting = %q, want the pending-link copy", got)
	}
}

// ---- surface 3: inbox_message.go ----

func TestInboxCardReadsTheRecipientsLanguage(t *testing.T) {
	// Not parallel: the card's deep link needs an app URL, and t.Setenv is
	// process-wide. It runs in the serial phase, before any parallel test
	// resumes.
	for _, tc := range localeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WECOM_APP_URL", "https://multica.example")
			q := &fakeOutboundQueries{
				memberBinding: db.ChannelUserBinding{ChannelUserID: "T-asker"},
				workspace:     db.Workspace{Slug: "acme"},
				userLanguage:  tc.language,
				userBindingID: localeTestUserID,
			}
			o, instID, conn := newOutboundWithConn(t, q)
			q.memberBinding.InstallationID = instID

			const recipient = "33333333-3333-3333-3333-333333333333"
			const workspace = "44444444-4444-4444-4444-444444444444"
			if !o.tryDeliverInbox(context.Background(), map[string]any{
				"recipient_type": "member",
				"recipient_id":   recipient,
				"workspace_id":   workspace,
				"type":           "issue_assigned",
				"title":          "New issue",
			}, recipient, workspace) {
				t.Fatal("tryDeliverInbox returned false; expected delivery to a bound member")
			}

			want := copyPacks[tc.locale]
			got := sentMarkdown(t, conn, 0)
			if !strings.HasPrefix(got, "**["+want.label("issue_assigned")+"]") {
				t.Fatalf("inbox card = %q, want the %s label %q", got, tc.locale, want.label("issue_assigned"))
			}
			if !strings.Contains(got, "["+want.InboxDetailLink+"](") {
				t.Fatalf("inbox card = %q, want the %s detail-link anchor %q", got, tc.locale, want.InboxDetailLink)
			}
		})
	}
}

// TestInboxCardUnknownTypeUsesTheSamePacksFallback — a notification kind the
// adapter has not been taught still gets a label, from the same pack as the
// rest of the card.
func TestInboxCardUnknownTypeUsesTheSamePacksFallback(t *testing.T) {
	t.Parallel()
	q := &fakeOutboundQueries{
		memberBinding: db.ChannelUserBinding{ChannelUserID: "T-asker"},
		workspace:     db.Workspace{Slug: "acme"},
		userLanguage:  "en",
		userBindingID: localeTestUserID,
	}
	o, instID, conn := newOutboundWithConn(t, q)
	q.memberBinding.InstallationID = instID

	const recipient = "33333333-3333-3333-3333-333333333333"
	const workspace = "44444444-4444-4444-4444-444444444444"
	if !o.tryDeliverInbox(context.Background(), map[string]any{
		"recipient_type": "member",
		"recipient_id":   recipient,
		"workspace_id":   workspace,
		"type":           "something_invented_next_year",
		"title":          "New issue",
	}, recipient, workspace) {
		t.Fatal("tryDeliverInbox returned false; expected delivery to a bound member")
	}
	if got, want := sentMarkdown(t, conn, 0), "**["+copyPacks[LocaleEn].InboxTypeFallback+"]"; !strings.HasPrefix(got, want) {
		t.Fatalf("inbox card = %q, want the English fallback label %q", got, want)
	}
}

// ---- surface 4: the read loop's own receipt ----

func TestUnreadableKindReceiptReadsTheSendersLanguage(t *testing.T) {
	t.Parallel()
	for _, tc := range localeCases {
		t.Run(tc.name, func(t *testing.T) {
			c := testChannel(func(context.Context, channel.InboundMessage) error { return nil })
			c.installationID = mustTestUUID(t)
			c.languages = fakeLanguages{senderID: "USER_A", userID: localeTestUserID, language: tc.language}
			conn := &recordingConn{}
			// A location card: a kind this adapter cannot read at all.
			err := c.dispatchFrame(context.Background(), msgCallbackFrame(t, "location", ""),
				conn.autoAck(newWSSender(conn, nil)), slog.Default())
			if err != nil {
				t.Fatalf("dispatchFrame: %v", err)
			}
			if got, want := sentMarkdown(t, conn, 0), copyPacks[tc.locale].UnsupportedMsgType; got != want {
				t.Fatalf("receipt = %q, want the %s copy %q", got, tc.locale, want)
			}
		})
	}
}

// ---- the deployment knob ----

// restoreLocale points the deployment at l for the duration of one test and
// puts the previous value back. Callers must NOT be parallel: this is a
// process-wide setting every reader with no profile reads.
func restoreLocale(t *testing.T, l Locale) {
	t.Helper()
	prev := deploymentLocale()
	deploymentLocaleValue.Store(l)
	t.Cleanup(func() { deploymentLocaleValue.Store(prev) })
}

// TestSetDeploymentLocaleUnsetStaysChinese is the compatibility guard: every
// existing deployment sets nothing, and nothing must keep meaning zh-Hans.
func TestSetDeploymentLocaleUnsetStaysChinese(t *testing.T) {
	if DefaultLocale != LocaleZhHans {
		t.Fatalf("DefaultLocale = %q, want zh-Hans — WeCom is a Chinese platform", DefaultLocale)
	}
	restoreLocale(t, DefaultLocale)
	if got := SetDeploymentLocale(""); got != LocaleZhHans {
		t.Fatalf("SetDeploymentLocale(\"\") = %q, want the Chinese default left in place", got)
	}
	if got := copyFor(deploymentLocale()).AgentOffline; got != copyPacks[LocaleZhHans].AgentOffline {
		t.Fatalf("unset deployment reads %q, want the Chinese pack", got)
	}
}

// TestSetDeploymentLocaleIgnoresWhatItDoesNotRecognise — an env var is
// validated by nobody. A typo must leave the language where it was rather than
// quietly moving a tenant onto the other pack.
func TestSetDeploymentLocaleIgnoresWhatItDoesNotRecognise(t *testing.T) {
	restoreLocale(t, LocaleZhHans)
	for _, junk := range []string{"zh_Hant", "english", "EN-US", `"en"`, "  ", "fr"} {
		if got := SetDeploymentLocale(junk); got != LocaleZhHans {
			t.Fatalf("SetDeploymentLocale(%q) = %q, want the previous value kept", junk, got)
		}
	}
	for _, ok := range []struct {
		in   string
		want Locale
	}{{"en", LocaleEn}, {"EN", LocaleEn}, {" en ", LocaleEn}, {"zh-Hans", LocaleZhHans}, {"zh", LocaleZhHans}} {
		if got := SetDeploymentLocale(ok.in); got != ok.want {
			t.Fatalf("SetDeploymentLocale(%q) = %q, want %q", ok.in, got, ok.want)
		}
	}
}

// TestDeploymentLocaleMovesTheCopyNobodyHasAProfileFor is the knob's whole
// justification: set it to en and the surfaces addressed to a reader nobody
// can name — a room, an unbound sender — come out in English.
func TestDeploymentLocaleMovesTheCopyNobodyHasAProfileFor(t *testing.T) {
	restoreLocale(t, LocaleEn)
	en := copyPacks[LocaleEn]

	// A room: no shared profile, so the deployment answers for it.
	reg := newSendersRegistry()
	inst := engine.ResolvedInstallation{ID: mustTestUUID(t)}
	conn := &recordingConn{}
	reg.set(inst.ID, conn.autoAck(newWSSender(conn, nil)))
	r := NewOutboundReplier(OutboundReplierConfig{
		Senders:   reg,
		Languages: languagesFor("zh-Hans"),
		AppURL:    "https://multica.example",
	})
	r.binding = fakeBinder{raw: "RAW_TOKEN"}

	r.Reply(context.Background(), inst, channel.InboundMessage{Source: channel.Source{
		ChatID:   "GROUP_CHAT",
		ChatType: channel.ChatTypeGroup,
		SenderID: "T-unbound",
	}}, engine.Result{Outcome: engine.OutcomeNeedsBinding, Sender: "T-unbound"})

	// Frame 0 is the prompt, sent privately to the sender; frame 1 is the
	// token-less line the room gets.
	prompt := sentMarkdown(t, conn, 0)
	if !strings.HasPrefix(prompt, en.BindingPromptPrefix) || !strings.HasSuffix(prompt, en.BindingPromptSuffix) {
		t.Fatalf("binding prompt = %q, want it wrapped in the English pack", prompt)
	}
	if got := sentMarkdown(t, conn, 1); got != en.BindingSentPrivately {
		t.Fatalf("group line = %q, want the English %q", got, en.BindingSentPrivately)
	}
}
