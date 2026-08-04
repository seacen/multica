package wecom

// language_destination_test.go — the group half of the locale rule, driven end
// to end through the bubble rather than through localeFor alone.
//
// The unit test for localeFor was always green; the bug lived in the callers,
// and it survived because no stream test ever set TypingIndicatorConfig.
// Languages. With the lookup nil, every reader resolves to the deployment
// default, every assertion is against the Chinese pack, and swapping any
// selection site for a hardcoded default changes nothing — the guards were
// vacuous on four of seven sites. These tests give the rig a real lookup and a
// sender whose profile is English, which is the only way the two answers can
// differ at all.

import (
	"context"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
)

// withEnglishSpeakingPrincipal gives the rig a language lookup in which the
// principal's Multica profile says English. Everyone else is unbound.
func withEnglishSpeakingPrincipal(r *streamRig) {
	r.typing.languages = fakeLanguages{
		senderID: r.principalSender,
		userID:   r.inst.InstallerUserID,
		language: "en",
	}
}

// TestAGroupBubbleSpeaksTheDeploymentsLanguageNotTheTriggerers is the bug.
// An English-profile member writes in a room of Chinese speakers; every closing
// line that room can see used to come back in English, because the handle took
// its Locale from the sender with no look at the room.
func TestAGroupBubbleSpeaksTheDeploymentsLanguageNotTheTriggerers(t *testing.T) {
	r := newStreamRig(t)
	withEnglishSpeakingPrincipal(r)

	r.ingestMessage(t, groupInbound("REQ-G", "R-room", r.principalSender))
	r.typing.OnSettled(context.Background(), r.session)

	want := copyFor(DefaultLocale).StreamNotStarted
	frames := streamViews(t, &r.conn.recordingConn)
	if len(frames) == 0 {
		t.Fatal("the room was told nothing at all")
	}
	last := frames[len(frames)-1].Content
	if !strings.Contains(last, want) {
		t.Fatalf("the room read %q\nwant the deployment's language: %q", last, want)
	}
	if strings.Contains(last, copyFor(LocaleEn).StreamNotStarted) {
		t.Fatalf("the room was answered in the triggering member's own language: %q", last)
	}
}

// TestAPrivateBubbleStillSpeaksThatPersonsLanguage is the other half: the fix
// must not flatten everyone to the default. A 1:1 has exactly one reader and
// their profile still decides.
func TestAPrivateBubbleStillSpeaksThatPersonsLanguage(t *testing.T) {
	r := newStreamRig(t)
	withEnglishSpeakingPrincipal(r)

	r.ingest(t, "REQ-P")
	r.typing.OnSettled(context.Background(), r.session)

	want := copyFor(LocaleEn).StreamNotStarted
	frames := streamViews(t, &r.conn.recordingConn)
	if len(frames) == 0 {
		t.Fatal("the person was told nothing at all")
	}
	last := frames[len(frames)-1].Content
	if !strings.Contains(last, want) {
		t.Fatalf("a 1:1 read %q\nwant the member's own English: %q", last, want)
	}
}

// TestTheDeploymentDecidesWhatARoomReads: an English-speaking tenant sets
// MULTICA_WECOM_DEFAULT_LOCALE and its rooms answer in English — without
// borrowing any individual's personal setting.
//
// Not parallel, and restores what it found: the deployment locale is process
// -wide by construction.
func TestTheDeploymentDecidesWhatARoomReads(t *testing.T) {
	before := deploymentLocale()
	t.Cleanup(func() { deploymentLocaleValue.Store(before) })

	if got := SetDeploymentLocale("en"); got != LocaleEn {
		t.Fatalf("SetDeploymentLocale(en) resolved %q", got)
	}

	r := newStreamRig(t)
	// Nobody here has a profile at all — the room's language can only come
	// from the deployment.
	r.ingestMessage(t, groupInbound("REQ-D", "R-room", "T-stranger"))
	r.typing.OnSettled(context.Background(), r.session)

	frames := streamViews(t, &r.conn.recordingConn)
	if len(frames) == 0 {
		t.Fatal("the room was told nothing at all")
	}
	last := frames[len(frames)-1].Content
	if !strings.Contains(last, copyFor(LocaleEn).StreamNotStarted) {
		t.Fatalf("an English deployment's room read %q, want English", last)
	}
}

// TestASpoiltDeploymentLocaleChangesNothing: a typo in the env var must not
// silently move a tenant's language.
func TestASpoiltDeploymentLocaleChangesNothing(t *testing.T) {
	before := deploymentLocale()
	t.Cleanup(func() { deploymentLocaleValue.Store(before) })

	for _, in := range []string{"", "   ", "klingon", "zz-ZZ"} {
		SetDeploymentLocale(in)
		if got := deploymentLocale(); got != before {
			t.Fatalf("%q moved the deployment locale to %q, want it left at %q", in, got, before)
		}
	}
}

// TestTheReplierAnswersTheRoomNotTheSender covers the second ungated site: the
// offline / archived / issue-created notices are posted to the room the message
// came from, so the room's language decides them too.
func TestTheReplierAnswersTheRoomNotTheSender(t *testing.T) {
	minter := &stubMinter{}
	r, _, conn, inst, _ := replierUnder(t, minter)
	r.languages = fakeLanguages{senderID: "T-alex", userID: uuidOf(9), language: "en"}

	msg := groupTrigger()
	r.Reply(context.Background(), inst, msg, engine.Result{Outcome: engine.OutcomeAgentOffline})

	got := contentsOf(conn)
	if len(got) != 1 {
		t.Fatalf("want one notice, got %v", got)
	}
	if got[0] != copyFor(DefaultLocale).AgentOffline {
		t.Fatalf("the room read %q, want the deployment's language %q", got[0], copyFor(DefaultLocale).AgentOffline)
	}
}
