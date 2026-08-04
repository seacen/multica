package wecom

// language_lint_test.go — the locale rule, enforced instead of documented.
//
// Every user-visible string in this package is chosen by DESTINATION: a 1:1
// reads the one person's profile language, a room reads the deployment's.
// localeFor is where that decision lives. localeForSender answers a different
// question — what does this PERSON read — and using it for a room is the bug
// this file exists to stop coming back: a group bubble written in whichever
// member triggered it, in front of everyone else, while the same room told the
// same thing through a different code path got a different language.
//
// A comment saying "private, go through localeFor" is not enforcement — Go has
// no file-level privacy, and four of the seven selection sites had already
// drifted before anyone noticed. This is the enforcement.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOnlyLanguageGoResolvesASendersLocale(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	var offenders []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		// language.go owns it; the tests below drive it on purpose.
		if name == "language.go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if strings.Contains(string(body), "localeForSender(") {
			offenders = append(offenders, name)
		}
	}

	if len(offenders) > 0 {
		t.Errorf("these files pick a locale from the SENDER rather than the destination: %v\n"+
			"Use localeFor(ctx, q, installationID, chatType, personID) instead — it answers "+
			"\"what does this destination read\", which is the only question the copy has. "+
			"A room is not a person: sending it one member's profile language is what this guards.",
			offenders)
	}
}
