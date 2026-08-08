package wecom

// language_lint_test.go — the two locale rules, enforced instead of documented.
//
// language.go already says "reach localeForSender through localeFor" and
// strings.go already says "everything the adapter can say is a field here".
// Neither sentence stops anything: Go has no file-level privacy, and a literal
// typed into the file that sends it compiles exactly as well as a pack lookup,
// reads fine to whoever wrote it, and pins that one surface to one language
// while every other surface follows the reader.
//
// This is the state the package was already in once — the copy for the
// greeting, the binding prompt, the inbox card and the bubble each lived in
// the file that sent it — and nothing except these two tests would notice it
// coming back.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// packageGoFiles lists this package's non-test .go files, minus the ones named.
func packageGoFiles(t *testing.T, except ...string) []string {
	t.Helper()
	skip := map[string]bool{}
	for _, name := range except {
		skip[name] = true
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if skip[name] {
			continue
		}
		out = append(out, name)
	}
	return out
}

// TestOnlyLanguageGoResolvesASendersLocale — every user-visible string in this
// package is chosen by DESTINATION: a 1:1 reads the one person's profile
// language, a room reads the deployment's. localeFor is where that decision
// lives. localeForSender answers a different question — what does this PERSON
// read — and using it for a room is the bug this guards: a group message
// written in whichever member triggered it, in front of everyone else.
func TestOnlyLanguageGoResolvesASendersLocale(t *testing.T) {
	t.Parallel()

	var offenders []string
	for _, name := range packageGoFiles(t, "language.go") {
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

// TestOnlyStringsGoHoldsUserVisibleCopy — a Chinese string literal anywhere but
// strings.go is copy that cannot be translated, because nothing outside the
// pack has a second language to offer. It compiles, it reads fine to whoever
// wrote it, and it silently pins one surface to one language while the rest of
// the adapter follows the reader — which is exactly the state this package was
// in when a colleague reading English got an English bubble and a Chinese
// everything-else.
//
// Comments are exempt by construction: this walks the AST and only ever looks
// at string literals, so the reasoning in a comment can still be written in
// whichever language explains it best.
func TestOnlyStringsGoHoldsUserVisibleCopy(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()
	type offence struct {
		file string
		line int
		text string
	}
	var offenders []offence

	for _, name := range packageGoFiles(t, "strings.go") {
		f, err := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if !hasHan(lit.Value) {
				return true
			}
			offenders = append(offenders, offence{name, fset.Position(lit.Pos()).Line, lit.Value})
			return true
		})
	}

	for _, o := range offenders {
		t.Errorf("%s:%d holds user-visible copy as a literal: %s\n"+
			"Add a field to copyPack in strings.go, give it both languages, and read it through "+
			"copyFor(localeFor(...)). A literal here is a surface that cannot answer an English reader.",
			o.file, o.line, o.text)
	}
}

func hasHan(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}
