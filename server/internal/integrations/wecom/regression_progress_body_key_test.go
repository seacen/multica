package wecom

// regression_progress_body_key_test.go — a tool call's body has to stay out of
// the bubble whatever the provider called the key it arrived under.
//
// progress_render.go states the rule as an absolute: a step may name the
// argument that identifies the work, and may NEVER carry a content block. What
// enforces it for an MCP call and for an unknown tool is argsFragment, and
// argsFragment refuses exactly 24 key names (progressBodyKeys) and renders
// everything else. A denylist only knows the names somebody already wrote
// down, so the rule holds for the tools that existed when the list was written
// and fails open for every one after them: a new provider, a new tool, a
// renamed field, and the body is pasted into the principal's WeCom bubble —
// where there is no redaction, no unsend, and the room's compliance export
// keeps it.
//
// progress_detail_test.go covers the 24 names that are on the list and proves
// they are dropped. This file covers the names that are not.

import (
	"strings"
	"testing"
)

// bodyUnderAnUnlistedKey is a file body as a tool would really pass one: the
// secret is on the first line, because the fragment is cut at
// progressFragmentRunes and a leak that only showed up past the cut would
// prove nothing about what a person actually sees. The padding after it is
// what makes this a body rather than an argument — no line of a chat bubble
// was ever meant to hold it.
func bodyUnderAnUnlistedKey() string {
	return "AWS_SECRET_ACCESS_KEY=hunter2\nDB_PASSWORD=corr3ct-horse\n" +
		strings.Repeat("# nothing else on this line\n", 200)
}

// TestAToolsBodyStaysOutOfTheBubbleWhateverTheKeyIsCalled — what breaks for a
// person when this regresses: they ask the bot something from their phone, the
// agent writes or reads a file on the way to the answer, and the file's
// contents — an .env, a customer list, a draft nobody has seen — appear in the
// WeCom chat, with the secret on the first line because that is the part that
// fits. They cannot unsend it and WeCom will not redact it.
//
// Every key below is one a real provider uses for a body and none of them is
// among the 24 the adapter knows to refuse. The tools are unmapped names and
// an MCP call, which are the two paths that render the parameter list.
func TestAToolsBodyStaysOutOfTheBubbleWhateverTheKeyIsCalled(t *testing.T) {
	cases := []struct {
		name string
		// tool and input are the call as the daemon broadcasts it.
		tool  string
		input map[string]any
		// bodyKey names the key carrying the body, for the failure message.
		bodyKey string
		// named is what the step must still say once the body is gone: a step
		// stripped down to nothing is a run that looks stalled.
		named string
	}{
		{
			name:    "an edit tool that spells the new file body code",
			tool:    "insert_edit_into_file",
			input:   map[string]any{"filePath": "/srv/app/.env", "code": bodyUnderAnUnlistedKey()},
			bodyKey: "code",
			named:   "insert_edit_into_file",
		},
		{
			name:    "an edit tool that renamed new_string to new_text",
			tool:    "str_replace_based_edit_tool",
			input:   map[string]any{"path": "/srv/app/.env", "new_text": bodyUnderAnUnlistedKey()},
			bodyKey: "new_text",
			named:   "str_replace_based_edit_tool",
		},
		{
			name:    "a write tool that spells the body file_contents",
			tool:    "save_file",
			input:   map[string]any{"destination": "/srv/app/.env", "file_contents": bodyUnderAnUnlistedKey()},
			bodyKey: "file_contents",
			named:   "save_file",
		},
		{
			name:    "a runner that hands back what it captured as stdout",
			tool:    "sandbox_exec",
			input:   map[string]any{"cmd": "cat .env", "stdout": bodyUnderAnUnlistedKey()},
			bodyKey: "stdout",
			named:   "sandbox_exec",
		},
		{
			name:    "an MCP server that takes a page body as markdown",
			tool:    "mcp__notion__append_block",
			input:   map[string]any{"page": "Q3 plan", "markdown": bodyUnderAnUnlistedKey()},
			bodyKey: "markdown",
			named:   "notion",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line := lineFor(t, toolUse(tc.tool, tc.input), LocaleZhHans)

			for _, leaked := range []string{"hunter2", "AWS_SECRET_ACCESS_KEY", "corr3ct-horse"} {
				if strings.Contains(line, leaked) {
					t.Errorf("the bubble carried %q — the body %s passed under %q reached the principal's chat.\nline = %q",
						leaked, tc.tool, tc.bodyKey, line)
				}
			}

			if strings.TrimSpace(line) == "" {
				t.Errorf("%s produced no line at all; a step nobody sees happen looks like a run that stalled", tc.tool)
			}
			if !strings.Contains(line, tc.named) {
				t.Errorf("line = %q, want it still to say %q: keeping the body out must not cost the step its name", line, tc.named)
			}
		})
	}
}

// TestNoFileBodyRidesOutOnTheFrameWeComSends is the same defect at the far end
// of the path — published on the bus the way the daemon publishes it, rendered
// by the live typing manager, and read off the frame that actually goes down
// the bot's socket. The unit above says the line is wrong; this one says the
// wrong line is what the person on WeCom receives.
func TestNoFileBodyRidesOutOnTheFrameWeComSends(t *testing.T) {
	rig, bus, _, _ := busRig(t)
	rig.ingest(t, "REQ-42")

	bus.Publish(taskMessageEvent(chatTaskID(), toolUse("save_file", map[string]any{
		"destination":   "/srv/app/.env",
		"file_contents": bodyUnderAnUnlistedKey(),
	})))

	for i, frame := range streamViews(t, &rig.conn.recordingConn) {
		for _, leaked := range []string{"hunter2", "AWS_SECRET_ACCESS_KEY", "corr3ct-horse"} {
			if strings.Contains(frame.Content, leaked) {
				t.Errorf("frame %d sent to WeCom carried %q out of the written file; it cannot be unsent.\ncontent = %q",
					i, leaked, frame.Content)
			}
		}
	}
}
