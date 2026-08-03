package wecom

// progress_render.go — turning what the agent is doing into something a
// colleague on WeCom can read, and keeping everything else in the run out of
// the chat.
//
// The daemon already broadcasts the transcript: task:message fires for every
// tool call, every tool result, every chunk of prose, batched about twice a
// second. That is the only mid-run signal fine-grained enough to answer "what
// is it doing right now" — task:progress fires exactly twice per run and both
// lines are for an operator, not a user. So this file consumes the transcript
// and emits a sentence.
//
// THE PRIVACY RULE, which every function below obeys and no future one may
// relax: a progress line may name the KIND of work and, at most, one fragment
// this file has vetted itself — a file's base name, the program a command
// invoked, a tool's own name. It may never carry an argument as the agent
// wrote it. Not the command line, not a file's contents, not a grep pattern,
// not a URL, not a subagent's brief, not an error's text. WeCom is a consumer
// chat surface with no redaction of its own and no way to unsend, the bubble
// is read over shoulders and forwarded, and the transcript routinely contains
// credentials, customer data and file bodies. The bubble says "正在执行命令";
// the transcript in the web UI is where someone entitled to the detail goes
// and looks.
//
// WHO IS ENTITLED is the other half of the rule, and it lives in progressLevel
// below. The paragraph above answers "what may a step say"; the tier answers
// "to whom", and outside the principal's own one-to-one chat the answer is
// nothing at all. Neither gate makes the other redundant.

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multica-ai/multica/server/pkg/protocol"
)

const (
	// progressMaxLines is how many steps the bubble shows. The window scrolls:
	// the newest work is what tells the user it is still moving, and a bubble
	// that grows without bound eventually costs more to read than it says.
	progressMaxLines = 8

	// progressMinInterval is the floor between two refresh frames of one
	// bubble. Tool calls arrive several a second; every frame is a write on
	// the bot's single socket and counts against the platform's own limits, so
	// steps inside the window are folded into the next frame rather than each
	// getting one. The closing frame never passes through here.
	progressMinInterval = 1500 * time.Millisecond

	// progressFragmentRunes caps the one vetted fragment a line may carry, so
	// a pathological name cannot turn one step into a paragraph.
	progressFragmentRunes = 40
)

// progressLevel is how much of a run one bubble may show. It is decided once,
// when the message is ingested and the audience is known, and carried on the
// stream handle for the bubble's whole life.
//
// Two tiers and no middle one. A run's steps are the principal's own working
// notes; anyone else in the conversation is a bystander to them, and a tier
// that showed bystanders "reading a file" without saying which would be honest
// but would still put a scrolling activity log into somebody else's chat. So
// the choice is the whole list or none of it.
type progressLevel uint8

const (
	// progressLevelNone shows the bubble and the answer, and nothing in
	// between. It is the zero value on purpose: a handle nobody classified
	// shows nothing.
	progressLevelNone progressLevel = iota

	// progressLevelDetail shows the run as it happens, arguments included.
	progressLevelDetail
)

// progressKind is what a step is, independent of language. The event arrives
// on a bus that knows nothing about installations, so a step is classified
// first and worded later, once the bubble's own locale is known.
type progressKind uint8

const (
	// progressRaw is a line produced by the run itself (a task:progress
	// summary). It is printed as given, only trimmed to one line.
	progressRaw progressKind = iota
	progressRead
	progressEdit
	progressCommand
	progressSearch
	progressWeb
	progressSubtask
	progressPlan
	progressService
	progressTool
	progressError
)

// progressStep is one thing the agent did, already stripped of everything the
// user may not see. arg and arg2 are only ever filled by the vetting helpers
// below — never straight from the payload.
type progressStep struct {
	kind progressKind
	arg  string
	arg2 string
}

// progressKindByTool maps the tool names the providers actually emit onto the
// kinds this adapter has words for. Keys are lowercased tool names; Claude
// Code, Codex and Cursor all appear because one workspace can run any of them.
// A name missing from here still produces a line — see step's default branch —
// so a new tool degrades to "using X" rather than to silence.
var progressKindByTool = map[string]progressKind{
	// reading
	"read":         progressRead,
	"read_file":    progressRead,
	"readfile":     progressRead,
	"notebookread": progressRead,
	"view":         progressRead,
	"open_file":    progressRead,

	// changing files
	"write":              progressEdit,
	"write_file":         progressEdit,
	"edit":               progressEdit,
	"edit_file":          progressEdit,
	"multiedit":          progressEdit,
	"multi_edit":         progressEdit,
	"notebookedit":       progressEdit,
	"str_replace_editor": progressEdit,
	"apply_patch":        progressEdit,
	"patch_apply":        progressEdit,
	"create_file":        progressEdit,

	// running things
	"bash":             progressCommand,
	"bashoutput":       progressCommand,
	"killshell":        progressCommand,
	"shell":            progressCommand,
	"exec":             progressCommand,
	"exec_command":     progressCommand,
	"run_terminal_cmd": progressCommand,
	"terminal":         progressCommand,

	// looking through the code
	"grep":            progressSearch,
	"glob":            progressSearch,
	"search":          progressSearch,
	"file_search":     progressSearch,
	"codebase_search": progressSearch,
	"list_dir":        progressSearch,
	"ls":              progressSearch,

	// looking things up outside
	"webfetch":   progressWeb,
	"web_fetch":  progressWeb,
	"websearch":  progressWeb,
	"web_search": progressWeb,
	"fetch":      progressWeb,

	// handing work off
	"task":           progressSubtask,
	"agent":          progressSubtask,
	"dispatch_agent": progressSubtask,

	// planning
	"todowrite":    progressPlan,
	"todoread":     progressPlan,
	"update_plan":  progressPlan,
	"exitplanmode": progressPlan,
}

// stepFromTaskMessage classifies one task:message, or reports that it is not
// progress at all.
//
// Only tool calls and errors qualify. A tool_result is the other half of a
// call the user has already been told about, and its output is exactly the
// kind of content this file exists to keep out of the chat. Agent text and
// thinking are the answer being written, and the answer gets its own frame.
// Rejecting all three here is also what keeps the cost of this subscriber near
// zero: together they are most of the event's volume, and none of them reaches
// the database lookup behind this function.
func stepFromTaskMessage(payload any) (progressStep, bool) {
	msgType, tool, input := taskMessageFields(payload)
	switch msgType {
	case "tool_use":
		return stepFromToolUse(tool, input), true
	case "error":
		return progressStep{kind: progressError}, true
	}
	return progressStep{}, false
}

// taskMessageFields reads the three fields that matter off a task:message
// payload, typed in-process or in its map form after a serialization round
// trip. Deliberately narrow: content and output are never read.
func taskMessageFields(payload any) (msgType, tool string, input map[string]any) {
	switch p := payload.(type) {
	case protocol.TaskMessagePayload:
		return p.Type, p.Tool, p.Input
	case *protocol.TaskMessagePayload:
		if p == nil {
			return "", "", nil
		}
		return p.Type, p.Tool, p.Input
	case map[string]any:
		msgType, _ = p["type"].(string)
		tool, _ = p["tool"].(string)
		input, _ = p["input"].(map[string]any)
		return msgType, tool, input
	}
	return "", "", nil
}

// stepFromToolUse decides what a tool call is doing and picks the one fragment
// worth naming. Every branch either takes a vetted fragment or takes nothing.
func stepFromToolUse(tool string, input map[string]any) progressStep {
	if server, name, ok := mcpToolParts(tool); ok {
		return progressStep{kind: progressService, arg: server, arg2: name}
	}
	switch progressKindByTool[strings.ToLower(strings.TrimSpace(tool))] {
	case progressRead:
		return progressStep{kind: progressRead, arg: fileFragment(input)}
	case progressEdit:
		return progressStep{kind: progressEdit, arg: fileFragment(input)}
	case progressCommand:
		return progressStep{kind: progressCommand, arg: commandFragment(input)}
	case progressSearch:
		return progressStep{kind: progressSearch}
	case progressWeb:
		return progressStep{kind: progressWeb}
	case progressSubtask:
		return progressStep{kind: progressSubtask}
	case progressPlan:
		return progressStep{kind: progressPlan}
	}
	// An unknown tool still gets a line: a step the user never sees happen is
	// indistinguishable from a run that has stalled.
	return progressStep{kind: progressTool, arg: safeFragment(tool)}
}

// line words a step in one locale, for one audience. The only values
// interpolated are the step's own fragments, which the vetting helpers have
// already cleaned.
//
// The tier check is HERE, at the single point where a step becomes words,
// rather than in each branch below or in each caller. That is what makes it
// hold for a tool type nobody has written yet: a new kind gets a case in this
// switch, and inherits the gate whether its author thought about audiences or
// not. recordStep has a cheaper check of its own, but this is the one that is
// exhaustive.
func (s progressStep) line(c copyPack, level progressLevel) string {
	if level != progressLevelDetail {
		return ""
	}
	p := c.Progress
	switch s.kind {
	case progressRaw:
		return s.arg
	case progressRead:
		if s.arg == "" {
			return p.ReadPlain
		}
		return fmt.Sprintf(p.Read, s.arg)
	case progressEdit:
		if s.arg == "" {
			return p.EditPlain
		}
		return fmt.Sprintf(p.Edit, s.arg)
	case progressCommand:
		if s.arg == "" {
			return p.Command
		}
		return fmt.Sprintf(p.CommandNamed, s.arg)
	case progressSearch:
		return p.Search
	case progressWeb:
		return p.Web
	case progressSubtask:
		return p.Subtask
	case progressPlan:
		return p.Plan
	case progressService:
		return fmt.Sprintf(p.Service, s.arg, s.arg2)
	case progressError:
		return p.Failed
	case progressTool:
		if s.arg == "" {
			return p.Fallback
		}
		return fmt.Sprintf(p.Tool, s.arg)
	}
	return ""
}

// fileFragment returns the BASE NAME of the file a tool call names, and
// nothing else. The directory is dropped as well as the contents: a path can
// be a customer's name or an unreleased project's, and the base name is what
// tells the user which file without telling them where it lives.
func fileFragment(input map[string]any) string {
	for _, key := range []string{"file_path", "path", "notebook_path", "target_file", "filename", "file"} {
		v, _ := input[key].(string)
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		// Windows separators too — the daemon runs wherever the user's machine is.
		v = v[strings.LastIndexAny(v, `\`)+1:]
		if base := safeFragment(path.Base(v)); base != "" && base != "." && base != "/" {
			return base
		}
	}
	return ""
}

// commandFragment returns the program a command invoked and drops its whole
// argument list. `git`, `go`, `pytest` tell the user what kind of work is
// happening; the arguments are where the URLs, hostnames and credentials are.
// A wrapper shell is treated as no name at all — "正在执行 bash 命令" says
// nothing the plain line does not.
func commandFragment(input map[string]any) string {
	var raw string
	for _, key := range []string{"command", "cmd", "script"} {
		if v, _ := input[key].(string); strings.TrimSpace(v) != "" {
			raw = strings.TrimSpace(v)
			break
		}
	}
	if raw == "" {
		return ""
	}
	first, _, _ := strings.Cut(raw, " ")
	first = first[strings.LastIndexAny(first, `/\`)+1:]
	if first == "" || len(first) > 16 {
		return ""
	}
	switch first {
	case "bash", "sh", "zsh", "fish", "env", "sudo", "nohup", "time":
		return ""
	}
	// Anything that is not a plain program name — an env assignment, a pipe, a
	// quoted string, a path fragment with spaces — is dropped rather than
	// guessed at. A conservative allow-list is the point: this is the one
	// place a command's own text could reach the chat.
	for i, r := range first {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case i > 0 && (r >= '0' && r <= '9'):
		case i > 0 && (r == '-' || r == '_' || r == '.' || r == '+'):
		default:
			return ""
		}
	}
	return first
}

// mcpToolParts splits an MCP tool name — `mcp__<server>__<tool>` — into the
// service and the operation. Both halves are identifiers the workspace
// configured, not anything the agent composed, so naming them is safe and is
// the only way "正在调用 calendar · list_events" beats "正在使用某个工具".
func mcpToolParts(tool string) (server, name string, ok bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(tool), "mcp__")
	if !found {
		return "", "", false
	}
	server, name, _ = strings.Cut(rest, "__")
	server, name = safeFragment(server), safeFragment(name)
	if server == "" {
		return "", "", false
	}
	if name == "" {
		name = server
	}
	return server, name, true
}

// safeFragment makes a fragment fit to be pasted into the bubble: one line, no
// control characters, no markup that could break out of the <think> wrapper
// the body is built from, and short enough to stay one line on a phone.
func safeFragment(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r < 0x20 || r == 0x7f:
			return -1
		case r == '<' || r == '>' || r == '&':
			return -1
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if runes := []rune(s); len(runes) > progressFragmentRunes {
		s = string(runes[:progressFragmentRunes]) + "…"
	}
	return s
}

// ---- the rolling feed ----

// progressLine is one step as the user reads it, with how many times in a row
// it happened. Twenty greps in a row are one line and a count, not twenty
// lines that push everything else out of the window.
type progressLine struct {
	text  string
	count int
}

// progressFeed is one bubble's scrolling list of steps, plus the throttle that
// decides when the list is worth another frame. One per open bubble, owned by
// the stream store so it dies exactly when the bubble does.
type progressFeed struct {
	mu        sync.Mutex
	lines     []progressLine
	lastFlush time.Time
	now       func() time.Time
}

func newProgressFeed(now func() time.Time) *progressFeed {
	if now == nil {
		now = time.Now
	}
	return &progressFeed{now: now}
}

// record folds one step into the feed and returns the bubble body to send —
// or "" when this step merges into a frame that has already gone out inside
// the throttle window. A merged step is not lost: it is in the list, and the
// next frame to go out carries it.
//
// openedAt is when the user asked, which is what the elapsed clock counts
// from — not when the first step happened.
func (f *progressFeed) record(step progressStep, c copyPack, openedAt time.Time, level progressLevel) string {
	text := strings.TrimSpace(step.line(c, level))
	if text == "" {
		return ""
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if n := len(f.lines); n > 0 && f.lines[n-1].text == text {
		f.lines[n-1].count++
	} else {
		f.lines = append(f.lines, progressLine{text: text, count: 1})
		if len(f.lines) > progressMaxLines {
			f.lines = append(f.lines[:0], f.lines[len(f.lines)-progressMaxLines:]...)
		}
	}

	now := f.now()
	if !f.lastFlush.IsZero() && now.Sub(f.lastFlush) < progressMinInterval {
		return ""
	}
	f.lastFlush = now
	return f.renderLocked(c, now.Sub(openedAt))
}

// renderLocked builds the bubble body. Everything sits inside <think> tags so
// the client renders it as its own thinking affordance rather than as the
// bot's answer — a run that dies before chat:done would otherwise leave a list
// of half-finished steps looking like a reply. Caller holds f.mu.
func (f *progressFeed) renderLocked(c copyPack, elapsed time.Duration) string {
	var b strings.Builder
	b.WriteString("<think>")
	b.WriteString(strings.TrimSpace(c.StreamProgressPrefix))
	for _, l := range f.lines {
		b.WriteString("\n· ")
		b.WriteString(l.text)
		if l.count > 1 {
			b.WriteString(" ×")
			b.WriteString(strconv.Itoa(l.count))
		}
	}
	if elapsed >= time.Second {
		b.WriteString("\n")
		b.WriteString(fmt.Sprintf(c.Progress.Elapsed, formatElapsed(elapsed)))
	}
	b.WriteString("</think>")
	return b.String()
}

// formatElapsed renders a duration the same way the web chat's status pill
// does (packages/views/chat/lib/format.ts), so the same run reads the same on
// both surfaces. Digits and unit letters carry across languages, which is why
// this needs no copy of its own.
func formatElapsed(d time.Duration) string {
	secs := int(d / time.Second)
	if secs < 0 {
		secs = 0
	}
	if secs < 60 {
		return strconv.Itoa(secs) + "s"
	}
	m, s := secs/60, secs%60
	if s == 0 {
		return strconv.Itoa(m) + "m"
	}
	return strconv.Itoa(m) + "m " + strconv.Itoa(s) + "s"
}
