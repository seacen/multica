package wecom

// strings.go — every string a WeCom user reads, in one place, selected per
// installation.
//
// The copy used to sit inline wherever it was sent: the offline and archived
// notices in replier.go, the binding prompt built by hand a few lines below
// them, the inbox card's type labels in inbox_message.go. All of it Chinese,
// which is right for our own tenant and wrong the moment anyone else installs
// the bot — and impossible to find, because "what does this bot say" was
// spread across three files.
//
// Slack's adapter hardcodes English and Lark's hardcodes Chinese, so there is
// no house i18n mechanism to join. This is deliberately not one either: a
// struct of strings per locale, chosen by a config field, defaulting to
// Chinese. No catalogue files, no message ids, no plural rules. If wecom ever
// needs a third language with real formatting rules, that is the moment to
// reach for a framework — not now.

import (
	"strings"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

// Locale names the language an installation's users are answered in.
type Locale string

const (
	LocaleZhHans Locale = "zh-Hans"
	LocaleEn     Locale = "en"

	// DefaultLocale is Chinese: WeChat Work is a Chinese platform and every
	// installation that predates this field is a Chinese-speaking one.
	DefaultLocale = LocaleZhHans
)

// resolveLocale maps a config value onto a supported Locale. Unknown or empty
// values fall back to DefaultLocale rather than failing — a typo in an
// installation's config should not stop the bot from talking.
func resolveLocale(s string) Locale {
	switch v := strings.ToLower(strings.TrimSpace(s)); {
	case v == "":
		return DefaultLocale
	case strings.HasPrefix(v, "en"):
		return LocaleEn
	case strings.HasPrefix(v, "zh"):
		return LocaleZhHans
	default:
		return DefaultLocale
	}
}

// copyPack is the full set of user-visible strings for one locale. Everything
// the adapter can say is a field here; nothing is built by concatenating
// fragments elsewhere.
type copyPack struct {
	// AgentOffline / AgentArchived answer an engine outcome.
	AgentOffline  string
	AgentArchived string

	// UnsupportedMsgType answers a message kind the adapter cannot read at
	// all — sent from the read loop, which never reaches the Replier.
	UnsupportedMsgType string

	// MediaImage / MediaFile / MediaVideo stand in for an attachment inside
	// the message body. They are written when the message is stored, before
	// anything has been downloaded, and they are what stays if the download
	// never lands — so they have to read as "there was a picture here", not
	// as a failure.
	MediaImage string
	MediaFile  string
	MediaVideo string

	// QuotePrefix heads the block a quoted message is rendered as. Every
	// line of the quote is marked, not just the first — an unmarked second
	// paragraph reads as the sender's own words.
	QuotePrefix string

	// MediaTooLarge / MediaUnreadable tell the sender that an attachment did
	// not make it. Two reasons rather than one because the fix differs: a
	// file over the limit needs splitting or a link, whereas an expired or
	// undecryptable download just needs sending again.
	MediaTooLarge   string
	MediaUnreadable string

	// MediaSendFailed goes the other way: the agent produced a file and it did
	// not reach the chat. The answer is already on screen and may well point at
	// the file, so silence would leave the user waiting for something that is
	// not coming. One line covers the whole turn however many files failed —
	// the reason (too big, storage down, WeCom refused it) is a log line, not
	// something the reader can act on.
	MediaSendFailed string

	// BindingPromptPrefix / BindingPromptSuffix wrap the bind URL.
	// BindingPending replaces the whole thing when the mint was throttled and
	// there is no URL to print.
	BindingPromptPrefix string
	BindingPromptSuffix string
	BindingPending      string

	// IssueCreatedPrefix leads the /issue confirmation; IssueTitleSeparator
	// joins the identifier to the title when there is one.
	IssueCreatedPrefix  string
	IssueTitleSeparator string

	// InboxDetailLink is the anchor text on the inbox card's deep link;
	// InboxTypeLabels names each notification kind, with InboxTypeFallback
	// covering a kind this adapter has not been taught yet.
	InboxDetailLink   string
	InboxTypeLabels   map[string]string
	InboxTypeFallback string

	// The four ways a streaming reply ends in something other than an answer.
	// Each one closes the loading bubble the question opened, so each one has
	// to carry visible text — WeCom ignores a closing frame it considers
	// empty and the bubble spins on forever (see stream_store.go).
	//
	// StreamNoReply — the agent finished with nothing to say.
	// StreamNotStarted — no run was triggered at all (agent offline or
	//   archived, or the enqueue failed); the replier's own notice follows as
	//   a separate message with the detail.
	// StreamFailed — the run failed.
	// StreamStillWorking — the run outlived the protocol's stream window, so
	//   we close the bubble ourselves and answer separately later.
	StreamNoReply      string
	StreamNotStarted   string
	StreamFailed       string
	StreamStillWorking string

	// StreamStuck is the odd one out: it does not close a bubble, it explains
	// one that can no longer be closed. When the server disowns a stream
	// mid-run (846608, 846605) the spinner is left turning on the user's screen
	// with nothing able to touch it, so this says so and says where the rest of
	// the round will turn up instead.
	StreamStuck string

	// StreamProgressPrefix heads the mid-run list of steps.
	StreamProgressPrefix string

	// Progress words each step the run takes. Kept in its own struct because
	// it is a table read as a table.
	Progress progressCopy
}

// progressCopy is what the bubble says while the run is still going: one whole
// line per kind of work the agent can be doing.
//
// Every line comes in two forms, and which one is used depends on whether the
// call named anything. The %s is the argument that identifies the work, and it
// has been through progress_render.go's own cleaning first — never a content
// block, never a control character, never longer than a few lines on a phone.
// See the two rules at the top of that file.
type progressCopy struct {
	// Read / Edit name the file; the Plain variants cover a call that names
	// no file this adapter recognises.
	Read      string
	ReadPlain string
	Edit      string
	EditPlain string

	// Command is a shell call; CommandNamed carries the command line.
	Command      string
	CommandNamed string

	// The four that used to say only what kind of work it was. Each Named
	// variant carries the one thing that separates this call from the next
	// one: the search term, the URL, the subagent's brief, the plan.
	Search       string
	SearchNamed  string
	Web          string
	WebNamed     string
	Subtask      string
	SubtaskNamed string
	Plan         string
	PlanNamed    string

	// Service words an MCP call as "<server> · <tool>"; ServiceArgs adds the
	// call's parameters, which for an MCP tool are the only description of
	// what it is doing that this adapter can produce.
	Service     string
	ServiceArgs string

	// Skill / SkillPlain name the packaged procedure a Skill call ran. The
	// tool is always called Skill, so the skill's own name is the line.
	Skill      string
	SkillPlain string

	// Tool / ToolArgs / Fallback cover a tool this adapter has not been
	// taught. Saying something vague beats saying nothing: a step the user
	// never sees happen is indistinguishable from a run that has stalled.
	Tool     string
	ToolArgs string
	Fallback string

	// Failed marks a step that errored; FailedNamed carries the message. The
	// run may still recover, which is why the line says so either way.
	Failed      string
	FailedNamed string

	// Thinking heads the agent's own reasoning, which sits under the step
	// list. It needs a heading because without one a paragraph of prose in
	// the middle of a status block reads as the answer arriving early.
	Thinking string

	// Elapsed closes the list with how long the user has been waiting. A
	// spinner with no clock on it reads as stuck.
	Elapsed string
}

// mediaPlaceholder returns the stand-in text for one attachment kind. An
// unknown kind falls back to the file wording, which is true of anything with
// bytes in it.
func (c copyPack) mediaPlaceholder(kind channel.MsgType) string {
	switch kind {
	case channel.MsgTypeImage:
		return c.MediaImage
	case channel.MsgTypeVideo:
		return c.MediaVideo
	default:
		return c.MediaFile
	}
}

// label returns the display name for an inbox notification type.
func (c copyPack) label(t string) string {
	if l, ok := c.InboxTypeLabels[t]; ok {
		return l
	}
	return c.InboxTypeFallback
}

// issueCreated renders the /issue confirmation.
func (c copyPack) issueCreated(identifier, title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return c.IssueCreatedPrefix + identifier
	}
	return c.IssueCreatedPrefix + identifier + c.IssueTitleSeparator + title
}

// copyFor returns the pack for a locale, falling back to DefaultLocale.
func copyFor(l Locale) copyPack {
	if pack, ok := copyPacks[l]; ok {
		return pack
	}
	return copyPacks[DefaultLocale]
}

var copyPacks = map[Locale]copyPack{
	LocaleZhHans: {
		AgentOffline:        "⚠️ 智能体当前不在线，你的消息已收到，等它上线后会处理。",
		AgentArchived:       "⚠️ 该智能体已归档，无法回复。请联系工作区管理员。",
		UnsupportedMsgType:  "我暂时读不了这种消息，麻烦用文字、图片或文件再发一次。",
		MediaImage:          "[图片]",
		MediaFile:           "[文件]",
		MediaVideo:          "[视频]",
		QuotePrefix:         "引用：",
		MediaTooLarge:       "⚠️ 有附件超过 100MB，我读不了，麻烦压缩一下或换个方式发给我。",
		MediaUnreadable:     "⚠️ 有附件我没能读取（链接可能已过期），麻烦重新发一次。",
		MediaSendFailed:     "⚠️ 有文件没能发出来，我这边保留着，需要的话我再试一次。",
		BindingPromptPrefix: "👋 请先绑定你的 Multica 账号，才能与我对话：\n",
		BindingPromptSuffix: "\n（链接 15 分钟内有效）",
		BindingPending:      "👋 绑定链接刚才已经发给你了，请点上一条消息里的链接完成绑定。",
		IssueCreatedPrefix:  "✅ 已创建 ",
		IssueTitleSeparator: " — ",
		InboxDetailLink:     "查看详情",
		InboxTypeLabels: map[string]string{
			"issue_assigned":     "任务指派",
			"mentioned":          "提及你",
			"status_changed":     "状态变更",
			"comment_added":      "新评论",
			"new_comment":        "新评论",
			"reaction_added":     "表情反应",
			"task_failed":        "任务失败",
			"unassigned":         "取消指派",
			"assignee_changed":   "指派人变更",
			"priority_changed":   "优先级变更",
			"due_date_changed":   "截止日期变更",
			"start_date_changed": "开始日期变更",
		},
		InboxTypeFallback:    "新消息",
		StreamNoReply:        "（这轮没有需要回复的内容）",
		StreamNotStarted:     "已收到，但这条暂时没能开始处理。",
		StreamFailed:         "⚠️ 这次没跑通，请稍后再试一次。",
		StreamStillWorking:   "还在处理，完成后我再单独回复你。",
		StreamStuck:          "⚠️ 上面那条进度不会再更新了，这轮的结果我用新消息发你。",
		StreamProgressPrefix: "正在处理：",
		Progress: progressCopy{
			Read:         "正在读取 %s",
			ReadPlain:    "正在读取文件",
			Edit:         "正在修改 %s",
			EditPlain:    "正在修改文件",
			Command:      "正在执行命令",
			CommandNamed: "正在执行 %s",
			Search:       "正在检索代码",
			SearchNamed:  "正在检索 %s",
			Web:          "正在查资料",
			WebNamed:     "正在查 %s",
			Subtask:      "正在派子任务",
			SubtaskNamed: "正在派子任务：%s",
			Plan:         "正在梳理计划",
			PlanNamed:    "正在梳理计划：%s",
			Service:      "正在调用 %s · %s",
			ServiceArgs:  "正在调用 %s · %s：%s",
			Skill:        "正在启用技能 %s",
			SkillPlain:   "正在启用技能",
			Tool:         "正在使用 %s",
			ToolArgs:     "正在使用 %s：%s",
			Fallback:     "正在处理",
			Failed:       "上一步出错了，正在继续",
			FailedNamed:  "上一步出错了：%s，正在继续",
			Thinking:     "思考：",
			Elapsed:      "已用时 %s",
		},
	},
	LocaleEn: {
		AgentOffline:        "⚠️ The agent is offline right now. Your message was received and will be handled once it's back.",
		AgentArchived:       "⚠️ This agent has been archived and can't reply. Please contact your workspace admin.",
		UnsupportedMsgType:  "I can't read that kind of message yet — please send text, an image or a file.",
		MediaImage:          "[Image]",
		MediaFile:           "[File]",
		MediaVideo:          "[Video]",
		QuotePrefix:         "Quoted: ",
		MediaTooLarge:       "⚠️ One of those attachments is over 100MB, which I can't read. Please compress it or send it another way.",
		MediaUnreadable:     "⚠️ I couldn't read one of those attachments — the link may have expired. Please send it again.",
		MediaSendFailed:     "⚠️ I couldn't send one of the files. It is still here — say the word and I'll try again.",
		BindingPromptPrefix: "👋 Link your Multica account before we can talk:\n",
		BindingPromptSuffix: "\n(the link is good for 15 minutes)",
		BindingPending:      "👋 I already sent you a link — tap the one in the message above to finish linking.",
		IssueCreatedPrefix:  "✅ Created ",
		IssueTitleSeparator: " — ",
		InboxDetailLink:     "View details",
		InboxTypeLabels: map[string]string{
			"issue_assigned":     "Assigned",
			"mentioned":          "Mentioned",
			"status_changed":     "Status changed",
			"comment_added":      "New comment",
			"new_comment":        "New comment",
			"reaction_added":     "Reaction",
			"task_failed":        "Task failed",
			"unassigned":         "Unassigned",
			"assignee_changed":   "Assignee changed",
			"priority_changed":   "Priority changed",
			"due_date_changed":   "Due date changed",
			"start_date_changed": "Start date changed",
		},
		InboxTypeFallback:    "Notification",
		StreamNoReply:        "(nothing to reply with this round)",
		StreamNotStarted:     "Got it, but this one couldn't start processing.",
		StreamFailed:         "⚠️ That run didn't go through. Please try again.",
		StreamStillWorking:   "Still working on it — I'll reply separately when it's done.",
		StreamStuck:          "⚠️ The status above won't update any further. I'll send this round's result as a new message.",
		StreamProgressPrefix: "Working on it: ",
		Progress: progressCopy{
			Read:         "Reading %s",
			ReadPlain:    "Reading a file",
			Edit:         "Editing %s",
			EditPlain:    "Editing a file",
			Command:      "Running a command",
			CommandNamed: "Running %s",
			Search:       "Searching the code",
			SearchNamed:  "Searching for %s",
			Web:          "Looking things up",
			WebNamed:     "Looking up %s",
			Subtask:      "Handing off a subtask",
			SubtaskNamed: "Handing off a subtask: %s",
			Plan:         "Working out a plan",
			PlanNamed:    "Working out a plan: %s",
			Service:      "Calling %s · %s",
			ServiceArgs:  "Calling %s · %s: %s",
			Skill:        "Using the %s skill",
			SkillPlain:   "Using a skill",
			Tool:         "Using %s",
			ToolArgs:     "Using %s: %s",
			Fallback:     "Working",
			Failed:       "That step errored — carrying on",
			FailedNamed:  "That step errored: %s — carrying on",
			Thinking:     "Thinking:",
			Elapsed:      "%s elapsed",
		},
	},
}
