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

import "strings"

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

	// UnsupportedMsgType answers a voice note, photo or file — sent from the
	// read loop, which never reaches the Replier.
	UnsupportedMsgType string

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
		UnsupportedMsgType:  "我目前只能处理文字消息，请用文字再发一次。",
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
		InboxTypeFallback: "新消息",
	},
	LocaleEn: {
		AgentOffline:        "⚠️ The agent is offline right now. Your message was received and will be handled once it's back.",
		AgentArchived:       "⚠️ This agent has been archived and can't reply. Please contact your workspace admin.",
		UnsupportedMsgType:  "I can only read text messages for now — please send it as text.",
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
		InboxTypeFallback: "Notification",
	},
}
