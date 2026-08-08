package wecom

// strings.go — every string a WeCom user reads, in one place, selected per
// READER.
//
// The copy used to sit inline wherever it was sent: the offline and archived
// notices in replier.go, the binding prompt built by hand a few lines below
// them, the inbox card's type labels in inbox_message.go. All of it Chinese,
// which is right for our own tenant and wrong the moment anyone else installs
// the bot — and impossible to find, because "what does this bot say" was
// spread across three files.
//
// Which pack a given message uses is decided by the DESTINATION, not by the
// installation (language.go): a 1:1 gets that person's Multica profile
// language, and a room — where there is no shared profile and no member list —
// gets the deployment's own language.
//
// Slack's adapter hardcodes English and Lark's hardcodes Chinese, so there is
// no house i18n mechanism to join. This is deliberately not one either: a
// struct of strings per locale. No catalogue files, no message ids, no plural
// rules. If wecom ever needs a third language with real formatting rules,
// that is the moment to reach for a framework — not now.
//
// The zh-Hans pack is the text this adapter was already sending, character for
// character. Nothing a Chinese tenant reads changes here; the English pack is
// new.

import (
	"strings"
	"sync/atomic"
)

// Locale names the language an installation's users are answered in.
type Locale string

const (
	LocaleZhHans Locale = "zh-Hans"
	LocaleEn     Locale = "en"

	// DefaultLocale is the compile-time fallback: Chinese, because WeCom
	// is a Chinese platform. It is what a deployment that says nothing gets.
	// Read deploymentLocale() rather than this — a deployment can say
	// otherwise, and a room's language is a property of the deployment, not of
	// whichever person happened to speak.
	DefaultLocale = LocaleZhHans
)

// deploymentLocaleValue is the language this server answers in when the reader
// is a room, or a person whose profile says nothing. Set once at boot from
// MULTICA_WECOM_DEFAULT_LOCALE (cmd/server/router.go) and read on every
// message, so it is an atomic rather than a plain var: -race would otherwise
// flag the boot write against the first inbound frame.
//
// It exists because the alternative answers are all worse. A hardcoded
// constant makes an English-speaking tenant's rooms Chinese with no way out
// but a rebuild. An installation-level column was tried and removed: nothing
// could write it. Borrowing the installer's personal profile language repeats,
// with a different person, the exact bug that motivated this — one member's
// setting deciding what a whole room reads. A deployment-level knob is the
// smallest thing that is actually about the deployment.
var deploymentLocaleValue atomic.Value

// SetDeploymentLocale fixes the deployment's language from a raw config string
// and returns what it resolved to, so the caller can log it. Anything
// unrecognised — including empty — leaves the current value in place: a typo in
// an env var must not silently switch a tenant's language.
//
// Deliberately NOT resolveLocale. That one reads a user's profile field, which
// the API has already validated to en / zh-Hans / ko / ja, so it can treat
// "anything that isn't Chinese" as a deliberate choice of the English pack. An
// env var has been validated by nobody: under that rule
// MULTICA_WECOM_DEFAULT_LOCALE=zh_Hant, or a stray quote, would quietly put a
// Chinese tenant's rooms into English. So this one matches exactly, and an
// operator who mistypes gets the old language and a log line, not a surprise.
func SetDeploymentLocale(raw string) Locale {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "zh-hans", "zh":
		deploymentLocaleValue.Store(LocaleZhHans)
		return LocaleZhHans
	case "en":
		deploymentLocaleValue.Store(LocaleEn)
		return LocaleEn
	default:
		return deploymentLocale()
	}
}

// deploymentLocale is the configured language, or DefaultLocale before
// anything has configured one — which is also what every test sees.
func deploymentLocale() Locale {
	if v, ok := deploymentLocaleValue.Load().(Locale); ok {
		return v
	}
	return DefaultLocale
}

// resolveLocale maps a user's profile language onto a supported Locale. The
// profile validates to en / zh-Hans / ko / ja (handler/auth.go), and there are
// two packs: Chinese for zh*, English for everyone who chose anything else —
// a ko or ja user deliberately picked "not Chinese", and English is the
// lingua-franca pack we have for them. Only an EMPTY value falls back to
// deploymentLocale: absence is not a choice.
func resolveLocale(s string) Locale {
	switch v := strings.ToLower(strings.TrimSpace(s)); {
	case v == "":
		return deploymentLocale()
	case strings.HasPrefix(v, "zh"):
		return LocaleZhHans
	default:
		return LocaleEn
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

	// MediaSendFailed says the agent produced a file and it did not reach the
	// chat. The answer is already on screen and may well point at the file, so
	// silence would leave the user waiting for something that is not coming.
	// One line covers the whole turn however many files failed — the reason
	// (too big, storage down, WeCom refused it) is a log line, not something
	// the reader can act on.
	MediaSendFailed string

	// BindingPromptPrefix / BindingPromptSuffix wrap the bind URL.
	// BindingPending replaces the whole thing when the mint was throttled and
	// there is no URL to print. Both go to the sender alone, never to a room
	// (replier.go) — the URL carries a bearer token.
	BindingPromptPrefix string
	BindingPromptSuffix string
	BindingPending      string

	// WelcomeBound / WelcomeUnbound* are what a person sees the moment they
	// open the bot's chat for the first time (welcome.go). Answering nothing
	// leaves them looking at an empty window with no idea what the bot is for.
	//
	// The unbound pair wraps the same bind URL the prompt uses, and for the
	// same reason it is only ever sent 1:1: the URL carries a bearer token.
	// WelcomeUnboundPending replaces both when the mint was throttled and
	// there is no URL to print.
	WelcomeBound          string
	WelcomeUnboundPrefix  string
	WelcomeUnboundSuffix  string
	WelcomeUnboundPending string

	// BindingSentPrivately is what the ROOM sees when an unbound member
	// triggers the bot in a group: the prompt itself went to that member's
	// 1:1 chat, and without this line the group would read the bot as broken.
	// It names no one and carries no token, so it is safe in front of an
	// audience of unknown size.
	BindingSentPrivately string

	// IssueCreatedPrefix leads the /issue confirmation; IssueTitleSeparator
	// joins the identifier to the title when there is one.
	IssueCreatedPrefix  string
	IssueTitleSeparator string

	// IssueDuplicatePrefix leads the answer for an /issue the engine refused
	// because an active issue already matched. The identifier and title that
	// follow belong to THAT issue, which is why the copy has to say so rather
	// than reading like a confirmation: this case used to be answered with
	// the created copy, so the person was shown a number somebody else opened,
	// under a title they never wrote, for a report that was never filed.
	IssueDuplicatePrefix string

	// The ways a streaming reply ends in something other than an answer. Each
	// one closes the loading bubble the question opened, so each one has to
	// carry visible text — WeCom discards a closing frame it considers empty
	// and the bubble spins on forever (see hasVisibleChar in ws_frame.go).
	//
	// StreamNoReply — the agent finished with nothing to say.
	// StreamMerged — a QUEUED round's run finished with nothing of its own to
	//   say; the reply ahead of it already covered this message. A first
	//   round's empty finish keeps StreamNoReply, which has no earlier answer
	//   to point at.
	// StreamNotStarted — no run was triggered at all (agent offline or
	//   archived, or the enqueue failed); the replier's own notice follows as
	//   a separate message with the detail.
	// StreamFailed — the run failed.
	// StreamCancelled — the user stopped the run, so no answer is coming.
	//   Separate copy from StreamFailed on purpose: inviting a retry of
	//   something somebody just stopped on purpose reads as the bot not having
	//   noticed.
	// StreamStillWorking — the run outlived the protocol's stream window, so
	//   we close the bubble ourselves and answer separately later.
	// StreamNoReplyWithFiles — the agent finished with no words but produced
	// files, which arrive as separate messages right after this one. Distinct
	// from StreamNoReply because that copy says nothing is coming, and then
	// something arrives: a bubble that contradicts the next message reads as a
	// bug even though both halves are working.
	StreamNoReply          string
	StreamNoReplyWithFiles string
	StreamMerged           string
	StreamNotStarted       string
	StreamFailed           string
	StreamCancelled        string
	StreamStillWorking     string

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
//
// The title is whatever the reporter typed, and this goes out as a markdown
// body — so it goes through breakMemberLinks here, at the render boundary,
// exactly as buildInboxMarkdown treats a title before putting it in a card.
// Without it, "/issue 安全升级：请点击 [重置密码](https://evil.example) 完成验证"
// comes back from the Multica bot, in the group, as a working link inside a
// confirmation everyone has reason to trust. Our own copy fragments stay raw;
// only the member-authored field is broken.
func (c copyPack) issueCreated(identifier, title string) string {
	title = breakMemberLinks(strings.TrimSpace(title))
	if title == "" {
		return c.IssueCreatedPrefix + identifier
	}
	return c.IssueCreatedPrefix + identifier + c.IssueTitleSeparator + title
}

// issueDuplicate renders the answer for an /issue the engine refused because
// an active issue already matched. The identifier and title belong to THAT
// issue, not to the message being answered.
//
// The title here belongs to somebody ELSE's issue, which makes breaking its
// links more important rather than less: the reporter is being shown text they
// did not write.
func (c copyPack) issueDuplicate(identifier, title string) string {
	out := c.IssueDuplicatePrefix + identifier
	if t := breakMemberLinks(strings.TrimSpace(title)); t != "" {
		out += c.IssueTitleSeparator + t
	}
	return out
}

// copyFor returns the pack for a locale, falling back to the deployment's.
func copyFor(l Locale) copyPack {
	if pack, ok := copyPacks[l]; ok {
		return pack
	}
	return copyPacks[deploymentLocale()]
}

var copyPacks = map[Locale]copyPack{
	LocaleZhHans: {
		AgentOffline:         "⚠️ 智能体当前不在线，你的消息已收到，等它上线后会处理。",
		AgentArchived:        "⚠️ 该智能体已归档，无法回复。请联系工作区管理员。",
		UnsupportedMsgType:   "抱歉，我目前只能处理文字消息。",
		MediaSendFailed:      "⚠️ 有文件没能发出来，我这边保留着，需要的话我再试一次。",
		BindingPromptPrefix:  "👋 请先绑定你的 Multica 账号，才能与我对话：\n",
		BindingPromptSuffix:  "\n（链接 15 分钟内有效）",
		BindingPending:       "👋 绑定链接刚才已经发给你了，就在上方，请直接点击完成绑定。",
		BindingSentPrivately: "👋 已把绑定链接私发给你，请在与我的单聊里点击完成绑定。",

		WelcomeBound:          "👋 你好，我是 Multica 智能助手。有事直接发消息给我，或者用 “/issue 标题” 建一条任务。（目前只能处理文字消息）",
		WelcomeUnboundPrefix:  "👋 你好，我是 Multica 智能助手。请先绑定你的 Multica 账号，才能与我对话：\n",
		WelcomeUnboundSuffix:  "\n（链接 15 分钟内有效）",
		WelcomeUnboundPending: "👋 你好，我是 Multica 智能助手。绑定链接刚才已经发给你了，就在上方，请直接点击完成绑定。",

		IssueCreatedPrefix:   "✅ 已创建 ",
		IssueTitleSeparator:  " — ",
		IssueDuplicatePrefix: "⚠️ 未创建 —— 已存在进行中的 ",

		StreamNoReply:          "（这轮没有需要回复的内容）",
		StreamNoReplyWithFiles: "（这轮没有文字回复，附件在下面）",
		StreamMerged:           "✅ 这条已并入上一条回复一起处理了。",
		StreamNotStarted:       "已收到，但这条暂时没能开始处理。",
		StreamFailed:           "⚠️ 这次没跑通，请稍后再试一次。",
		StreamCancelled:        "⏹️ 这次处理已取消。",
		StreamStillWorking:     "还在处理，完成后我再单独回复你。",

		InboxDetailLink: "查看详情",
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
		AgentOffline:         "⚠️ The agent is offline right now. Your message was received and will be handled once it's back.",
		AgentArchived:        "⚠️ This agent has been archived and can't reply. Please contact your workspace admin.",
		UnsupportedMsgType:   "Sorry, I can only read text messages for now.",
		MediaSendFailed:      "⚠️ I couldn't send one of the files. It is still here — say the word and I'll try again.",
		BindingPromptPrefix:  "👋 Link your Multica account before we can talk:\n",
		BindingPromptSuffix:  "\n(the link is good for 15 minutes)",
		BindingPending:       "👋 I already sent you a link — it is just above, tap it to finish linking.",
		BindingSentPrivately: "👋 I've sent the link to your direct chat with me — tap it there to finish linking.",

		WelcomeBound:          "👋 Hi, I'm the Multica assistant. Send me whatever you need, or file a task with “/issue <title>”. (I can only read text messages for now.)",
		WelcomeUnboundPrefix:  "👋 Hi, I'm the Multica assistant. Link your Multica account before we can talk:\n",
		WelcomeUnboundSuffix:  "\n(the link is good for 15 minutes)",
		WelcomeUnboundPending: "👋 Hi, I'm the Multica assistant. I already sent you a link — it is just above, tap it to finish linking.",

		IssueCreatedPrefix:   "✅ Created ",
		IssueTitleSeparator:  " — ",
		IssueDuplicatePrefix: "⚠️ Not created — an active issue already covers this: ",

		StreamNoReply:          "(nothing to reply with this round)",
		StreamNoReplyWithFiles: "(no text this round — the files follow)",
		StreamMerged:           "✅ Handled together with my previous reply.",
		StreamNotStarted:       "Got it, but this one couldn't start processing.",
		StreamFailed:           "⚠️ That run didn't go through. Please try again.",
		StreamCancelled:        "⏹️ That run was cancelled.",
		StreamStillWorking:     "Still working on it — I'll reply separately when it's done.",

		InboxDetailLink: "View details",
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
