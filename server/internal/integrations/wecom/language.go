package wecom

// language.go — which language the bot's own copy speaks, per reader.
//
// The agent's answer is never touched: it passes through in whatever language
// the model chose. What needs a decision is the adapter's own voice — the
// receipts, closers and cards in strings.go — and the decision is made per
// PERSON, not per installation. Multica already carries a validated language
// on every user profile (the web UI's own setting), so a reader we can name
// gets their profile language, and a reader we cannot — an unbound colleague,
// a group room as a whole — gets the default. There is no per-message language
// detection: profile beats heuristics, and the profile is the one signal the
// user themselves controls.
//
// The lookup is two indexed reads (binding, then user) and every caller treats
// a miss as DefaultLocale rather than an error: a receipt in the wrong
// language still says something useful, and dropping it over a language choice
// would be worse than sending it in either one.

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// languageLookup resolves the person behind a message to their profile
// language. *db.Queries satisfies it.
type languageLookup interface {
	GetChannelUserBindingByUserID(ctx context.Context, arg db.GetChannelUserBindingByUserIDParams) (db.ChannelUserBinding, error)
	GetUser(ctx context.Context, id pgtype.UUID) (db.User, error)
}

// localeForUser reads a Multica user's profile language. Anything missing —
// nil lookup, no row, an empty field — is the default.
func localeForUser(ctx context.Context, q languageLookup, userID pgtype.UUID) Locale {
	if q == nil || !userID.Valid {
		return DefaultLocale
	}
	user, err := q.GetUser(ctx, userID)
	if err != nil {
		return DefaultLocale
	}
	return resolveLocale(strings.TrimSpace(user.Language.String))
}

// localeForChat resolves the copy language for a chat as a whole. A 1:1 chat
// is one person and its chatid IS that person's bot-scoped userid, so their
// profile decides; a group is many people with no shared profile, and its
// receipts stay on the default.
func localeForChat(ctx context.Context, q languageLookup, installationID pgtype.UUID, chatType int, chatID string) Locale {
	if chatType != chatTypeSingleInt {
		return DefaultLocale
	}
	return localeForSender(ctx, q, installationID, chatID)
}

// localeForSender resolves the copy language for the WeCom user behind
// senderID on this installation: their profile language when they are bound,
// the default when they are not (or when nothing can be read). The binding row
// is the same one the identity resolver reads a moment earlier; the
// TypingNotifier and Replier seams do not carry that answer across, so this
// reads it again — one indexed row, off the ACK path.
func localeForSender(ctx context.Context, q languageLookup, installationID pgtype.UUID, senderID string) Locale {
	senderID = strings.TrimSpace(senderID)
	if q == nil || !installationID.Valid || senderID == "" {
		return DefaultLocale
	}
	binding, err := q.GetChannelUserBindingByUserID(ctx, db.GetChannelUserBindingByUserIDParams{
		InstallationID: installationID,
		ChannelUserID:  senderID,
	})
	if err != nil {
		return DefaultLocale
	}
	return localeForUser(ctx, q, binding.MulticaUserID)
}
