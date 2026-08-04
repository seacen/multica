// Package wecom is the WeChat Work (企业微信) smart-bot ("智能机器人" / aibot)
// adapter for the channel-agnostic inbound engine (MUL-3620). It plugs into the
// same engine.Router / ResolverSet as Feishu and Slack.
//
// Unlike the internal customer-service ("内部客服号") flow which is HTTP-callback
// based, the smart-bot flow is a client-initiated WebSocket long connection
// against wss://openws.work.weixin.qq.com. Each installation carries a
// (bot_id, secret) pair; after the WS handshake the client subscribes with
// aibot_subscribe and thereafter receives aibot_msg_callback events and sends
// aibot_send_msg / aibot_respond_msg / aibot_upload_media_* over the same
// socket. No public callback URL is required.
//
// One installation = one bot = one WebSocket. WeChat allows only one active
// connection per bot; a second connection kicks the first with a
// disconnected_event. That single-active-connection guarantee lines up with
// engine.Supervisor's WS lease, so the multi-replica invariant (at most one
// active connection per installation across processes) already holds without
// wecom-specific code.
package wecom

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TypeWecom is the channel discriminator for the WeCom smart-bot adapter.
// It is defined here alongside the wecom-specific types (rather than in
// package channel) so a build that excludes wecom does not force a package
// channel edit; slack follows the same pattern with TypeSlack.
const TypeWecom channel.Type = "wecom"

// channelTypeWecom is the string form persisted in channel_installation.
const channelTypeWecom = string(TypeWecom)

// InstallationStatus mirrors the channel_installation.status column values.
type InstallationStatus string

const (
	InstallationActive  InstallationStatus = "active"
	InstallationRevoked InstallationStatus = "revoked"
)

// Installation is the decoded, in-memory view of a WeCom smart-bot
// channel_installation row. SecretEncrypted is a secretbox-sealed blob and
// never plaintext; callers who need the plaintext go through the Credentials
// resolver.
type Installation struct {
	ID              pgtype.UUID
	WorkspaceID     pgtype.UUID
	AgentID         pgtype.UUID
	InstallerUserID pgtype.UUID
	Status          InstallationStatus

	// BotID is the smart-bot identifier the WeChat Work admin console
	// assigns at bot creation. It is BOTH the auth identity presented in
	// the aibot_subscribe frame AND the routing key we persist as
	// config->>'app_id' so GetChannelInstallationByAppID resolves an
	// inbound event to its installation.
	BotID string

	// SecretEncrypted is the sealed long-connection secret. This is
	// distinct from the token/EncodingAESKey used by callback-mode bots
	// (which we do not use). Rotated via re-install.
	SecretEncrypted []byte

	// PrincipalUserID overrides who this bot belongs to for the purpose of
	// showing a run's steps (progress_render.go). Empty — which is every row
	// that predates the field — means the installer. Set it when the person
	// who installed the bot is not the person who uses it.
	PrincipalUserID string
}

// InstallationCredentials is the plaintext-bearing view the WebSocket
// subscribe frame needs. It is minted per-connect by the credentials resolver
// so a plaintext secret never lives on the durable Installation itself.
type InstallationCredentials struct {
	BotID  string
	Secret string
}

// installConfig is the on-disk (JSONB) shape of channel_installation.config
// for wecom smart-bot rows. `app_id == bot_id` so the shared
// idx_channel_installation_type_appid index and GetChannelInstallationByAppID
// query stay generic.
type installConfig struct {
	AppID           string `json:"app_id"`
	BotID           string `json:"bot_id"`
	SecretEncrypted []byte `json:"secret_encrypted"`

	// PrincipalUserID is optional and absent on every existing row; absent
	// means the installer. An installation-level setting with a sensible
	// default, not a new table.
	//
	// There is deliberately NO locale key beside it any more. The copy
	// language follows each reader's own Multica profile (language.go); an
	// installation-wide language was a field nothing could write and the wrong
	// scope besides — a workspace speaks more than one language the moment a
	// second person binds.
	PrincipalUserID string `json:"principal_user_id,omitempty"`
}

// encodeInstallConfig marshals an Installation's config-bearing fields into
// the JSONB blob stored in channel_installation.config.
func encodeInstallConfig(inst Installation) ([]byte, error) {
	if inst.BotID == "" {
		return nil, errors.New("wecom: bot_id is required")
	}
	return json.Marshal(installConfig{
		AppID:           inst.BotID,
		BotID:           inst.BotID,
		SecretEncrypted: inst.SecretEncrypted,
		PrincipalUserID: inst.PrincipalUserID,
	})
}

// installationFromRow hydrates an Installation from a channel_installation
// row. The row's ChannelType is trusted (the callers already scope queries by
// channel_type = 'wecom'), so we don't re-check it here.
func installationFromRow(row db.ChannelInstallation) (Installation, error) {
	var cfg installConfig
	if len(row.Config) > 0 {
		if err := json.Unmarshal(row.Config, &cfg); err != nil {
			return Installation{}, fmt.Errorf("wecom: decode installation config: %w", err)
		}
	}
	return Installation{
		ID:              row.ID,
		WorkspaceID:     row.WorkspaceID,
		AgentID:         row.AgentID,
		InstallerUserID: row.InstallerUserID,
		Status:          InstallationStatus(row.Status),
		BotID:           cfg.BotID,
		SecretEncrypted: cfg.SecretEncrypted,
		PrincipalUserID: cfg.PrincipalUserID,
	}, nil
}
