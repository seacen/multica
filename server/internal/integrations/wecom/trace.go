package wecom

// trace.go — an opt-in record of every frame this adapter reads and writes.
//
// Why it exists: nothing else in the package logs a frame. Failures log, and
// only failures, so a run that goes wrong QUIETLY — the bubble that was
// addressed to the room instead of the person, the closer written in the wrong
// language, the command that was silently dropped — leaves no trace on the
// server at all. Verifying those needs someone to describe what they saw on a
// phone, which is slow, lossy, and impossible for anything about ordering or
// timing.
//
// With MULTICA_WECOM_TRACE=1 the server records enough to check a real-device
// session afterwards: which way the frame went, what chat it was addressed to,
// whether that chat is a room or a person, which stream it belonged to, and
// what the server said back.
//
// Off by default, and off is the right state outside a test session. What it
// records includes a bounded prefix of message text, because "which of the
// copy strings was that" cannot be answered from lengths alone — so it is
// message content, in a log, and it should be turned on deliberately and
// turned off after. The prefix is capped rather than full, the cap counts
// runes so a Chinese message is not cut mid-character, and nothing here ever
// touches a secret: the bot secret, the binding token and the media AES key
// live on fields this never reads.

import (
	"log/slog"
	"sync/atomic"
)

// tracing is set once at boot and read on every frame.
var tracing atomic.Bool

// SetTrace turns frame tracing on or off. Called from the server wiring with
// MULTICA_WECOM_TRACE; returns what it set so the caller can log it.
func SetTrace(on bool) bool {
	tracing.Store(on)
	return on
}

// tracingOn reports whether frames are being recorded.
func tracingOn() bool { return tracing.Load() }

// tracePreviewRunes bounds what of a message body reaches the log. Long enough
// to tell one copy string from another and to see which language it is in;
// short enough that a transcript is not reconstructable from the log.
const tracePreviewRunes = 120

// tracePreview returns a bounded, single-line prefix of s. Newlines become
// spaces so one frame stays one log line, and the cut is on a rune boundary.
func tracePreview(s string) string {
	out := make([]rune, 0, tracePreviewRunes)
	cut := false
	for _, r := range s {
		if len(out) == tracePreviewRunes {
			cut = true
			break
		}
		if r == '\n' || r == '\r' {
			r = ' '
		}
		out = append(out, r)
	}
	if cut {
		return string(out) + "…"
	}
	return string(out)
}

// traceOut records a frame on its way to WeChat Work. Called from the one
// place that writes the socket, so nothing can be sent without appearing here.
func traceOut(log *slog.Logger, frame map[string]any) {
	if !tracingOn() || log == nil {
		return
	}
	attrs := []any{"dir", "out"}
	if cmd, ok := frame["cmd"].(string); ok {
		attrs = append(attrs, "cmd", cmd)
	}
	if h, ok := frame["headers"].(map[string]any); ok {
		if v, ok := h["req_id"].(string); ok {
			attrs = append(attrs, "req_id", v)
		}
	}
	body, _ := frame["body"].(map[string]any)
	if body != nil {
		if v, ok := body["chatid"].(string); ok {
			attrs = append(attrs, "chatid", v)
		}
		if v, ok := body["chat_type"].(int); ok {
			attrs = append(attrs, "chat_type", v)
		}
		if v, ok := body["msgtype"].(string); ok {
			attrs = append(attrs, "msgtype", v)
		}
		if md, ok := body["markdown"].(map[string]string); ok {
			attrs = append(attrs, "len", len(md["content"]), "text", tracePreview(md["content"]))
		}
		if st, ok := body["stream"].(map[string]any); ok {
			if v, ok := st["id"].(string); ok {
				attrs = append(attrs, "stream_id", v)
			}
			if v, ok := st["finish"].(bool); ok {
				attrs = append(attrs, "finish", v)
			}
			if v, ok := st["content"].(string); ok {
				attrs = append(attrs, "len", len(v), "text", tracePreview(v))
			}
		}
	}
	log.Info("wecom trace", attrs...)
}

// traceIn records a frame arriving from WeChat Work, including the server's
// verdict on something we sent — an errcode here is how a silent failure
// becomes visible.
func traceIn(log *slog.Logger, env frameEnvelope) {
	if !tracingOn() || log == nil {
		return
	}
	log.Info("wecom trace",
		"dir", "in",
		"cmd", env.Cmd,
		"req_id", env.Headers.ReqID,
		"errcode", env.ErrCode,
		"errmsg", tracePreview(env.ErrMsg),
	)
}

// traceInbound records a normalized user message after decode: what the
// adapter believes about who sent it and where it landed, which is exactly the
// pair that gets confused (the room's id in one field, the person's in the
// other).
func traceInbound(log *slog.Logger, mc aibotMsgCallback, text string) {
	if !tracingOn() || log == nil {
		return
	}
	log.Info("wecom trace",
		"dir", "in.msg",
		"msg_id", mc.MsgID,
		"chatid", mc.ChatID,
		"chat_type", mc.ChatType,
		"sender", mc.From.UserID,
		"msgtype", mc.MsgType,
		"len", len(text),
		"text", tracePreview(text),
	)
}
