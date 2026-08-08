package metrics

import "github.com/prometheus/client_golang/prometheus"

// WecomMetrics is the production sink behind the WeCom adapter's Metrics
// interface (server/internal/integrations/wecom/metrics.go).
//
// The adapter is built to degrade quietly. A dial that fails, a handshake the
// server refuses, an ingest queue the read loop has to wait on — each of them
// yields the connection back to the Supervisor, which backs off and tries
// again, and none of them changes anything an operator can see. A bot that has
// been down since Tuesday and a bot nobody happened to message today produce
// the same silence.
//
// The two connection counters are deliberately separate. A dial or a read that
// fails is infrastructure and usually recovers on its own; a handshake the
// server refuses on its merits is a wrong secret or a deleted bot, and it will
// repeat identically on every backoff until a person fixes the installation.
// Summed into one number the operator cannot tell "wait" from "rotate the
// credential".
//
// No installation_id label anywhere. It is the same class of unbounded
// identifier as workspace_id and session_id, which forbiddenMetricLabels
// rejects outright; per-installation attribution is in the structured logs,
// which carry it at every one of these call sites.
// The three feature counters below follow the same rule as the connection
// pair: each one is a ratio against something already counted, never a lone
// number. A bubble that finished is only interesting beside one that fell
// back; a greeting that was sent only beside one that was not; an attachment
// that failed only beside the kind of failure it was. A count on its own
// cannot be alerted on, because nobody knows what a normal value is.
type WecomMetrics struct {
	ConnectFailures      prometheus.Counter
	AuthFailures         prometheus.Counter
	CallbacksQueued      prometheus.Counter
	CallbackQueueBlocked prometheus.Counter
	StreamFinished       prometheus.Counter
	StreamFellBack       prometheus.Counter
	Welcome              *prometheus.CounterVec
	MediaFailures        *prometheus.CounterVec
}

func NewWecomMetrics() *WecomMetrics {
	counter := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "multica", Subsystem: "wecom", Name: name, Help: help,
		})
	}
	counterVec := func(name, help, label string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "multica", Subsystem: "wecom", Name: name, Help: help,
		}, []string{label})
	}
	return &WecomMetrics{
		ConnectFailures: counter("connect_failures_total",
			"Long-connection dials and handshakes that did not complete for an infrastructure reason. Excludes credential rejections, which are counted apart."),
		AuthFailures: counter("auth_failures_total",
			"aibot_subscribe answered with a non-zero errcode. The bot stays down until somebody fixes the installation."),
		CallbacksQueued: counter("inbound_callbacks_total",
			"Inbound callbacks handed to the ingest worker. The baseline every other inbound number is read against."),
		CallbackQueueBlocked: counter("inbound_queue_blocked_total",
			"Times the read loop had to wait on a full ingest queue. Backpressure by design; a rising rate means the engine is behind and the socket is about to stop being drained."),
		StreamFinished: counter("stream_finished_total",
			"Answers that landed in the bubble the question opened."),
		StreamFellBack: counter("stream_fell_back_total",
			"Answers sent as a new message because the bubble refused the closing frame. The answer is not lost; the experience is the one the bubble was built to replace."),
		Welcome: counterVec("welcome_total",
			"enter_chat greetings by outcome: sent, skipped (a group, which is deliberate) or failed (a window that closed before the greeting was written).", "outcome"),
		MediaFailures: counterVec("media_failures_total",
			"Attachments that never reached the agent, by reason. 'blocked_address' means the media host resolved somewhere the SSRF guard refuses, which is either WeCom moving its CDN or somebody pointing us inward.", "reason"),
	}
}

func (m *WecomMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.ConnectFailures, m.AuthFailures,
		m.CallbacksQueued, m.CallbackQueueBlocked,
		m.StreamFinished, m.StreamFellBack,
		m.Welcome, m.MediaFailures,
	}
}

// ---- the adapter's Metrics interface ----

func (m *WecomMetrics) RecordConnectFailure()       { m.ConnectFailures.Inc() }
func (m *WecomMetrics) RecordAuthFailure()          { m.AuthFailures.Inc() }
func (m *WecomMetrics) RecordCallbackQueued()       { m.CallbacksQueued.Inc() }
func (m *WecomMetrics) RecordCallbackQueueBlocked() { m.CallbackQueueBlocked.Inc() }
func (m *WecomMetrics) RecordStreamFinished()       { m.StreamFinished.Inc() }
func (m *WecomMetrics) RecordStreamFellBack()       { m.StreamFellBack.Inc() }

func (m *WecomMetrics) RecordWelcomeSent()    { m.Welcome.WithLabelValues("sent").Inc() }
func (m *WecomMetrics) RecordWelcomeSkipped() { m.Welcome.WithLabelValues("skipped").Inc() }
func (m *WecomMetrics) RecordWelcomeFailed()  { m.Welcome.WithLabelValues("failed").Inc() }

func (m *WecomMetrics) RecordMediaFailure(reason string) {
	m.MediaFailures.WithLabelValues(allowedWecomLabel(reason, knownWecomMediaReasons)).Inc()
}

// The adapter passes only its own constants, but a label value that reached
// the registry unchecked would let one future call site turn a bounded series
// into an unbounded one. Anything unrecognised lands in a single bucket
// instead of minting a series of its own — and a spike on "other" is itself
// the signal that this list has drifted behind the code.
var knownWecomMediaReasons = map[string]struct{}{
	"blocked_address": {},
	"too_large":       {},
	"unreadable":      {},
}

func allowedWecomLabel(value string, known map[string]struct{}) string {
	if _, ok := known[value]; ok {
		return value
	}
	return "other"
}
