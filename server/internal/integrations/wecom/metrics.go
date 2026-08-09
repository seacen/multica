package wecom

// metrics.go — the health signals this adapter emits.
//
// Every failure in the connection path degrades quietly by design. A dial that
// fails and a handshake the server refuses hand the connection back to the
// Supervisor for backoff and retry; a full ingest queue parks the read loop
// instead, and the socket stops being drained until the worker catches up.
// Quiet is the right behaviour for the person in front of the chat and the
// wrong behaviour for the operator behind it — nothing on a dashboard changes
// when a bot has been unable to connect for an hour.
//
// The same is true one layer up, of the three features that answer a user
// without the user being able to tell whether they worked: a bubble that
// refused its closing frame still delivers the answer, as a new message; a
// greeting that missed its window is simply never sent (how long that window
// is, is welcomeDeadline's assumption, not a documented figure); an
// attachment that could not be fetched leaves its "[Image]" placeholder in the
// body and the agent answers as though it had seen the picture. All three read
// as a quiet afternoon from outside.
//
// The counters here are chosen for what somebody would page on rather than for
// completeness: the connection is not coming up, and if so whether that needs a
// person or just time; the read loop is being made to wait by an ingest worker
// that cannot keep up; and each of those three features has stopped doing the
// thing it exists to do.
//
// No installation id anywhere. It is an unbounded identifier and the metrics
// package rejects that class of label outright (forbiddenMetricLabels in
// internal/metrics/labels.go). What attribution exists is in the structured
// logs, and it is not uniform: the two connection counters have it, because
// the failure they count is also returned to the Supervisor, which logs it
// with installation_id. The two inbound counters have nothing beside them —
// a queue that blocks writes no log line at all, so the counter tells an
// operator that some bot is behind and not which one.

// Metrics is the sink this adapter reports to. Every method must tolerate being
// called concurrently, and none of them may block: they run on the read loop,
// on the event bus, and on the detached media path.
type Metrics interface {
	// RecordConnectFailure — a dial, a handshake write or a handshake read
	// that did not complete, or a handshake the server answered with a code
	// that classifySubscribeAck could not verify (a throttle, a platform-side
	// failure). Excludes an outright credential rejection, which has its own
	// counter: everything counted here recovers on its own, that one needs an
	// admin.
	RecordConnectFailure()
	// RecordAuthFailure — aibot_subscribe was refused on the credentials, as
	// classifySubscribeAck judges it (ErrCredentialsRejected: 40001 / 40013).
	// Deliberately not every non-zero errcode — the codes that only mean "could
	// not verify" go to RecordConnectFailure, because paging an operator to
	// rotate a good secret costs a second outage. The bot will not connect
	// until somebody fixes the credentials, so a sustained rate here is an
	// alert and not a blip.
	RecordAuthFailure()

	// RecordCallbackQueued — one inbound callback handed to the worker. The
	// baseline every other inbound number is read against.
	RecordCallbackQueued()
	// RecordCallbackQueueBlocked — the worker queue was full and the read
	// loop had to wait. Backpressure, deliberately: it is how a slow ingest
	// stops rather than a message being dropped. A rising rate says the
	// engine is not keeping up with one bot's traffic, and past a point
	// WeCom stops seeing the socket drained and replaces the connection.
	RecordCallbackQueueBlocked()

	// RecordStreamFinished / FellBack — how the bubble ended. A fall-back is
	// an answer that arrived as a new message because the bubble could not
	// take the closing frame; the answer is not lost, but the experience is
	// the one the bubble was built to replace. The ratio between the two is
	// the signal; the count of bubbles opened is not, so it is not collected.
	RecordStreamFinished()
	RecordStreamFellBack()

	// RecordWelcomeSent / Skipped / Failed — the enter_chat greeting.
	// Skipped is a group, which is deliberate and should track group traffic.
	// Failed is a window that closed before the greeting was written, and it
	// is never retried, so this counter is the only trace it leaves.
	RecordWelcomeSent()
	RecordWelcomeSkipped()
	RecordWelcomeFailed()

	// RecordMediaFailure — an attachment that never reached the agent.
	// reason is one of the mediaDrop* constants below.
	RecordMediaFailure(reason string)
}

// Reasons an attachment did not make it. Fixed set: these are label values and
// the label space has to stay bounded.
const (
	// mediaDropBlockedAddress — the media host resolved somewhere the guard
	// refuses. Either WeCom changed its CDN or somebody is pointing us at an
	// internal address; both want looking at, and they are the two an
	// operator can act on.
	mediaDropBlockedAddress = "blocked_address"
	// mediaDropTooLarge — past the size ceiling. The sender is told, so a
	// sustained rate is a conversation about the ceiling rather than a fault.
	mediaDropTooLarge = "too_large"
	// mediaDropUnreadable — everything else: an expired url, a decrypt that
	// failed, an upload the object store refused.
	mediaDropUnreadable = "unreadable"
)

// nopMetrics is what the constructor falls back to. A nil sink must never be a
// nil-pointer dereference on the read loop.
type nopMetrics struct{}

func (nopMetrics) RecordConnectFailure()       {}
func (nopMetrics) RecordAuthFailure()          {}
func (nopMetrics) RecordCallbackQueued()       {}
func (nopMetrics) RecordCallbackQueueBlocked() {}
func (nopMetrics) RecordStreamFinished()       {}
func (nopMetrics) RecordStreamFellBack()       {}
func (nopMetrics) RecordWelcomeSent()          {}
func (nopMetrics) RecordWelcomeSkipped()       {}
func (nopMetrics) RecordWelcomeFailed()        {}
func (nopMetrics) RecordMediaFailure(string)   {}

// orNopMetrics turns an unset sink into one that is safe to call.
func orNopMetrics(m Metrics) Metrics {
	if m == nil {
		return nopMetrics{}
	}
	return m
}
