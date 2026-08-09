package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Every collector has to be registrable together. A duplicate name or a
// malformed help string only surfaces at MustRegister, which in production is
// process start.
func TestWecomMetricsRegisterCleanly(t *testing.T) {
	reg := prometheus.NewRegistry()
	for _, c := range NewWecomMetrics().Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
}

// The adapter's Metrics interface and this implementation must not drift. The
// compile-time check lives in the adapter; this one catches a method that
// exists but does nothing.
func TestEveryWecomCounterActuallyCounts(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWecomMetrics()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	m.RecordConnectFailure()
	m.RecordAuthFailure()
	m.RecordCallbackQueued()
	m.RecordCallbackQueueBlocked()
	m.RecordStreamFinished()
	m.RecordStreamFellBack()
	m.RecordWelcomeSent()
	m.RecordMediaFailure("too_large")

	seen := gatherWecomValues(t, reg)
	for _, want := range []string{
		"multica_wecom_connect_failures_total",
		"multica_wecom_auth_failures_total",
		"multica_wecom_inbound_callbacks_total",
		"multica_wecom_inbound_queue_blocked_total",
		"multica_wecom_stream_finished_total",
		"multica_wecom_stream_fell_back_total",
		"multica_wecom_welcome_total",
		"multica_wecom_media_failures_total",
	} {
		if seen[want] != 1 {
			t.Errorf("%s = %v, want 1 — the counter is wired to nothing", want, seen[want])
		}
	}
}

// The two labelled counters are the ones a call site could turn unbounded: a
// reason or an outcome spelled differently mints a series of its own, and a
// metric whose cardinality follows the code is one nobody can alert on. An
// unrecognised value is bucketed rather than dropped, so the observation still
// shows up and a spike on "other" says the allow-list has drifted.
func TestUnknownWecomLabelValuesAreBucketed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWecomMetrics()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	m.RecordMediaFailure("blocked_address")
	m.RecordMediaFailure("a-reason-invented-next-year")
	m.RecordMediaFailure("another-one")

	seen := gatherWecomLabelValues(t, reg, "multica_wecom_media_failures_total", "reason")
	if seen["blocked_address"] != 1 {
		t.Errorf("blocked_address = %v, want 1", seen["blocked_address"])
	}
	if seen["other"] != 2 {
		t.Errorf("other = %v, want 2 — two unknown reasons should share one series, not mint two", seen["other"])
	}
	if len(seen) != 2 {
		t.Errorf("reason series = %v, want exactly blocked_address and other", seen)
	}
}

// The three welcome outcomes have to be one metric with three label values
// rather than three metrics: the only reading anyone takes off it is the
// ratio, and a ratio across separate metric names is a query nobody writes.
func TestWelcomeOutcomesShareOneMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWecomMetrics()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	m.RecordWelcomeSent()
	m.RecordWelcomeSent()
	m.RecordWelcomeSkipped()
	m.RecordWelcomeFailed()

	seen := gatherWecomLabelValues(t, reg, "multica_wecom_welcome_total", "outcome")
	for outcome, want := range map[string]float64{"sent": 2, "skipped": 1, "failed": 1} {
		if seen[outcome] != want {
			t.Errorf("outcome=%s is %v, want %v", outcome, seen[outcome], want)
		}
	}
}

// The whole point of two connection counters is that an operator can tell a
// wrong secret from a dead network. Sharing a series would put them back to
// guessing.
func TestAuthAndConnectFailuresAreSeparateSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWecomMetrics()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	m.RecordAuthFailure()
	m.RecordAuthFailure()
	m.RecordConnectFailure()

	seen := gatherWecomValues(t, reg)
	if got := seen["multica_wecom_auth_failures_total"]; got != 2 {
		t.Errorf("auth failures = %v, want 2", got)
	}
	if got := seen["multica_wecom_connect_failures_total"]; got != 1 {
		t.Errorf("connect failures = %v, want 1", got)
	}
}

// No metric here may carry an unbounded identifier as a label — the same rule
// labels.go enforces for the rest of the codebase. installation_id is the one
// that would be tempting to add, and it belongs in the logs instead. Those
// carry it for the two connection failures, which reach the Supervisor as a
// returned error; the two inbound counters have no log line beside them, so
// leaving the label off really does cost the answer to "which bot" on those
// two. That is the trade this test pins, not a free win.
func TestWecomMetricsCarryNoUnboundedLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWecomMetrics()
	for _, c := range m.Collectors() {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	m.RecordConnectFailure()
	m.RecordAuthFailure()
	m.RecordCallbackQueued()
	m.RecordCallbackQueueBlocked()
	m.RecordStreamFinished()
	m.RecordStreamFellBack()
	m.RecordWelcomeSent()
	m.RecordMediaFailure("blocked_address")

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if _, forbidden := forbiddenMetricLabels[l.GetName()]; forbidden {
					t.Errorf("%s carries the unbounded label %q", f.GetName(), l.GetName())
				}
				if l.GetName() == "installation_id" {
					t.Errorf("%s labels by installation_id; that belongs in the logs", f.GetName())
				}
			}
		}
	}
}

// The registry is what production actually scrapes; a collector that is built
// but never registered exports nothing.
func TestTheRegistryExposesTheWecomCounters(t *testing.T) {
	r := NewRegistry(RegistryOptions{})
	if r.Wecom == nil {
		t.Fatal("Registry.Wecom is nil; nothing can report through it")
	}
	r.Wecom.RecordAuthFailure()

	families, err := r.Gatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "multica_wecom_auth_failures_total" {
			return
		}
	}
	t.Fatal("multica_wecom_auth_failures_total is not on the registry the metrics server scrapes")
}

func gatherWecomValues(t *testing.T, reg prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		for _, metric := range f.GetMetric() {
			out[f.GetName()] = wecomValueOf(metric)
		}
	}
	return out
}

// gatherWecomLabelValues reads one metric family's values keyed by a single
// label, which is how a bounded-cardinality claim is actually checked.
func gatherWecomLabelValues(t *testing.T, reg prometheus.Gatherer, family, label string) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != family {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == label {
					out[l.GetValue()] = wecomValueOf(metric)
				}
			}
		}
	}
	return out
}

func wecomValueOf(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	if g := m.GetGauge(); g != nil {
		return g.GetValue()
	}
	return 0
}
