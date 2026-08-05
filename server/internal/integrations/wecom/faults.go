package wecom

// faults.go — a switch that makes a failure happen on purpose, so the paths
// that only run when something breaks can be walked on real hardware.
//
// Why this exists. A good half of what can go wrong here only goes wrong when
// something else fails first: WeCom refuses a frame, an acknowledgement never
// comes back, the socket drops mid-drain, an upload takes longer than its
// budget. None of that can be produced by using the product — you cannot make
// the platform refuse a frame by tapping harder — so those paths were only
// ever reachable from an in-process test. That is a real gap: an in-process
// test proves the code branches correctly, not that the branch does anything
// useful to a person holding a phone.
//
// With MULTICA_WECOM_FAULTS set, an operator can arm one fault, send one
// message, and watch the recovery path run against the real platform, the real
// long connection and a real agent. The failure is manufactured; nothing else
// about the trip is.
//
// Off by default, and this is not a thing to run with. Every fault is
// one-shot: it fires once and disarms, so a session cannot be left quietly
// broken by an arm nobody remembers. Arming anything at all logs a warning
// naming what was armed.
//
// Set it to a comma-separated list, e.g.
//
//	MULTICA_WECOM_FAULTS=drop_next_send,refuse_next_stream_frame

import (
	"log/slog"
	"strings"
	"sync"
)

// Fault names. Each is one-shot.
const (
	// FaultDropNextSend makes the next outbound message vanish between "the
	// registry accepted it" and the socket — the shape of a holding-queue
	// overflow or a write the platform silently discarded. Used to walk the
	// binding-prompt dead end: the prompt is lost, and what the user is told
	// the next time they write is the thing under test.
	FaultDropNextSend = "drop_next_send"

	// FaultSwallowNextAck withholds the acknowledgement for the next frame
	// that waits on one, so the caller times out on a frame that actually
	// landed. Used to walk the media double-delivery path.
	FaultSwallowNextAck = "swallow_next_ack"

	// FaultRefuseNextStreamFrame answers the next stream frame with 846608 —
	// the platform saying it will not take another frame for that bubble. Used
	// to walk the disowned-bubble notice and the fall back to a plain message.
	FaultRefuseNextStreamFrame = "refuse_next_stream_frame"

	// FaultStallNextWrite holds the next socket write past a caller's budget
	// without failing it, which is how a slow platform eats a deadline that
	// belongs to somebody else.
	FaultStallNextWrite = "stall_next_write"
)

var knownFaults = map[string]bool{
	FaultDropNextSend:          true,
	FaultSwallowNextAck:        true,
	FaultRefuseNextStreamFrame: true,
	FaultStallNextWrite:        true,
}

var (
	faultMu    sync.Mutex
	armedFault = map[string]bool{}
)

// SetFaults arms the faults named in a comma-separated config string and
// returns the ones it recognised, so the caller can log exactly what is live.
// An unknown name is ignored rather than fatal: this is a debugging aid, and
// refusing to boot over a typo in it would be a worse failure than the typo.
func SetFaults(raw string) []string {
	faultMu.Lock()
	defer faultMu.Unlock()
	armedFault = map[string]bool{}
	var armed []string
	for _, name := range strings.Split(raw, ",") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || !knownFaults[name] {
			continue
		}
		armedFault[name] = true
		armed = append(armed, name)
	}
	return armed
}

// faultFires reports whether a fault is armed, and disarms it. One-shot by
// design: a fault that stayed armed would keep breaking a session long after
// whoever armed it stopped watching, and the point is to walk one recovery
// path once.
func faultFires(name string) bool {
	faultMu.Lock()
	defer faultMu.Unlock()
	if !armedFault[name] {
		return false
	}
	delete(armedFault, name)
	return true
}

// anyFaultArmed reports whether anything is still waiting to fire. Read at
// boot only, for the warning.
func anyFaultArmed() bool {
	faultMu.Lock()
	defer faultMu.Unlock()
	return len(armedFault) > 0
}

// logFault records a fault firing. It is a warning rather than a debug line on
// purpose: whatever happens next in the log is not the system's own behaviour,
// and somebody reading it later needs to know that.
func logFault(log *slog.Logger, name, where string) {
	if log == nil {
		log = slog.Default()
	}
	log.Warn("wecom: INJECTED FAULT fired — the failure below was manufactured, not real",
		"fault", name, "at", where)
}
