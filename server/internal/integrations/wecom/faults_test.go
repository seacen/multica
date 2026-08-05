package wecom

// faults_test.go — the switch that manufactures failures has to be off unless
// somebody asked for it, and has to fire once.
//
// Both halves are safety properties, not features. A fault armed by accident
// would silently break a production tenant's replies; a fault that stayed
// armed would keep breaking them long after whoever armed it stopped watching.

import (
	"strings"
	"testing"
)

func TestNoFaultFiresUntilOneIsArmed(t *testing.T) {
	SetFaults("")
	for _, name := range []string{
		FaultDropNextSend, FaultSwallowNextAck,
		FaultRefuseNextStreamFrame, FaultStallNextWrite,
	} {
		if faultFires(name) {
			t.Errorf("%s fired with nothing armed — a production deployment would be dropping replies", name)
		}
	}
	if anyFaultArmed() {
		t.Error("something is armed after arming nothing")
	}
}

func TestAnArmedFaultFiresExactlyOnce(t *testing.T) {
	t.Cleanup(func() { SetFaults("") })

	armed := SetFaults(FaultDropNextSend)
	if len(armed) != 1 || armed[0] != FaultDropNextSend {
		t.Fatalf("SetFaults reported %v, want just %q", armed, FaultDropNextSend)
	}
	if !faultFires(FaultDropNextSend) {
		t.Fatal("the armed fault did not fire")
	}
	if faultFires(FaultDropNextSend) {
		t.Error("it fired twice — a fault that stays armed keeps breaking a session nobody is watching any more")
	}
	if anyFaultArmed() {
		t.Error("still armed after firing")
	}
}

func TestArmingOneFaultDoesNotArmTheOthers(t *testing.T) {
	t.Cleanup(func() { SetFaults("") })

	SetFaults(FaultSwallowNextAck)
	for _, other := range []string{FaultDropNextSend, FaultRefuseNextStreamFrame, FaultStallNextWrite} {
		if faultFires(other) {
			t.Errorf("arming %s also armed %s", FaultSwallowNextAck, other)
		}
	}
	if !faultFires(FaultSwallowNextAck) {
		t.Error("the one that was armed did not fire")
	}
}

// TestArmingIsExplicitAboutWhatItTook: an operator types this by hand under
// time pressure. A name that was ignored has to be visibly absent from what
// comes back, or they will send a message and wait for a fault that was never
// armed.
func TestArmingIsExplicitAboutWhatItTook(t *testing.T) {
	t.Cleanup(func() { SetFaults("") })

	armed := SetFaults("  DROP_NEXT_SEND , not_a_fault ,, swallow_next_ack ")
	got := strings.Join(armed, ",")
	if !strings.Contains(got, FaultDropNextSend) || !strings.Contains(got, FaultSwallowNextAck) {
		t.Fatalf("armed = %q, want both real names (case and spacing must not matter)", got)
	}
	if strings.Contains(got, "not_a_fault") {
		t.Errorf("armed = %q, want the unknown name left out so the operator sees it was ignored", got)
	}
}

// TestRearmingReplacesRatherThanAccumulates: SetFaults is called once per boot
// from the environment, so a second call has to describe the whole intent, not
// add to whatever the last one left behind.
func TestRearmingReplacesRatherThanAccumulates(t *testing.T) {
	t.Cleanup(func() { SetFaults("") })

	SetFaults(FaultDropNextSend)
	SetFaults(FaultStallNextWrite)

	if faultFires(FaultDropNextSend) {
		t.Error("the first arming survived a re-arm that did not name it")
	}
	if !faultFires(FaultStallNextWrite) {
		t.Error("the re-armed fault did not fire")
	}
}
