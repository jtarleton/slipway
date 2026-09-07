package jobs

import "testing"

func TestTerminalStates(t *testing.T) {
	terminal := map[State]bool{
		Pending:   false,
		Submitted: false,
		Running:   false,
		Succeeded: true,
		Failed:    true,
		Cancelled: true,
		Orphaned:  true,
	}

	for state, want := range terminal {
		if got := state.Terminal(); got != want {
			t.Errorf("%s.Terminal() = %v, want %v", state, got, want)
		}
		if !state.Valid() {
			t.Errorf("%s.Valid() = false, want true", state)
		}
	}

	if State("bogus").Valid() {
		t.Error(`State("bogus").Valid() = true, want false`)
	}
	if State("bogus").Terminal() {
		t.Error(`State("bogus").Terminal() = true, want false`)
	}
}

func TestCanTransition(t *testing.T) {
	tests := []struct {
		from, to State
		want     bool
	}{
		{Pending, Submitted, true},
		{Pending, Succeeded, true}, // crash after submit, Job finished meanwhile
		{Pending, Orphaned, true},  // crash after submit, Job swept meanwhile
		{Submitted, Running, true},
		{Running, Failed, true},
		{Running, Orphaned, true},

		{Succeeded, Failed, false}, // terminal states are terminal
		{Failed, Running, false},
		{Cancelled, Pending, false},
		{Orphaned, Running, false},
		{Submitted, Pending, false}, // no going backwards
		{Running, Submitted, false},

		{State("bogus"), Running, false},
		{Running, State("bogus"), false},
	}

	for _, tt := range tests {
		if got := CanTransition(tt.from, tt.to); got != tt.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestNoSelfTransitions(t *testing.T) {
	for _, s := range States() {
		if CanTransition(s, s) {
			t.Errorf("CanTransition(%s, %s) = true; a state must not transition to itself", s, s)
		}
	}
}

func TestDecide(t *testing.T) {
	tests := []struct {
		name   string
		cur    State
		obs    Observation
		action Action
		next   State
	}{
		{
			name:   "pending with no Job submits",
			cur:    Pending,
			obs:    Observation{Found: false},
			action: Submit,
			next:   Submitted,
		},
		{
			name:   "pending with an existing idle Job adopts it",
			cur:    Pending,
			obs:    Observation{Found: true},
			action: Adopt,
			next:   Submitted,
		},
		{
			name:   "pending with an already-running Job jumps to running",
			cur:    Pending,
			obs:    Observation{Found: true, Active: 1},
			action: Adopt,
			next:   Running,
		},
		{
			name:   "pending with an already-finished Job finalizes",
			cur:    Pending,
			obs:    Observation{Found: true, Succeeded: 1},
			action: Finalize,
			next:   Succeeded,
		},
		{
			name:   "submitted with an active pod moves to running",
			cur:    Submitted,
			obs:    Observation{Found: true, Active: 1},
			action: Adopt,
			next:   Running,
		},
		{
			name:   "submitted with no pod yet waits",
			cur:    Submitted,
			obs:    Observation{Found: true},
			action: Wait,
			next:   Submitted,
		},
		{
			name:   "running with an active pod waits",
			cur:    Running,
			obs:    Observation{Found: true, Active: 1},
			action: Wait,
			next:   Running,
		},
		{
			name:   "running to success",
			cur:    Running,
			obs:    Observation{Found: true, Succeeded: 1},
			action: Finalize,
			next:   Succeeded,
		},
		{
			name:   "retries exhausted is a failure",
			cur:    Running,
			obs:    Observation{Found: true, Failed: 3},
			action: Finalize,
			next:   Failed,
		},
		{
			name:   "a failed pod with a retry still pending keeps running",
			cur:    Running,
			obs:    Observation{Found: true, Failed: 1, Active: 1},
			action: Wait,
			next:   Running,
		},
		{
			name:   "success wins over earlier pod failures",
			cur:    Running,
			obs:    Observation{Found: true, Failed: 2, Succeeded: 1},
			action: Finalize,
			next:   Succeeded,
		},
		{
			name:   "deadline exceeded is a failure",
			cur:    Running,
			obs:    Observation{Found: true, Active: 1, DeadlineExceeded: true},
			action: Finalize,
			next:   Failed,
		},
		{
			name:   "a vanished Job is orphaned, not failed",
			cur:    Running,
			obs:    Observation{Found: false},
			action: Finalize,
			next:   Orphaned,
		},
		{
			name:   "a vanished submitted Job is orphaned too",
			cur:    Submitted,
			obs:    Observation{Found: false},
			action: Finalize,
			next:   Orphaned,
		},
		{
			name:   "terminal states are left alone",
			cur:    Succeeded,
			obs:    Observation{Found: false},
			action: Wait,
			next:   Succeeded,
		},
		{
			name:   "a cancelled job is not resurrected by a running pod",
			cur:    Cancelled,
			obs:    Observation{Found: true, Active: 1},
			action: Wait,
			next:   Cancelled,
		},
		{
			name:   "an unrecognized state is never acted on",
			cur:    State("bogus"),
			obs:    Observation{Found: false},
			action: Wait,
			next:   State("bogus"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Decide(tt.cur, tt.obs)
			if got.Action != tt.action || got.Next != tt.next {
				t.Errorf("Decide(%s, %+v) = {%s -> %s}, want {%s -> %s}\nreason: %s",
					tt.cur, tt.obs, got.Action, got.Next, tt.action, tt.next, got.Reason)
			}
			if got.Reason == "" {
				t.Error("Decide returned an empty reason; every decision must explain itself")
			}
		})
	}
}

// observations enumerates every shape of cluster report the reconciler can see.
func observations() []Observation {
	var out []Observation
	for _, found := range []bool{false, true} {
		for _, active := range []int32{0, 1} {
			for _, succeeded := range []int32{0, 1} {
				for _, failed := range []int32{0, 3} {
					for _, deadline := range []bool{false, true} {
						out = append(out, Observation{
							Found:            found,
							Active:           active,
							Succeeded:        succeeded,
							Failed:           failed,
							DeadlineExceeded: deadline,
						})
					}
				}
			}
		}
	}
	return out
}

// TestDecideIsTotal is the property that makes the engine trustworthy: for
// every state the store can hold and every observation the cluster can report,
// Decide produces a decision that is either a no-op or a legal transition. A
// reconciler that can write an illegal state is a reconciler that can strand a
// deploy.
func TestDecideIsTotal(t *testing.T) {
	states := append(States(), State("bogus"))

	for _, cur := range states {
		for _, obs := range observations() {
			d := Decide(cur, obs)

			switch d.Action {
			case Wait, Submit, Adopt, Finalize:
			default:
				t.Fatalf("Decide(%s, %+v) returned unknown action %q", cur, obs, d.Action)
			}

			if d.Next != cur && !CanTransition(cur, d.Next) {
				t.Errorf("Decide(%s, %+v) proposes illegal transition to %s", cur, obs, d.Next)
			}

			if cur.Terminal() && (d.Action != Wait || d.Next != cur) {
				t.Errorf("Decide(%s, %+v) acted on a terminal state: {%s -> %s}", cur, obs, d.Action, d.Next)
			}

			if d.Action == Submit && cur != Pending {
				t.Errorf("Decide(%s, %+v) proposes Submit from a non-pending state", cur, obs)
			}

			if d.Action == Finalize && !d.Next.Terminal() {
				t.Errorf("Decide(%s, %+v) finalizes into non-terminal %s", cur, obs, d.Next)
			}
		}
	}
}

// TestDecideIsPure guards the property the whole design leans on: no clocks, no
// counters, no hidden state between calls.
func TestDecideIsPure(t *testing.T) {
	for _, cur := range States() {
		for _, obs := range observations() {
			if Decide(cur, obs) != Decide(cur, obs) {
				t.Fatalf("Decide(%s, %+v) is not deterministic", cur, obs)
			}
		}
	}
}
