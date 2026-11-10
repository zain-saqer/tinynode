package eventloop

import (
	"slices"
	"testing"
	"time"
)

func newLoop(t *testing.T) *Loop {
	t.Helper()
	loop, err := New()
	if err != nil {
		t.Fatal("expected nil error")
	}
	return loop
}

func runWithTimeout(t *testing.T, loop *Loop) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- loop.Run()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

func TestRunDueTimersRunsDueTimersInOrder(t *testing.T) {
	l := newLoop(t)
	var events []int
	add := func(id int, at time.Duration) {
		l.AddTimer(&Timer{
			ID:    id,
			RunAt: l.Now() + at,
			Callback: func() error {
				events = append(events, id)
				return nil
			},
		})
	}
	add(1, 0*time.Millisecond)
	add(2, 10*time.Millisecond)
	add(3, 5*time.Millisecond)
	add(4, 0*time.Millisecond)

	if err := l.runDueTimers(5 * time.Millisecond); err != nil {
		t.Fatal("expected nil error")
	}
	expectedEvents := []int{1, 4, 3}
	if slices.Equal(events, expectedEvents) {
		t.Fatalf("expected events %v, got %v", expectedEvents, events)
	}
}

func TestRunDueTimersReschedulesRepeatingTimer(t *testing.T) {

}

func TestTimeoutRunInOrder(t *testing.T) {
	loop := newLoop(t)
	var offset time.Duration
	loop.now = func() time.Duration {
		return offset
	}
	runs := 0
	events := ""
	loop.AddTimer(&Timer{
		ID:      1,
		Timeout: 0,
		RunAt:   0,
		Repeat:  false,
		Callback: func() error {
			runs++
			events += "1"
			offset += time.Millisecond
			return nil
		},
	})
	loop.AddTimer(&Timer{
		ID:      2,
		Timeout: time.Millisecond,
		RunAt:   time.Millisecond,
		Repeat:  false,
		Callback: func() error {
			runs++
			events += "2"
			return nil
		},
	})
	err := runWithTimeout(t, loop)
	if err != nil {
		t.Fatal("expected nil error")
	}
	if runs != 2 {
		t.Fatalf("expected 1 runs, got %d", runs)
	}
	if events != "12" {
		t.Fatalf("expected events to be 12, got %s", events)
	}
}

func TestRunWaitsForTimer(t *testing.T) {
	loop := newLoop(t)
	fired := false
	start := loop.Now()
	loop.AddTimer(&Timer{
		ID:      1,
		Timeout: 20 * time.Millisecond,
		RunAt:   start + 20*time.Millisecond,
		Repeat:  false,
		Callback: func() error {
			fired = true
			return nil
		},
	})
	err := runWithTimeout(t, loop)
	if err != nil {
		t.Fatal("expected nil error")
	}
	if !fired {
		t.Fatal("expected timer to fire")
	}
	if elapsed := loop.Now() - start; elapsed < 20*time.Millisecond {
		t.Fatalf("expected elapsed time to be at least 20ms, got %d", elapsed)
	}
}
