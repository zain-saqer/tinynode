package eventloop

import (
	"errors"
	"math"
	"slices"
	"testing"
	"time"
)

// Clamping

func TestAddTimeoutMaxBoundary(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	// max timeout
	timeout := (1<<31 - 1) * time.Millisecond
	timer := l.AddTimeout(timeout, func() error {
		return nil
	})
	if timer.timeout != timeout {
		t.Fatalf("expected timeout %v, got %v", timeout, timer.timeout)
	}
	// 1 ms above the timeout clamps to 1 ms
	timeout2 := (1 << 31) * time.Millisecond
	timer2 := l.AddTimeout(timeout2, func() error {
		return nil
	})
	if timer2.timeout != time.Millisecond {
		t.Fatalf("expected timeout %v, got %v", time.Millisecond, timer2.timeout)
	}
}

func TestAddTimeoutClampsNonPositiveTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{"zero", 0},
		{"negative 1ns", -1},
		{"negative 5ms", -5 * time.Millisecond},
		{"min int64", math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLoop(t)
			l.now = func() time.Duration {
				return 0
			}
			runs := 0
			l.AddTimeout(tt.timeout, func() error {
				runs++
				return nil
			})
			if err := l.runDueTimers(time.Duration(0)); err != nil {
				t.Fatal(err)
			}
			if runs != 0 {
				t.Fatalf("expected 0 runs, got %d", runs)
			}
			if err := l.runDueTimers(time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if runs != 1 {
				t.Fatalf("expected 1 runs, got %d", runs)
			}
		})
	}
}

// setTimeout(a, 0) then setTimeout(b, -5): both clamp to 1ms, so a runs first.
func TestClampedTimeoutsRunInInsertionOrder(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	events := make([]int, 0)
	l.AddTimeout(0, func() error {
		events = append(events, 1)
		return nil
	})
	l.AddTimeout(-5*time.Millisecond, func() error {
		events = append(events, 2)
		return nil
	})
	if err := l.runDueTimers(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(events, []int{1, 2}) {
		t.Fatalf("expected events to be [1, 2], got %v", events)
	}
}

func runDueTimersWithTimeout(t *testing.T, l *Loop, now time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- l.runDueTimers(now)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("runDueTimers did not return")
		return nil
	}
}

// A 0ms interval with a frozen clock fires once per runDueTimers pass
// instead of looping forever.
func TestAddIntervalClampsZeroTimeout(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	runs := 0
	l.AddInterval(0, func() error {
		runs++
		return nil
	})
	if err := runDueTimersWithTimeout(t, l, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("expected 1 runs, got %d", runs)
	}
}

func TestAddTimeoutHugeTimeoutDoesNotOverflow(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	runs := 0
	timeout := l.AddTimeout(math.MaxInt64, func() error {
		runs++
		return nil
	})
	if timeout.timeout != time.Millisecond {
		t.Fatalf("expected timeout to be %v, got %v", time.Millisecond, timeout.timeout)
	}
	if err := l.runDueTimers(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("expected 1 runs, got %d", runs)
	}
}

// Ordering

func TestRunDueTimersRunsDueTimersInOrder(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	var events []int
	add := func(id int, timeout time.Duration) {
		l.AddTimeout(timeout, func() error {
			events = append(events, id)
			return nil
		})
	}
	add(1, 0*time.Millisecond)
	add(2, 10*time.Millisecond)
	add(3, 5*time.Millisecond)
	add(4, 0*time.Millisecond)

	if err := l.runDueTimers(5 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	expectedEvents := []int{1, 4, 3}
	if !slices.Equal(events, expectedEvents) {
		t.Fatalf("expected events %v, got %v", expectedEvents, events)
	}
}

// Several timers with the same deadline run in the order they were added.
func TestEqualDeadlinesRunInInsertionOrder(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	var events []int
	add := func(id int, timeout time.Duration) {
		l.AddTimeout(timeout, func() error {
			events = append(events, id)
			return nil
		})
	}
	add(1, 2*time.Millisecond)
	add(2, 1*time.Millisecond)
	add(3, 1*time.Millisecond)
	add(4, 2*time.Millisecond)

	if err := l.runDueTimers(5 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	expectedEvents := []int{2, 3, 1, 4}
	if !slices.Equal(events, expectedEvents) {
		t.Fatalf("expected events %v, got %v", expectedEvents, events)
	}
}

// Repeating

func TestRunDueTimersReschedulesRepeatingTimer(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	runs := 0
	l.now = func() time.Duration {
		return offset
	}
	l.AddInterval(time.Millisecond, func() error {
		runs++
		offset += time.Millisecond
		return nil
	})
	for range 5 {
		if err := l.runDueTimers(5 * time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 5 {
		t.Fatalf("expected 5 runs, got %d", runs)
	}
}

func TestRescheduledIntervalRunsAfterTimersWithSameDeadline(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	events := ""
	l.AddInterval(time.Millisecond, func() error {
		events += "A"
		return nil
	})
	l.AddTimeout(2*time.Millisecond, func() error {
		events += "B"
		return nil
	})
	offset = time.Millisecond
	if err := l.runDueTimers(offset); err != nil {
		t.Fatal(err)
	}
	offset = 2 * time.Millisecond
	if err := l.runDueTimers(offset); err != nil {
		t.Fatal(err)
	}
	// A is re-inserted at 2ms after B, so B runs first.
	if events != "ABA" {
		t.Fatalf("expected events to be ABA, got %s", events)
	}
}

// Errors

func TestRunDueTimersStopsWhenTimerReturnsError(t *testing.T) {
	loop := newLoop(t)
	expectedError := errors.New("timer error")
	loop.now = func() time.Duration {
		return 0
	}
	loop.AddTimeout(time.Millisecond, func() error {
		return expectedError
	})
	secondTimerRan := false
	loop.AddTimeout(2*time.Millisecond, func() error {
		secondTimerRan = true
		return nil
	})
	if err := loop.runDueTimers(3 * time.Millisecond); !errors.Is(err, expectedError) {
		t.Fatalf("got %v, want %v", err, expectedError)
	}
	if secondTimerRan {
		t.Fatal("expected second timer to not run")
	}
}

// Cancelling

// CancelTimer ignores timers this loop didn't create (nil, a zero Timer, or a
// timer from another loop), even when the timer's index doesn't fit this
// loop's heap. Nothing is removed and nothing panics.
func TestCancelTimerNoOpOnInvalidTimers(t *testing.T) {
	assertOnlyTimer := func(t *testing.T, loop *Loop, timer *Timer) {
		t.Helper()
		if len(loop.timers) != 1 {
			t.Fatalf("expected length of timers to be 1, got %d", len(loop.timers))
		}
		if loop.timers[0] != timer {
			t.Fatalf("expected existing timer to still be in timers, got %v", loop.timers)
		}
	}
	t.Run("nil", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		timer1 := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(nil)
		assertOnlyTimer(t, loop, timer1)
	})
	t.Run("zero timer", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		timer1 := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(&Timer{})
		assertOnlyTimer(t, loop, timer1)
	})
	t.Run("other loop timer", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		timer1 := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop2 := newLoop(t)
		timer2 := loop2.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(timer2)
		assertOnlyTimer(t, loop, timer1)
	})

	t.Run("zero timer on empty loop", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		loop.CancelTimer(&Timer{})
		if len(loop.timers) != 0 {
			t.Fatalf("expected length of timers to be 0, got %d", len(loop.timers))
		}
	})

	t.Run("other loop timer past heap end", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		timer1 := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop2 := newLoop(t)
		loop2.now = func() time.Duration { return 0 }
		loop2.CancelTimer(timer1)
		if len(loop2.timers) != 0 {
			t.Fatalf("expected length of timers to be 0, got %d", len(loop2.timers))
		}
		assertOnlyTimer(t, loop, timer1)
		if timer1.canceled {
			t.Fatalf("expected timer not to be canceled")
		}
	})
}

// Cancelling a timer through the wrong loop doesn't mark it canceled. Timer B
// cancels interval A through loop2 while A is popped and waiting in loop1's
// batch, and A still runs and is rescheduled.
func TestCancelTimerDoesNotChangeOtherLoopTimers(t *testing.T) {
	loop1 := newLoop(t)
	loop1.now = func() time.Duration { return 0 }
	loop2 := newLoop(t)
	loop2.now = func() time.Duration { return 0 }
	runs := 0
	var intervalA *Timer
	loop1.AddTimeout(time.Millisecond, func() error {
		runs++
		loop2.CancelTimer(intervalA)
		return nil
	})
	intervalA = loop1.AddInterval(time.Millisecond, func() error {
		runs++
		return nil
	})
	if err := loop1.runDueTimers(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("expected 2 callbacks, got %d", runs)
	}
	if len(loop1.timers) != 1 {
		t.Fatalf("expected length of timers to be 1, got %d", len(loop1.timers))
	}
	if loop1.timers[0] != intervalA {
		t.Fatalf("expected existing timer to still be in timers, got %v", loop1.timers)
	}
}

// A timer cancelled before its deadline never runs, and the loop is no
// longer alive because of it.
func TestCancelTimerBeforeItFires(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	runs := 0
	timer := l.AddTimeout(time.Millisecond, func() error {
		runs++
		return nil
	})
	l.CancelTimer(timer)
	if err := l.runDueTimers(2 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 0 {
		t.Fatal("expected timer to not run")
	}
	if l.isAlive() {
		t.Fatal("expected loop to not be alive")
	}
}

// An interval that cancels itself inside its callback is not rescheduled.
func TestCancelTimerFromOwnCallback(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	runs := 0
	var timer *Timer
	timer = l.AddInterval(time.Millisecond, func() error {
		runs++
		l.CancelTimer(timer)
		offset += time.Millisecond
		return nil
	})
	if err := l.runDueTimers(1 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 1 {
		t.Fatalf("expected timer to run once, but it ran %d times", runs)
	}
	if err := l.runDueTimers(2 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 1 {
		t.Fatalf("expected timer to run once, but it ran %d times", runs)
	}
}

// A timer that cancels another due timer in the same pass stops it from
// running.
func TestCancelTimerFromAnotherTimersCallback(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	runs := 0
	timer := l.AddTimeout(2*time.Millisecond, func() error {
		runs++
		return nil
	})
	l.AddTimeout(time.Millisecond, func() error {
		l.CancelTimer(timer)
		return nil
	})
	if err := l.runDueTimers(3 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 0 {
		t.Fatalf("expected canceled timer to not run")
	}
}

// Cancelling the same timer twice is a no-op the second time.
func TestCancelTimerTwice(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	runs := 0
	timer := l.AddTimeout(2*time.Millisecond, func() error {
		runs++
		return nil
	})
	l.CancelTimer(timer)
	l.CancelTimer(timer)
	if err := l.runDueTimers(3 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 0 {
		t.Fatalf("expected canceled timer to not run")
	}
}

// Cancelling a timeout that has already fired is a no-op and leaves the
// other timers in the heap alone.
func TestCancelTimerAfterItFired(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration {
		return offset
	}
	runs := 0
	timer := l.AddTimeout(1*time.Millisecond, func() error {
		runs++
		return nil
	})
	timer2 := l.AddTimeout(2*time.Millisecond, func() error {
		runs++
		return nil
	})
	if err := l.runDueTimers(1 * time.Millisecond); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if runs != 1 {
		t.Fatalf("expected runs to be 1, got %d", runs)
	}
	l.CancelTimer(timer)
	if len(l.timers) != 1 {
		t.Fatalf("expected one timer left in the heap")
	}
	if l.timers[0].ID != timer2.ID {
		t.Fatalf("expected timer2 to be the only timer left in the heap")
	}
}

// Per-loop state

// Two loops running on two goroutines don't share timer counters
// (run with -race).
func TestLoopsRunConcurrently(t *testing.T) {
	l1 := newLoop(t)
	l1.now = func() time.Duration {
		return 0
	}
	l2 := newLoop(t)
	l2.now = func() time.Duration {
		return 0
	}
	add := func(l *Loop, event int, timeout time.Duration, events *[]int) {
		l.AddTimeout(timeout, func() error {
			*events = append(*events, event)
			return nil
		})
	}
	l1Events := make([]int, 0)
	l2Events := make([]int, 0)
	done := make(chan error, 1)
	done2 := make(chan error, 1)
	go func() {
		add(l1, 1, time.Millisecond, &l1Events)
		add(l1, 2, time.Millisecond, &l1Events)
		add(l1, 3, time.Millisecond, &l1Events)
		done <- l1.runDueTimers(1 * time.Millisecond)
	}()
	go func() {
		add(l2, 4, time.Millisecond, &l2Events)
		add(l2, 5, time.Millisecond, &l2Events)
		add(l2, 6, time.Millisecond, &l2Events)
		done2 <- l2.runDueTimers(1 * time.Millisecond)
	}()
	if err := <-done; err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !slices.Equal(l1Events, []int{1, 2, 3}) {
		t.Fatalf("expected l1Events to be [1, 2, 3], got %v", l1Events)
	}
	if !slices.Equal(l2Events, []int{4, 5, 6}) {
		t.Fatalf("expected l2Events to be [4, 5, 6], got %v", l2Events)
	}
}
