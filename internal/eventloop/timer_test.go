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
	h1 := l.AddTimeout(timeout, func() error {
		return nil
	})
	if h1.t.timeout != timeout {
		t.Fatalf("got timeout %v, want %v", h1.t.timeout, timeout)
	}
	// 1 ms above the timeout clamps to 1 ms
	timeout2 := (1 << 31) * time.Millisecond
	h2 := l.AddTimeout(timeout2, func() error {
		return nil
	})
	if h2.t.timeout != time.Millisecond {
		t.Fatalf("got timeout %v, want %v", h2.t.timeout, time.Millisecond)
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
				t.Fatalf("got %d runs, want 0", runs)
			}
			if err := l.runDueTimers(time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if runs != 1 {
				t.Fatalf("got %d runs, want 1", runs)
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
		t.Fatalf("got events %v, want [1 2]", events)
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
		t.Fatalf("got %d runs, want 1", runs)
	}
}

func TestAddTimeoutHugeTimeoutDoesNotOverflow(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	runs := 0
	h := l.AddTimeout(math.MaxInt64, func() error {
		runs++
		return nil
	})
	if h.t.timeout != time.Millisecond {
		t.Fatalf("got timeout %v, want %v", h.t.timeout, time.Millisecond)
	}
	if err := l.runDueTimers(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("got %d runs, want 1", runs)
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
		t.Fatalf("got events %v, want %v", events, expectedEvents)
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
		t.Fatalf("got events %v, want %v", events, expectedEvents)
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
		t.Fatalf("got %d runs, want 5", runs)
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
		t.Fatalf("got events %s, want ABA", events)
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
	assertOnlyTimer := func(t *testing.T, loop *Loop, tm *timer) {
		t.Helper()
		if len(loop.timers) != 1 {
			t.Fatalf("got %d timers, want 1", len(loop.timers))
		}
		if loop.timers[0] != tm {
			t.Fatalf("got timer ID %d in the heap, want %d", loop.timers[0].id, tm.id)
		}
	}
	t.Run("nil", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		h := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(nil)
		assertOnlyTimer(t, loop, h.t)
	})
	t.Run("no *timer inside", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		h := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(&Timer{})
		assertOnlyTimer(t, loop, h.t)
	})
	t.Run("other loop Timer", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		h1 := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop2 := newLoop(t)
		h2 := loop2.AddTimeout(time.Millisecond, func() error { return nil })
		loop.CancelTimer(h2)
		assertOnlyTimer(t, loop, h1.t)
	})

	t.Run("zero Timer on empty loop", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		loop.CancelTimer(&Timer{})
		if len(loop.timers) != 0 {
			t.Fatalf("got %d timers, want 0", len(loop.timers))
		}
	})

	t.Run("other loop Timer past heap end", func(t *testing.T) {
		loop := newLoop(t)
		loop.now = func() time.Duration { return 0 }
		h := loop.AddTimeout(time.Millisecond, func() error { return nil })
		loop2 := newLoop(t)
		loop2.now = func() time.Duration { return 0 }
		loop2.CancelTimer(h)
		if len(loop2.timers) != 0 {
			t.Fatalf("got %d timers, want 0", len(loop2.timers))
		}
		assertOnlyTimer(t, loop, h.t)
		if h.t.canceled {
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
		t.Fatalf("got %d runs, want 2", runs)
	}
	if len(loop1.timers) != 1 {
		t.Fatalf("got %d timers, want 1", len(loop1.timers))
	}
	if loop1.timers[0] != intervalA.t {
		t.Fatalf("got timer ID %d in the heap, want %d (A)", loop1.timers[0].id, intervalA.ID())
	}
}

// A timer canceled before its deadline never runs, and the loop is no
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
		t.Fatalf("got error %v, want nil", err)
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
	var h *Timer
	h = l.AddInterval(time.Millisecond, func() error {
		runs++
		l.CancelTimer(h)
		offset += time.Millisecond
		return nil
	})
	if err := l.runDueTimers(1 * time.Millisecond); err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	if runs != 1 {
		t.Fatalf("got %d runs, want 1", runs)
	}
	if err := l.runDueTimers(2 * time.Millisecond); err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	if runs != 1 {
		t.Fatalf("got %d runs, want 1", runs)
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
		t.Fatalf("got error %v, want nil", err)
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
		t.Fatalf("got error %v, want nil", err)
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
	h1 := l.AddTimeout(1*time.Millisecond, func() error {
		runs++
		return nil
	})
	h2 := l.AddTimeout(2*time.Millisecond, func() error {
		runs++
		return nil
	})
	if err := l.runDueTimers(1 * time.Millisecond); err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	if runs != 1 {
		t.Fatalf("got %d runs, want 1", runs)
	}
	l.CancelTimer(h1)
	if len(l.timers) != 1 {
		t.Fatalf("got %d timers, want 1", len(l.timers))
	}
	if l.timers[0].id != h2.t.id {
		t.Fatalf("got timer ID %d in the heap, want %d (timer2)", l.timers[0].id, h2.ID())
	}
}

// Cancelling a copy of a Timer (cp := *timer) cancels the same timer, even
// after the heap has reordered since the copy was made, and leaves the other
// timers alone.
func TestCancelTimerCopyCancelsSameTimer(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	aRan := false
	bRan := false
	a := l.AddTimeout(20*time.Millisecond, func() error { aRan = true; return nil })
	cp := *a
	b := l.AddTimeout(10*time.Millisecond, func() error { bRan = true; return nil })
	l.CancelTimer(&cp)
	l.CancelTimer(a)
	if len(l.timers) != 1 {
		t.Fatalf("got %d timers, want 1", len(l.timers))
	}
	if l.timers[0] != b.t {
		t.Fatalf("got timer ID %d in the heap, want %d (b)", l.timers[0].id, b.ID())
	}
	if err := l.runDueTimers(20 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if aRan {
		t.Fatal("a ran")
	}
	if !bRan {
		t.Fatal("b didn't run")
	}
}

// IDs

// ID returns 1, 2, … in creation order for timers on one loop, starts again
// at 1 on another loop, and returns 0 for a nil or zero Timer.
func TestTimerID(t *testing.T) {
	l := newLoop(t)
	zeroTimer := &Timer{}
	if zeroTimer.ID() != 0 {
		t.Fatalf("got ID %d, want 0", zeroTimer.ID())
	}
	var nilTimer *Timer
	if nilTimer.ID() != 0 {
		t.Fatalf("got ID %d, want 0", nilTimer.ID())
	}
	tm1 := l.AddTimeout(time.Millisecond, func() error { return nil })
	if tm1.ID() != 1 {
		t.Fatalf("got ID %d, want 1", tm1.ID())
	}
	tm2 := l.AddTimeout(time.Millisecond, func() error { return nil })
	if tm2.ID() != 2 {
		t.Fatalf("got ID %d, want 2", tm2.ID())
	}
	l2 := newLoop(t)
	tm3 := l2.AddTimeout(time.Millisecond, func() error { return nil })
	if tm3.ID() != 1 {
		t.Fatalf("got ID %d, want 1", tm3.ID())
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
		t.Fatalf("got error %v, want nil", err)
	}
	if err := <-done2; err != nil {
		t.Fatalf("got error %v, want nil", err)
	}
	if !slices.Equal(l1Events, []int{1, 2, 3}) {
		t.Fatalf("got l1Events %v, want [1 2 3]", l1Events)
	}
	if !slices.Equal(l2Events, []int{4, 5, 6}) {
		t.Fatalf("got l2Events %v, want [4 5 6]", l2Events)
	}
}

// Nil callbacks

// AddTimeout and AddInterval panic right away on a nil callback, at the
// caller's line, instead of later when the timer fires. Nothing is added to
// the heap and no ID is used up.
func TestAddTimerPanicsOnNilCallback(t *testing.T) {
	tests := []struct {
		name string
		add  func(l *Loop) *Timer
	}{
		{"timeout", func(l *Loop) *Timer { return l.AddTimeout(time.Millisecond, nil) }},
		{"interval", func(l *Loop) *Timer { return l.AddInterval(time.Millisecond, nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLoop(t)
			l.now = func() time.Duration { return 0 }
			func() {
				defer func() {
					if r := recover(); r == nil {
						t.Fatal("got no panic, want a panic")
					}
				}()
				tt.add(l)
			}()
			if len(l.timers) != 0 {
				t.Fatalf("got %d timers, want 0", len(l.timers))
			}
			h := l.AddTimeout(time.Millisecond, func() error { return nil })
			if h.ID() != 1 {
				t.Fatalf("got ID %d, want 1", h.ID())
			}
		})
	}
}
