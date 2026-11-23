package eventloop

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func newTestLoop(t *testing.T) *Loop {
	t.Helper()
	loop, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := loop.Close(); err != nil {
			t.Error(err)
		}
	})
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

func TestTimeoutRunInOrder(t *testing.T) {
	loop := newTestLoop(t)
	offset := time.Millisecond
	loop.now = func() time.Duration {
		return offset
	}
	runs := 0
	events := make([]int, 0)
	loop.AddTimeout(time.Millisecond,
		func() error {
			runs++
			events = append(events, 1)
			offset += 25 * time.Millisecond
			return nil
		})
	loop.AddTimeout(10*time.Millisecond,
		func() error {
			runs++
			events = append(events, 2)
			return nil
		})
	offset = 5 * time.Millisecond
	err := runWithTimeout(t, loop)
	if err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("got %d runs, want 2", runs)
	}
	if !slices.Equal(events, []int{1, 2}) {
		t.Fatalf("got events %v, want %v", events, []int{1, 2})
	}
}

func TestRunWaitsForTimer(t *testing.T) {
	loop := newTestLoop(t)
	fired := false
	start := loop.Now()
	loop.AddTimeout(20*time.Millisecond,
		func() error {
			fired = true
			return nil
		})
	err := runWithTimeout(t, loop)
	if err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("expected timer to fire")
	}
	if elapsed := loop.Now() - start; elapsed < 20*time.Millisecond {
		t.Fatalf("got elapsed %v, want at least 20ms", elapsed)
	}
}

func TestEventLoopStopsWhenTimerReturnsError(t *testing.T) {
	loop := newTestLoop(t)
	offset := time.Duration(0)
	loop.now = func() time.Duration {
		return offset
	}
	expectedError := errors.New("timer error")
	loop.AddTimeout(time.Millisecond, func() error {
		return expectedError
	})
	offset = 3 * time.Millisecond
	if err := runWithTimeout(t, loop); !errors.Is(err, expectedError) {
		t.Fatalf("got %v, want %v", err, expectedError)
	}
	if err := runWithTimeout(t, loop); !errors.Is(err, expectedError) {
		t.Fatalf("got %v, want %v", err, expectedError)
	}
}

// Run returns right away on a loop with no timers.
func TestRunReturnsOnEmptyLoop(t *testing.T) {
	loop := newTestLoop(t)
	err := runWithTimeout(t, loop)
	if err != nil {
		t.Fatal(err)
	}
}

// Run on a closed loop with a pending timer must fail with ErrClosed instead
// of blocking or touching a reused fd.
func TestRunAfterCloseReturnsErrClosed(t *testing.T) {
	l := newTestLoop(t)
	l.now = func() time.Duration {
		return 0
	}
	l.AddTimeout(time.Millisecond, func() error { return nil })
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want %v", err, ErrClosed)
	}
}

// Calling Close twice on a Loop returns nil both times.
func TestLoopCloseTwiceReturnsNil(t *testing.T) {
	l := newTestLoop(t)
	if err := l.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// Run on a closed loop with no timers, tasks or holds must return ErrClosed,
// not nil. Before the closed check, isAlive was false, so Run returned nil
// without ever reaching the poller.
func TestRunOnClosedEmptyLoopReturnsErrClosed(t *testing.T) {
	l := newTestLoop(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); !errors.Is(err, ErrClosed) {
		t.Fatalf("runWithTimeout() = %v, want %v", err, ErrClosed)
	}
}

// Close drops the timers that have not fired: the heap is empty afterwards,
// so the callbacks (and anything they captured) can be garbage collected.
func TestCloseDropsPendingTimers(t *testing.T) {
	l := newTestLoop(t)
	l.AddTimeout(time.Millisecond, func() error { return nil })
	l.AddInterval(time.Millisecond, func() error { return nil })
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if len(l.timers) != 0 {
		t.Fatalf("len(l.timers) = %d, want 0", len(l.timers))
	}
}

// Run called from a Callback panics with panicLoopRunning instead of starting
// a second loop over the same timer and task batches.
func TestRunFromCallbackPanics(t *testing.T) {
	l := newTestLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration { return offset }
	l.AddTimeout(time.Millisecond, func() error { return l.Run() })
	offset += time.Millisecond
	if r := recoverPanic(func() {
		if err := l.Run(); err != nil {
			t.Fatal(err)
		}
	}); r != panicLoopRunning {
		t.Fatalf("panic value = %v, want %v", r, panicLoopRunning)
	}
	if l.running {
		t.Errorf("l.running = %v, want false", l.running)
	}
}

// Close called from a Callback returns ErrRunning and does nothing: the loop
// keeps its timers and its poller, so a later timer still fires.
func TestCloseFromCallbackReturnsErrRunning(t *testing.T) {
	l := newTestLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration { return offset }
	l.AddTimeout(time.Millisecond, func() error {
		offset += time.Millisecond
		if err := l.Close(); !errors.Is(err, ErrRunning) {
			t.Errorf("l.Close() = %v, want %v", err, ErrRunning)
		}
		return nil
	})
	runs := 0
	l.AddTimeout(2*time.Millisecond, func() error { runs++; return nil })
	offset += time.Millisecond
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Errorf("runs = %d, want 1", runs)
	}
}

// The running flag is cleared when Run returns, on every path, so Close works
// afterwards. The panic path is covered by TestRunFromCallbackPanics.
func TestRunClearsRunningOnReturn(t *testing.T) {
	errCallback := errors.New("callback error")
	tests := []struct {
		name    string
		setup   func(t *testing.T, l *Loop)
		wantErr error
	}{
		{
			name:  "normal return",
			setup: func(t *testing.T, l *Loop) {},
		},
		{
			name: "timer callback error",
			setup: func(t *testing.T, l *Loop) {
				l.AddTimeout(time.Millisecond, func() error { return errCallback })
			},
			wantErr: errCallback,
		},
		{
			name: "posted callback error",
			setup: func(t *testing.T, l *Loop) {
				if err := l.Post(func() error { return errCallback }); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: errCallback,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newTestLoop(t)
			offset := time.Duration(0)
			l.now = func() time.Duration { return offset }
			tt.setup(t, l)
			offset += time.Millisecond
			if err := runWithTimeout(t, l); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() = %v, want %v", err, tt.wantErr)
			}
			if err := l.Close(); err != nil {
				t.Errorf("Close() = %v, want nil", err)
			}
		})
	}
}
