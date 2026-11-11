package eventloop

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func newLoop(t *testing.T) *Loop {
	t.Helper()
	loop, err := New()
	if err != nil {
		t.Fatal(err)
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

func TestTimeoutRunInOrder(t *testing.T) {
	loop := newLoop(t)
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
		t.Fatalf("expected 2 runs, got %d", runs)
	}
	if !slices.Equal(events, []int{1, 2}) {
		t.Fatalf("expected events to be %v, got %v", []int{1, 2}, events)
	}
}

func TestRunWaitsForTimer(t *testing.T) {
	loop := newLoop(t)
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
		t.Fatalf("expected elapsed time to be at least 20ms, got %v", elapsed)
	}
}

func TestEventLoopStopsWhenTimerReturnsError(t *testing.T) {
	loop := newLoop(t)
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
