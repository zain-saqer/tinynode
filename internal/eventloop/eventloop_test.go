package eventloop

import (
	"errors"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newTestLoop(t *testing.T) *Loop {
	t.Helper()
	l, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	return l
}

// runWithTimeout fails the test instead of hanging it when Run never returns.
func runWithTimeout(t *testing.T, l *Loop) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- l.Run() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// newPipe returns both ends of a pipe and closes them when the test ends.
func newPipe(t *testing.T) (r, w int) {
	t.Helper()
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unix.Close(fds[0])
		unix.Close(fds[1])
	})
	return fds[0], fds[1]
}

// runDueTimers takes "now" as an argument, so these tests use fake time and never sleep.

func TestRunDueTimersRunsDueTimersInOrder(t *testing.T) {
	l := newTestLoop(t)
	var got []int
	add := func(id int, at time.Duration) {
		l.AddTimer(&Timer{ID: id, RunAt: at, Callback: func() error {
			got = append(got, id)
			return nil
		}})
	}
	add(3, 30*time.Millisecond)
	add(1, 10*time.Millisecond)
	add(2, 20*time.Millisecond)

	if err := l.runDueTimers(25 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []int{1, 2}) {
		t.Errorf("ran %v, want [1 2]", got)
	}
	if l.timers.Len() != 1 || l.timers[0].ID != 3 {
		t.Errorf("timer 3 should still be pending")
	}
}

func TestRunDueTimersReschedulesRepeatingTimer(t *testing.T) {
	l := newTestLoop(t)
	runs := 0
	timer := &Timer{RunAt: 10 * time.Millisecond, Timeout: 10 * time.Millisecond, Repeat: true,
		Callback: func() error { runs++; return nil }}
	l.AddTimer(timer)

	if err := l.runDueTimers(35 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if runs != 3 { // at 10, 20 and 30
		t.Errorf("runs = %d, want 3", runs)
	}
	if timer.RunAt != 40*time.Millisecond {
		t.Errorf("RunAt = %v, want 40ms", timer.RunAt)
	}
}

func TestRunDueTimersStopsAtFirstError(t *testing.T) {
	l := newTestLoop(t)
	boom := errors.New("boom")
	secondRan := false
	l.AddTimer(&Timer{RunAt: 1, Callback: func() error { return boom }})
	l.AddTimer(&Timer{RunAt: 2, Callback: func() error { secondRan = true; return nil }})

	if err := l.runDueTimers(10); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
	if secondRan {
		t.Error("second timer ran after the first one failed")
	}
}

// The tests below drive the real Run with real fds and short real timeouts.

func TestRunReturnsWhenNothingIsLeft(t *testing.T) {
	if err := runWithTimeout(t, newTestLoop(t)); err != nil {
		t.Fatal(err)
	}
}

func TestRunWaitsForTimer(t *testing.T) {
	l := newTestLoop(t)
	fired := false
	start := Now()
	l.AddTimer(&Timer{RunAt: start + 20*time.Millisecond, Callback: func() error {
		fired = true
		return nil
	}})

	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("timer did not fire")
	}
	if elapsed := Now() - start; elapsed < 20*time.Millisecond {
		t.Errorf("timer fired after %v, want at least 20ms", elapsed)
	}
}

func TestRunDispatchesReadableFd(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)

	var got []byte
	err := l.Register(r, Readable, func(fd int, interest Interest) error {
		buf := make([]byte, 16)
		n, _ := unix.Read(fd, buf)
		got = buf[:n]
		l.Unregister(fd) // nothing left, so Run returns
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	unix.Write(w, []byte("hi"))

	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Errorf("read %q, want %q", got, "hi")
	}
}

func TestRunReturnsCallbackError(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	boom := errors.New("boom")
	l.Register(r, Readable, func(int, Interest) error { return boom })
	unix.Write(w, []byte("x"))

	if err := runWithTimeout(t, l); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestQueueTaskWakesTheLoop(t *testing.T) {
	l := newTestLoop(t)
	r, _ := newPipe(t)
	// Keeps the loop alive and blocked in wait with no timeout; nothing is ever written.
	l.Register(r, Readable, func(int, Interest) error { return nil })

	ran := false
	go func() {
		time.Sleep(10 * time.Millisecond)
		l.QueueTask(func() {
			ran = true
			l.Unregister(r)
		})
	}()

	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Error("queued task did not run")
	}
}
