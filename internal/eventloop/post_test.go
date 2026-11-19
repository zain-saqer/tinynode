package eventloop

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// runConcurrently calls f(i) for i in [0, n) on n goroutines, waits for all of
// them, and returns their errors joined.
func runConcurrently(n int, f func(i int) error) error {
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { errs[i] = f(i) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// recoverPanic calls f and returns the value it panicked with, or nil if it
// returned normally.
func recoverPanic(f func()) (r any) {
	defer func() { r = recover() }()
	f()
	return nil
}

// With no timers and one hold, Run blocks. A worker goroutine posts via the
// hold after ~50 ms, the task runs, and Run returns.
func TestHoldBlocksTheLoopThenUnhold(t *testing.T) {
	l := newLoop(t)
	runs := 0
	result := make(chan error, 1)
	p := l.Hold()
	go func() {
		time.Sleep(50 * time.Millisecond)
		result <- p.Done(func() error {
			runs++
			return nil
		})
	}()
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	err := <-result
	if err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("callback ran %d times, want 1", runs)
	}
}

// Releasing a hold twice must not make the hold count negative: a second,
// still active hold must keep the loop alive.
func TestDoneTwiceDoesNotReleaseOtherHolds(t *testing.T) {
	l := newLoop(t)
	h1 := l.Hold()
	h2 := l.Hold()
	firstH1DoneRuns := 0
	secondH1DoneRuns := 0
	if err := h1.Done(func() error { firstH1DoneRuns++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := h1.Done(func() error { secondH1DoneRuns++; return nil }); !errors.Is(err, ErrAlreadyDone) {
		t.Fatalf("second h1.Done() = %v, want %v", err, ErrAlreadyDone)
	}
	h2Runs := 0
	done := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		done <- h2.Done(func() error {
			h2Runs++
			return nil
		})
	}()
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("h2.Done() = %v, want nil", err)
	}
	if firstH1DoneRuns != 1 {
		t.Fatalf("first h1 callback ran %d times, want 1", firstH1DoneRuns)
	}
	if secondH1DoneRuns != 0 {
		t.Fatalf("second h1 callback ran %d times, want 0", secondH1DoneRuns)
	}
	if h2Runs != 1 {
		t.Fatalf("h2 callback ran %d times, want 1", h2Runs)
	}
}

// Callbacks posted before Run keep the loop alive and run, even with no
// timers and no holds.
func TestPostBeforeRunRunsCallback(t *testing.T) {
	l := newLoop(t)
	runs := 0
	cb := func() error {
		runs++
		return nil
	}
	if err := l.Post(cb); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("callback ran %d times, want 1", runs)
	}
}

// Posted callbacks run in the order they were posted, and all of them run
// (not only the first of the batch).
func TestPostRunsCallbacksInOrder(t *testing.T) {
	l := newLoop(t)
	var events []int
	if err := l.Post(func() error { events = append(events, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Post(func() error { events = append(events, 2); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Post(func() error { events = append(events, 3); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(events, []int{1, 2, 3}) {
		t.Fatalf("callbacks ran in order %v, want [1 2 3]", events)
	}
}

// Post from another goroutine must wake a Run that is blocked waiting for a
// far-away timer.
func TestPostWakesBlockedRun(t *testing.T) {
	l := newLoop(t)
	l.now = func() time.Duration { return 0 }
	timerRuns := 0
	tmr := l.AddTimeout(5*time.Second, func() error { timerRuns++; return nil })
	done := make(chan error, 1)
	callbackRuns := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		done <- l.Post(func() error {
			callbackRuns++
			l.CancelTimer(tmr)
			return nil
		})
	}()
	start := time.Now()
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Fatalf("Run took %v, want < 1s", elapsed)
	}
	cbErr := <-done
	if cbErr != nil {
		t.Fatal(cbErr)
	}
	if timerRuns != 0 {
		t.Fatalf("timer ran %d times, want 0", timerRuns)
	}
	if callbackRuns != 1 {
		t.Fatalf("callback ran %d times, want 1", callbackRuns)
	}
}

// Many goroutines posting at the same time: every callback runs exactly once.
// Run with -race.
func TestConcurrentPostsAllRunOnce(t *testing.T) {
	l := newLoop(t)
	n := 50
	m := 20
	runs := make([]int, n*m)
	p := l.Hold()
	doneResult := make(chan error, 1)
	go func() {
		postErr := runConcurrently(n, func(i int) error {
			time.Sleep(50 * time.Millisecond)
			for j := range m {
				cb := func() error {
					runs[i*m+j]++
					return nil
				}
				if err := l.Post(cb); err != nil {
					return err
				}
			}
			return nil
		})
		// Release the hold even if a Post failed, so Run returns and the
		// test reports the Post error instead of a timeout.
		doneErr := p.Done(func() error { return nil })
		doneResult <- errors.Join(postErr, doneErr)
	}()
	t.Cleanup(func() {
		if err := <-doneResult; err != nil {
			t.Error(err)
		}
	})
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	for i, c := range runs {
		if c != 1 {
			t.Errorf("callback %d ran %d times, want 1", i, c)
		}
	}
}

// A callback posted from inside a callback runs in the next iteration, not in
// the current batch, so a callback that keeps re-posting itself cannot block
// timers.
func TestRepostingCallbackDoesNotStarveTimers(t *testing.T) {
	l := newLoop(t)
	offset := time.Duration(0)
	l.now = func() time.Duration { return offset }
	timeoutRuns := 0
	l.AddTimeout(10*time.Millisecond, func() error {
		timeoutRuns++
		return nil
	})
	cbRuns := 0
	var cb Callback
	cb = func() error {
		cbRuns++
		offset += time.Millisecond
		if cbRuns > 1000 {
			return errors.New("starved")
		}
		if timeoutRuns == 0 {
			return l.Post(cb)
		}
		return nil
	}
	if err := l.Post(cb); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if timeoutRuns != 1 {
		t.Fatalf("timer ran %d times, want 1", timeoutRuns)
	}
}

// An error from a posted callback stops Run and is returned unchanged
// (errors.Is and the message match, with no "eventloop" prefix).
func TestPostedCallbackErrorStopsRun(t *testing.T) {
	l := newLoop(t)
	sentinel := errors.New("sentinel error")
	if err := l.Post(func() error { return sentinel }); err != nil {
		t.Fatal(err)
	}
	secondCbRuns := 0
	if err := l.Post(func() error { secondCbRuns++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != sentinel {
		t.Fatalf("Run() = %v, want %v", err, sentinel)
	}
	if secondCbRuns != 0 {
		t.Fatalf("second callback ran %d times, want 0", secondCbRuns)
	}

	if err := runWithTimeout(t, l); err != sentinel {
		t.Fatalf("Run() = %v, want %v", err, sentinel)
	}
}

// The error from a callback passed to Pending.Done also stops Run unchanged,
// and the hold is still released.
func TestDoneCallbackErrorStopsRun(t *testing.T) {
	l := newLoop(t)
	sentinelErr := errors.New("sentinel error")
	p := l.Hold()
	result := make(chan error, 1)
	go func() {
		result <- p.Done(func() error { return sentinelErr })
	}()
	if err := runWithTimeout(t, l); err != sentinelErr {
		t.Fatalf("Run() = %v, want %v", err, sentinelErr)
	}
	err := <-result
	if err != nil {
		t.Fatal(err)
	}
	if l.holds != 0 {
		t.Fatalf("l.holds = %d, want 0", l.holds)
	}
}

// Post after Close returns ErrClosed and the callback never runs.
func TestPostAfterCloseReturnsErrClosed(t *testing.T) {
	l := newLoop(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	runs := 0
	if err := l.Post(func() error {
		runs++
		return nil
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Post() = %v, want %v", err, ErrClosed)
	}
	if runs != 0 {
		t.Fatalf("callback ran %d times, want 0", runs)
	}
}

// Done after Close returns an error wrapping ErrClosed.
func TestDoneAfterCloseReturnsErrClosed(t *testing.T) {
	l := newLoop(t)
	p := l.Hold()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Done(func() error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Done() = %v, want %v", err, ErrClosed)
	}
}

// Close and Post racing from different goroutines must not race on the queue
// or call poller.wake after poller.close. Run with -race.
func TestCloseConcurrentWithPost(t *testing.T) {
	l := newLoop(t)
	n := 10
	postResult := make(chan error, 1)
	go func() {
		postResult <- runConcurrently(n, func(int) error {
			for {
				if err := l.Post(func() error { return nil }); err != nil {
					if !errors.Is(err, ErrClosed) {
						return err
					}
					return nil
				}
			}
		})
	}()
	time.Sleep(10 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-postResult; err != nil {
		t.Fatal(err)
	}
}

// Copies of a Pending share one hold: Done on a copy releases it, and Done on
// the original afterwards fails.
func TestPendingCopiesShareHold(t *testing.T) {
	l := newLoop(t)
	firstDoneRuns := 0
	secondDoneRuns := 0
	p := l.Hold()
	cp := p
	if err := cp.Done(func() error { firstDoneRuns++; return nil }); err != nil {
		t.Error(err)
	}
	if err := p.Done(func() error { secondDoneRuns++; return nil }); !errors.Is(err, ErrAlreadyDone) {
		t.Errorf("p.Done() = %v, want %v", err, ErrAlreadyDone)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if firstDoneRuns != 1 {
		t.Errorf("copy callback ran %d times, want 1", firstDoneRuns)
	}
	if secondDoneRuns != 0 {
		t.Errorf("original callback ran %d times, want 0", secondDoneRuns)
	}
}

// Calling Post with a nil callback panics.
func TestPostPanicsWithNilCallback(t *testing.T) {
	l := newLoop(t)
	r := recoverPanic(func() {
		if err := l.Post(nil); err != nil {
			t.Error(err)
		}
	})
	if r != panicNilCallback {
		t.Errorf("panic = %v, want %q", r, panicNilCallback)
	}
}

// Done with a nil callback panics in Done, on the caller's goroutine, instead
// of later on the loop goroutine when the posted wrapper calls it.
func TestDonePanicsWithNilCallback(t *testing.T) {
	l := newLoop(t)
	p := l.Hold()
	r := recoverPanic(func() {
		if err := p.Done(nil); err != nil {
			t.Error(err)
		}
	})
	if r != panicNilCallback {
		t.Errorf("panic = %v, want %q", r, panicNilCallback)
	}
}

// Done on the zero Pending panics with a clear message instead of a nil
// pointer dereference.
func TestDoneOnZeroPendingPanics(t *testing.T) {
	var p Pending
	r := recoverPanic(func() {
		if err := p.Done(func() error { return nil }); err != nil {
			t.Error(err)
		}
	})
	want := "eventloop: Done on zero Pending"
	if r != want {
		t.Errorf("panic = %v, want %q", r, want)
	}
}

// If wake fails, Post returns the error and does not queue the callback, so
// an error from Post always means the callback will not run.
func TestPostDoesNotQueueWhenWakeFails(t *testing.T) {
	l := newLoop(t)
	// Close only the poller, so Post gets past the closed check and fails
	// in wake.
	if err := l.poller.close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Post(func() error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Post() = %v, want an error wrapping %v", err, ErrClosed)
	}
	if n := len(l.tasks); n != 0 {
		t.Fatalf("len(l.tasks) = %d, want 0", n)
	}
}

// Close drops callbacks that were posted but have not run: a later Run does
// not run them.
func TestCloseDropsQueuedCallbacks(t *testing.T) {
	l := newLoop(t)
	runs := 0
	if err := l.Post(func() error { runs++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run() = %v, want %v", err, ErrClosed)
	}
	if runs != 0 {
		t.Fatalf("callback ran %d times, want 0", runs)
	}
}

// A hold whose Done failed because the loop was closed is never released.
// Run must still return (with ErrClosed) instead of blocking forever.
func TestDoneAfterCloseDoesNotHangRun(t *testing.T) {
	l := newLoop(t)
	p := l.Hold()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Done(func() error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Done() = %v, want an error wrapping %v", err, ErrClosed)
	}
	if err := runWithTimeout(t, l); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run() = %v, want an error wrapping %v", err, ErrClosed)
	}
}

// A callback posted while a batch is running must survive to the next batch
// and run exactly once (runTasks must not clear or drop the live queue).
func TestCallbackPostedDuringBatchRunsOnce(t *testing.T) {
	l := newLoop(t)
	runs := 0
	cb := func() error {
		return l.Post(func() error { runs++; return nil })
	}
	if err := l.Post(cb); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("inner callback ran %d times, want 1", runs)
	}
}

// Once the two task slices have grown, running a batch must not allocate.
func TestRunTasksDoesNotAllocate(t *testing.T) {
	l := newLoop(t)
	cb := func() error { return nil }
	n := 100
	for range n {
		if err := l.Post(cb); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.runTasks(); err != nil {
		t.Fatal(err)
	}
	if err := l.runTasks(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		l.mu.Lock()
		for range n {
			l.tasks = append(l.tasks, cb)
		}
		l.mu.Unlock()
		if err := l.runTasks(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("got %v allocs, want 0", allocs)
	}
}
