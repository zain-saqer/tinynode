package eventloop

import (
	"errors"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// The tests in this file check the poller contract through newPoller, so every
// backend (kqueue now, epoll later) must pass them. Tests that look inside one
// backend go in that backend's _test file.

var emptyReady = func(fd int, interest Interest) error { return nil }

// readyCall is one call to a ready callback.
type readyCall struct {
	fd       int
	interest Interest
}

// recordReady returns a ready callback that appends every call to *got.
func recordReady(got *[]readyCall) func(fd int, interest Interest) error {
	return func(fd int, interest Interest) error {
		*got = append(*got, readyCall{fd, interest})
		return nil
	}
}

// assertReadyCalls fails the test unless got holds exactly the calls in want,
// in order.
func assertReadyCalls(t *testing.T, got []readyCall, want ...readyCall) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("ready calls = %v, want %v", got, want)
	}
}

// newTestPoller returns this platform's poller and closes it when the test
// ends.
func newTestPoller(t *testing.T) poller {
	t.Helper()
	p, err := newPoller()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func waitWithTimeout(t *testing.T, p poller, waitTimeout time.Duration, ready func(fd int, interest Interest) error) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		err := p.wait(waitTimeout, ready)
		result <- err
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return")
	}
	return nil
}

// newPipe returns both ends of a pipe and closes them when the test ends.
func newPipe(t *testing.T) (r, w int) {
	t.Helper()
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := unix.Close(fds[0]); err != nil {
			t.Error(err)
		}
		if err := unix.Close(fds[1]); err != nil {
			t.Error(err)
		}
	})
	return fds[0], fds[1]
}

func TestWaitTimesOut(t *testing.T) {
	p := newTestPoller(t)
	err := waitWithTimeout(t, p, 10*time.Millisecond, emptyReady)
	if err != nil {
		t.Fatal(err)
	}
}

// a trigger sent before wait must not be lost
func TestEventTriggerKeepsPendingBeforeWaitCall(t *testing.T) {
	p := newTestPoller(t)
	if err := p.wake(); err != nil {
		t.Fatal(err)
	}
	if err := waitWithTimeout(t, p, -1, emptyReady); err != nil {
		t.Fatal(err)
	}
}

// A wait that is already blocked must return once wake is called.
func TestWakeUnblocksBlockedWait(t *testing.T) {
	p := newTestPoller(t)
	waitResult := make(chan error, 1)
	go func() {
		waitResult <- p.wait(-1, emptyReady)
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("stopped waiting: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := p.wake(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitResult:
		if err != nil {
			t.Fatalf("wait returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return after wake")
	}
}

// wait must call ready with the fd and interest of a registered fd that
// becomes ready.
func TestWaitReportsReadableFd(t *testing.T) {
	p := newTestPoller(t)
	r, w := newPipe(t)
	if err := p.add(r, Readable); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w, []byte{0}); err != nil {
		t.Fatal(err)
	}
	var got []readyCall
	if err := waitWithTimeout(t, p, time.Second, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got, readyCall{r, Readable})
}

// a read trigger goes away after reading the data from fd
func TestReadTriggerDoesNotPersistAfterRead(t *testing.T) {
	p := newTestPoller(t)
	r, w := newPipe(t)
	if err := p.add(r, Readable); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w, []byte("test")); err != nil {
		t.Fatal(err)
	}
	var got []readyCall
	if err := waitWithTimeout(t, p, time.Second, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got, readyCall{r, Readable})
	buffer := make([]byte, 20)
	n, err := unix.Read(r, buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[:n]) != "test" {
		t.Fatalf("read data was %q, expected %q", buffer[:n], "test")
	}
	got = nil
	if err := waitWithTimeout(t, p, 10*time.Millisecond, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got)
}

// The wake event must be cleared once wait reports it (EV_CLEAR on kqueue).
// Otherwise it stays triggered and every later wait returns at once, so the
// loop spins.
func TestWakeIsClearedAfterWait(t *testing.T) {
	p := newTestPoller(t)
	if err := p.wake(); err != nil {
		t.Fatal(err)
	}
	if err := p.wake(); err != nil {
		t.Fatal(err)
	}
	if err := waitWithTimeout(t, p, -1, emptyReady); err != nil {
		t.Fatal(err)
	}
	startTime := time.Now()
	if err := p.wait(50*time.Millisecond, emptyReady); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(startTime)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("wait did not wait, elapsed time was %v", elapsed)
	}
}

// The write end of an empty pipe is immediately writable.
func TestWaitReportsWritableFd(t *testing.T) {
	p := newTestPoller(t)
	_, w := newPipe(t)
	if err := p.add(w, Writable); err != nil {
		t.Fatal(err)
	}
	var got []readyCall
	if err := p.wait(0, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got, readyCall{w, Writable})
}

// add must return an error for an invalid fd instead of failing silently.
func TestAddInvalidFdReturnsError(t *testing.T) {
	p := newTestPoller(t)
	if err := p.add(-1, Readable); !errors.Is(err, unix.EBADF) {
		t.Fatalf("add(-1) error = %v, want %v", err, unix.EBADF)
	}
}

// An error from ready stops processing the batch and is returned by wait.
func TestWaitReturnsReadyError(t *testing.T) {
	testError := errors.New("test error")
	p := newTestPoller(t)
	r1, w1 := newPipe(t)
	r2, w2 := newPipe(t)
	if err := p.add(r1, Readable); err != nil {
		t.Fatal(err)
	}
	if err := p.add(r2, Readable); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w1, []byte(`x`)); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w2, []byte(`x`)); err != nil {
		t.Fatal(err)
	}
	runs := 0
	ready := func(fd int, interest Interest) error {
		runs++
		return testError
	}
	err := waitWithTimeout(t, p, 50*time.Millisecond, ready)
	if err == nil {
		t.Fatal("wait did not return an error")
	}
	if !errors.Is(err, testError) {
		t.Fatalf("wait returned %v, want %v", err, testError)
	}
	if runs != 1 {
		t.Fatalf("ready was called %d times, want 1", runs)
	}
}

// After remove, a readable fd is no longer reported.
func TestRemoveStopsReporting(t *testing.T) {
	p := newTestPoller(t)
	r, w := newPipe(t)
	if err := p.add(r, Readable); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w, []byte(`x`)); err != nil {
		t.Fatal(err)
	}
	var got []readyCall
	if err := waitWithTimeout(t, p, 50*time.Millisecond, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got, readyCall{r, Readable})
	if err := p.remove(r, Readable); err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := waitWithTimeout(t, p, 50*time.Millisecond, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got)
}

// Removing an fd that was never registered returns nil.
func TestRemoveUnregisteredReturnsNil(t *testing.T) {
	p := newTestPoller(t)
	r, _ := newPipe(t)
	if err := p.remove(r, Readable); err != nil {
		t.Fatal(err)
	}
}

// Closing the write end of a pipe makes the read end readable (EOF), even
// though no data was written. A read then returns 0 bytes.
func TestCloseWriteEndReportsReadable(t *testing.T) {
	p := newTestPoller(t)
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		t.Fatal(err)
	}
	r, w := fds[0], fds[1]
	t.Cleanup(func() {
		if err := unix.Close(r); err != nil {
			t.Error(err)
		}
		if w != -1 {
			if err := unix.Close(w); err != nil {
				t.Error(err)
			}
		}
	})
	if err := p.add(r, Readable); err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(w); err != nil {
		t.Fatal(err)
	}
	w = -1
	var got []readyCall
	if err := waitWithTimeout(t, p, 50*time.Millisecond, recordReady(&got)); err != nil {
		t.Fatal(err)
	}
	assertReadyCalls(t, got, readyCall{r, Readable})
	buff := make([]byte, 10)
	n, err := unix.Read(r, buff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// add, remove, wake and wait on a closed poller must return ErrClosed instead
// of using fd -1 (or a reused fd).
func TestPollerMethodsAfterCloseReturnErrClosed(t *testing.T) {
	p := newTestPoller(t)
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		f    func() error
	}{
		{"add", func() error { return p.add(-1, Readable) }},
		{"remove", func() error { return p.remove(-1, Readable) }},
		{"wake", func() error { return p.wake() }},
		{"wait", func() error { return p.wait(0, emptyReady) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.f(); !errors.Is(err, ErrClosed) {
				t.Fatalf("expected ErrClosed, got %v", err)
			}
		})
	}
}
