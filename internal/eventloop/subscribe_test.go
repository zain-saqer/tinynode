package eventloop

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// fillWriteBuffer makes fd non-blocking and writes to it until the write
// fails with EAGAIN, so its buffer is full and it is no longer writable.
func fillWriteBuffer(t *testing.T, fd int) {
	t.Helper()
	if err := unix.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	buff := bytes.Repeat([]byte("x"), 4096)
	for {
		_, err := unix.Write(fd, buff)
		if err == nil {
			continue
		}
		if errors.Is(err, unix.EAGAIN) {
			return
		}
		t.Fatal(err)
	}
}

// drainBuffer reads from fd until the read would block (EAGAIN) or hits end of
// file, so fd has nothing left to read. fd must be non-blocking, or the last
// read blocks. It returns an error instead of taking t because it runs inside
// loop callbacks, which are not on the test goroutine.
func drainBuffer(fd int) error {
	buff := make([]byte, 4096)
	for {
		n, err := unix.Read(fd, buff)
		if n == 0 || errors.Is(err, unix.EAGAIN) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Delivery

// Subscribe Readable on a pipe with data runs the callback.
func TestSubscribeReadableRunsCallback(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	calls := 0
	testString := "test"
	if err := l.Subscribe(r, Readable, func() error {
		calls++
		buff := make([]byte, 10)
		n, err := unix.Read(r, buff)
		if err != nil {
			return err
		}
		readString := string(buff[:n])
		if readString != testString {
			return fmt.Errorf("read %q, want %q", readString, testString)
		}
		if err := l.Unsubscribe(r, Readable); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w, []byte(testString)); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

// Subscribe Writable on an fd with room in its send buffer runs the callback.
func TestSubscribeWritableRunsCallback(t *testing.T) {
	l := newTestLoop(t)
	_, w := newPipe(t)
	calls := 0
	if err := l.Subscribe(w, Writable, func() error {
		calls++
		if err := l.Unsubscribe(w, Writable); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// Subscribe Writable on an fd whose buffer is full does not run the callback
// until the reader makes room.
func TestSubscribeWritableWaitsForRoom(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	calls := 0
	fillWriteBuffer(t, w)
	readDone := false
	l.AddTimeout(50*time.Millisecond, func() error {
		// the poller reports a pipe as writable only when enough space is free
		// (kqueue: PIPE_BUF; Linux: a whole page), so free plenty
		buff := make([]byte, 65536)
		_, err := unix.Read(r, buff)
		readDone = true
		return err
	})
	guard := l.AddTimeout(500*time.Millisecond, func() error {
		return errors.New("writable not reported after the reader made room")
	})
	if err := l.Subscribe(w, Writable, func() error {
		calls++
		l.CancelTimer(guard)
		if !readDone {
			return errors.New("writable subscription fired before read")
		}
		if err := l.Unsubscribe(w, Writable); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// Subscriptions are level-triggered: the callback runs again on the next
// iteration while the fd stays ready (data left unread).
func TestSubscribeCallbackRunsAgainWhileReady(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	if _, err := unix.Write(w, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	calls := 0
	// guard stops Run with a clear error if the callback does not run again.
	// Without it, the subscription would keep Run alive and the test would
	// only fail when runWithTimeout gives up.
	guard := l.AddTimeout(500*time.Millisecond, func() error {
		return fmt.Errorf("callback ran %d time(s), want it to run again while r stays readable", calls)
	})
	if err := l.Subscribe(r, Readable, func() error {
		calls++
		if calls == 2 {
			l.CancelTimer(guard)
			return l.Unsubscribe(r, Readable)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	buff := make([]byte, 100)
	n, err := unix.Read(r, buff)
	if err != nil {
		t.Fatal(err)
	}
	if string(buff[:n]) != "hello" {
		t.Fatalf("read %q, want %q", buff[:n], "hello")
	}
}

// Subscribing the same fd and interest again replaces the callback: only the
// new one runs.
func TestSubscribeAgainReplacesCallback(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	if _, err := unix.Write(w, []byte{0}); err != nil {
		t.Fatal(err)
	}
	oldCalls := 0
	newCalls := 0
	guard := l.AddTimeout(100*time.Millisecond, func() error {
		return fmt.Errorf("old callback ran %d times, new one %d, want 0 and 1", oldCalls, newCalls)
	})
	if err := l.Subscribe(r, Readable, func() error {
		oldCalls++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(r, Readable, func() error {
		newCalls++
		l.CancelTimer(guard)
		return l.Unsubscribe(r, Readable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if oldCalls != 0 {
		t.Errorf("oldCalls = %d, want 0", oldCalls)
	}
	if newCalls != 1 {
		t.Errorf("oldCalls = %d, want 1", newCalls)
	}
}

// Readable and Writable on the same fd each run their own callback.
func TestSubscribeBothInterestsRunsBothCallbacks(t *testing.T) {
	l := newTestLoop(t)
	fd, peer := newSocketpair(t)
	const msg = "test"
	readCalls := 0
	writeCalls := 0
	guard := l.AddTimeout(100*time.Millisecond, func() error {
		return fmt.Errorf("readCalls = %d, writeCalls = %d, want 1 and 1", readCalls, writeCalls)
	})
	if err := l.Subscribe(fd, Writable, func() error {
		writeCalls++
		if _, err := unix.Write(peer, []byte(msg)); err != nil {
			return err
		}
		return l.Unsubscribe(fd, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Readable, func() error {
		readCalls++
		if writeCalls != 1 {
			return fmt.Errorf("writeCalls = %d, want 1", writeCalls)
		}
		buff := make([]byte, 100)
		n, err := unix.Read(fd, buff)
		if err != nil {
			return err
		}
		if got := string(buff[:n]); got != msg {
			return fmt.Errorf("read %q, want %q", got, msg)
		}
		l.CancelTimer(guard)
		return l.Unsubscribe(fd, Readable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if readCalls != 1 {
		t.Errorf("readCalls = %d, want 1", readCalls)
	}
	if writeCalls != 1 {
		t.Errorf("writeCalls = %d, want 1", writeCalls)
	}
}

// An error from the callback stops Run, and Run returns it unchanged
// (errors.Is and ==).
func TestSubscribeCallbackErrorStopsRun(t *testing.T) {
	l := newTestLoop(t)
	_, w := newPipe(t)
	sentinelErr := errors.New("sentinel error")
	if err := l.Subscribe(w, Writable, func() error {
		return sentinelErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != sentinelErr {
		t.Fatalf("runWithTimeout() = %v, want %v", err, sentinelErr)
	}
}

// Liveness

// Run does not return while a subscription exists and returns after the last
// Unsubscribe.
func TestSubscriptionKeepsRunAlive(t *testing.T) {
	l := newTestLoop(t)
	r, w := newPipe(t)
	calls := 0
	if err := l.Subscribe(r, Readable, func() error {
		calls++
		return l.Unsubscribe(r, Readable)
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		// Write once Run is idle in kevent, so that only the subscription
		// keeps it alive until then.
		time.Sleep(50 * time.Millisecond)
		_, err := unix.Write(w, []byte{0})
		result <- err
	}()
	runErr := runWithTimeout(t, l)
	if err := <-result; err != nil {
		t.Fatalf("write: %v", err)
	}
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// Run keeps running while one interest on the fd is still subscribed after
// the other is removed.
func TestUnsubscribeOneInterestKeepsRunAlive(t *testing.T) {
	l := newTestLoop(t)
	fd, peer := newSocketpair(t)
	readCalls := 0
	writeCalls := 0
	if err := l.Subscribe(fd, Readable, func() error {
		readCalls++
		if writeCalls != 1 {
			return errors.New("write not called")
		}
		return l.Unsubscribe(fd, Readable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Writable, func() error {
		writeCalls++
		return l.Unsubscribe(fd, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		// Write once Run is idle in kevent, so that only the Readable subscription
		// keeps it alive until then.
		time.Sleep(50 * time.Millisecond)
		_, err := unix.Write(peer, []byte{0})
		result <- err
	}()
	runErr := runWithTimeout(t, l)
	if err := <-result; err != nil {
		t.Fatalf("write: %v", err)
	}
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
	if readCalls != 1 {
		t.Errorf("readCalls = %d, want 1", readCalls)
	}
	if writeCalls != 1 {
		t.Errorf("writeCalls = %d, want 1", writeCalls)
	}
}

// Unsubscribe

// Unsubscribe removes only that interest: the other interest's callback on the
// same fd still runs. fd starts with a full send buffer and data to read, so
// only Readable is ready. The Readable callback unsubscribes Readable and drains
// peer, which makes room in fd's buffer, so Writable fires next.
func TestUnsubscribeKeepsOtherInterest(t *testing.T) {
	l := newTestLoop(t)
	fd, peer := newSocketpair(t)
	readCalls := 0
	writeCalls := 0
	fillWriteBuffer(t, fd)
	guard := l.AddTimeout(100*time.Millisecond, func() error {
		return fmt.Errorf("readCalls = %d, writeCalls = %d, want 1 and 1", readCalls, writeCalls)
	})
	if _, err := unix.Write(peer, []byte{0}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Readable, func() error {
		readCalls++
		if err := drainBuffer(peer); err != nil {
			return err
		}
		return l.Unsubscribe(fd, Readable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Writable, func() error {
		writeCalls++
		if readCalls != 1 {
			return fmt.Errorf("readCalls = %d, want 1", readCalls)
		}
		l.CancelTimer(guard)
		return l.Unsubscribe(fd, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if readCalls != 1 {
		t.Errorf("readCalls = %d, want 1", readCalls)
	}
	if writeCalls != 1 {
		t.Errorf("writeCalls = %d, want 1", writeCalls)
	}
}

// A callback can unsubscribe itself; it does not run again.
func TestUnsubscribeFromOwnCallback(t *testing.T) {
	l := newTestLoop(t)
	_, w := newPipe(t)
	calls := 0
	if err := l.Subscribe(w, Writable, func() error {
		calls++
		return l.Unsubscribe(w, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	// The timer keeps the loop alive for one more wait. w stays writable, so a
	// subscription that is still there would run the callback again.
	l.AddTimeout(20*time.Millisecond, func() error {
		if calls != 1 {
			return fmt.Errorf("callback ran %d times, want 1", calls)
		}
		return nil
	})
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
}

// When both events for an fd are in one batch, and the first callback
// unsubscribes the other interest, the other callback does not run. fd is
// readable and writable before Run, so both events are in the first batch.
// One callback is subscribed for both interests and removes both, so whichever
// event comes first, the other one, still pending in the same batch, must not
// run it.
func TestUnsubscribeInBatchSkipsPendingEvent(t *testing.T) {
	l := newTestLoop(t)
	fd, peer := newSocketpair(t)
	if _, err := unix.Write(peer, []byte{0}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	guard := l.AddTimeout(100*time.Millisecond, func() error {
		return errors.New("callback never ran")
	})
	ready := func() error {
		calls++
		l.CancelTimer(guard)
		if err := l.Unsubscribe(fd, Writable); err != nil {
			return err
		}
		return l.Unsubscribe(fd, Readable)
	}
	if err := l.Subscribe(fd, Writable, ready); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Readable, ready); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// Unsubscribing an fd or interest that is not subscribed returns nil. When
// only the interest is not subscribed, the other interest on the fd stays
// subscribed: its callback still runs.
func TestUnsubscribeNotSubscribedReturnsNil(t *testing.T) {
	l := newTestLoop(t)
	_, w := newPipe(t)
	if err := l.Unsubscribe(w, Readable); err != nil {
		t.Fatalf("Unsubscribe() = %v, want nil", err)
	}
	calls := 0
	if err := l.Subscribe(w, Writable, func() error {
		calls++
		return l.Unsubscribe(w, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Unsubscribe(w, Readable); err != nil {
		t.Fatalf("Unsubscribe() = %v, want nil", err)
	}
	// w is writable, so the Writable callback runs if it is still subscribed.
	// If Unsubscribe had removed it, Run would return right away.
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("Writable callback ran %d times, want 1", calls)
	}
}

// Calling Unsubscribe twice returns nil the second time.
func TestUnsubscribeTwiceReturnsNil(t *testing.T) {
	l := newTestLoop(t)
	r, _ := newPipe(t)
	if err := l.Subscribe(r, Readable, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Unsubscribe(r, Readable); err != nil {
		t.Fatalf("Unsubscribe() = %v, want nil", err)
	}
	if err := l.Unsubscribe(r, Readable); err != nil {
		t.Fatalf("Unsubscribe() = %v, want nil", err)
	}
}

// Unsubscribe after the fd is closed returns nil, and the subscription is
// still removed: Run returns.
func TestUnsubscribeAfterCloseFdRemovesSubscription(t *testing.T) {
	l := newTestLoop(t)
	r, _ := newPipe(t)
	fd, err := unix.Dup(r)
	if err != nil {
		t.Fatal(err)
	}
	// The pipe is never readable, so any call is a bug.
	if err := l.Subscribe(fd, Readable, func() error {
		return errors.New("callback ran for a closed fd")
	}); err != nil {
		if err := unix.Close(fd); err != nil {
			t.Error(err)
		}
		t.Fatal(err)
	}
	if err := unix.Close(fd); err != nil {
		t.Fatal(err)
	}
	if err := l.Unsubscribe(fd, Readable); err != nil {
		t.Fatalf("Unsubscribe() = %v, want nil", err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
}

// Errors and panics

// Subscribe on an fd that is not open returns an error wrapping EBADF and
// leaves no subscription behind: Run returns right away.
func TestSubscribeBadFdReturnsErrorAndSubscribesNothing(t *testing.T) {
	l := newTestLoop(t)
	// -1 is never open. A closed fd would not do: its number can be reused,
	// even by the loop's own kqueue.
	if err := l.Subscribe(-1, Readable, func() error {
		return errors.New("callback ran after Subscribe failed")
	}); !errors.Is(err, unix.EBADF) {
		t.Fatalf("Subscribe() = %v, want an error wrapping EBADF", err)
	}
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
}

// Subscribe panics on an unknown interest, with a message that names
// Subscribe and the bad value. The test checks those parts, not the exact
// wording.
func TestSubscribeUnknownInterestPanics(t *testing.T) {
	l := newTestLoop(t)
	fd, _ := newPipe(t)
	unknownInterest := Interest(-1)
	got := recoverPanic(func() {
		if err := l.Subscribe(fd, unknownInterest, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	msg, ok := got.(string)
	if !ok {
		t.Fatalf("panic = %#v, want a string", got)
	}
	for _, want := range []string{"Subscribe", unknownInterest.String()} {
		if !strings.Contains(msg, want) {
			t.Errorf("panic = %q, want it to contain %q", msg, want)
		}
	}
}

// Unsubscribe panics on an unknown interest, with a message that names
// Unsubscribe.
func TestUnsubscribeUnknownInterestPanics(t *testing.T) {
	l := newTestLoop(t)
	fd, _ := newPipe(t)
	unknownInterest := Interest(-1)
	got := recoverPanic(func() {
		if err := l.Unsubscribe(fd, unknownInterest); err != nil {
			t.Fatal(err)
		}
	})
	msg, ok := got.(string)
	if !ok {
		t.Fatalf("panic = %#v, want a string", got)
	}
	for _, want := range []string{"Unsubscribe", unknownInterest.String()} {
		if !strings.Contains(msg, want) {
			t.Errorf("panic = %q, want it to contain %q", msg, want)
		}
	}
}

// Subscribe panics with panicNilCallback when cb is nil.
func TestSubscribePanicsWithNilCallback(t *testing.T) {
	l := newTestLoop(t)
	fd, _ := newPipe(t)
	got := recoverPanic(func() {
		if err := l.Subscribe(fd, Writable, nil); err != nil {
			t.Fatal(err)
		}
	})
	if got != panicNilCallback {
		t.Errorf("panic = %#v, want %q", got, panicNilCallback)
	}
}

// Subscribe panics with panicLoopClosed on a closed loop.
func TestSubscribeAfterClosePanics(t *testing.T) {
	l := newTestLoop(t)
	if err := l.Close(); err != nil {
		t.Error(err)
	}
	r, _ := newPipe(t)
	got := recoverPanic(func() {
		if err := l.Subscribe(r, Writable, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	if got != panicLoopClosed {
		t.Errorf("panic = %#v, want %q", got, panicLoopClosed)
	}
}

// Unsubscribe on a closed loop returns nil.
func TestUnsubscribeAfterCloseReturnsNil(t *testing.T) {
	l := newTestLoop(t)
	r, _ := newPipe(t)
	if err := l.Subscribe(r, Readable, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Error(err)
	}
	if !l.isClosed() {
		t.Fatal("loop is not closed")
	}
	if err := l.Unsubscribe(r, Readable); err != nil {
		t.Error(err)
	}
}

// Close drops all subscriptions but does not close the subscribed fds: the
// caller owns them, and a new loop can subscribe them again.
func TestCloseDropsSubscriptionsAndLeavesFdsOpen(t *testing.T) {
	l := newTestLoop(t)
	_, w := newPipe(t)
	if err := l.Subscribe(w, Writable, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Error(err)
	}
	if len(l.watchers) != 0 {
		t.Errorf("l.watchers = %v, want empty", l.watchers)
	}
	// Check w before anything else can open an fd: if Close had closed w, a new
	// fd could reuse its number and hide the bug.
	if _, err := unix.FcntlInt(uintptr(w), unix.F_GETFD, 0); err != nil {
		t.Fatal(err)
	}
	l2 := newTestLoop(t)
	calls := 0
	if err := l2.Subscribe(w, Writable, func() error {
		calls++
		return l2.Unsubscribe(w, Writable)
	}); err != nil {
		t.Fatal(err)
	}
	if err := runWithTimeout(t, l2); err != nil {
		t.Error(err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}
