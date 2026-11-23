package eventloop

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Tests in this file test Loop through behavior that only kqueue has. Tests
// that hold for every backend are in subscribe_test.go.

// The fd-reuse case: fd is readable and writable, so both its events are in
// the first batch. Whichever callback runs first unsubscribes fd, then uses
// Dup2 to close fd and put the replacement socket at the same number, and
// subscribes new callbacks for both interests on it. The replacement is ready
// for nothing, so a call to either new callback can only come from the old
// event still pending in the batch.
//
// The test needs read and write to arrive as two separate events in one batch,
// which is how kqueue reports them. epoll reports both interests of an fd in a
// single event, so there would be no stale second event, and the test would
// pass without testing anything.
func TestUnsubscribeCloseReuseDoesNotRunNewCallbackForOldEvent(t *testing.T) {
	l := newTestLoop(t)
	fd, peer := newSocketpair(t)
	fd2, _ := newSocketpair(t)
	fillWriteBuffer(t, fd2)
	var calls [2]int
	ready := func(self, other Interest) func() error {
		return func() error {
			calls[self]++
			if calls[other] != 0 {
				return fmt.Errorf("%v callback called %d times, want 0", other, calls[other])
			}
			if err := l.Unsubscribe(fd, self); err != nil {
				return err
			}
			if err := l.Unsubscribe(fd, other); err != nil {
				return err
			}
			if err := unix.Dup2(fd2, fd); err != nil {
				return err
			}
			if err := l.Subscribe(fd, Readable, func() error {
				return errors.New("replacement Readable fired")
			}); err != nil {
				return err
			}
			return l.Subscribe(fd, Writable, func() error {
				return errors.New("replacement Writable fired")
			})
		}
	}
	if _, err := unix.Write(peer, []byte{0}); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Writable, ready(Writable, Readable)); err != nil {
		t.Fatal(err)
	}
	if err := l.Subscribe(fd, Readable, ready(Readable, Writable)); err != nil {
		t.Fatal(err)
	}
	// The timer ends Run once the batch is over: the replacement never becomes
	// ready, so nothing else would.
	l.AddTimeout(50*time.Millisecond, func() error {
		if err := l.Unsubscribe(fd, Readable); err != nil {
			return err
		}
		return l.Unsubscribe(fd, Writable)
	})
	if err := runWithTimeout(t, l); err != nil {
		t.Fatal(err)
	}
	if calls[Readable]+calls[Writable] != 1 {
		t.Fatalf("old callbacks ran %d times in all, want 1", calls[Readable]+calls[Writable])
	}
}
