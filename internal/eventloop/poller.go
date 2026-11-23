package eventloop

import (
	"time"
)

// blockForever is passed to poller.wait to block until an event or wake.
const blockForever = time.Duration(-1)

// poller waits for readiness on file descriptors. A poller is not safe for
// concurrent use, with one exception: wake may be called from any goroutine
// while wait runs. The caller must make sure wake and close never run at the
// same time, and that nothing is called after close (Loop does this by
// holding mu in both Post and Close).
type poller interface {
	// wait waits for events on file descriptors and calls ready for each one.
	// A negative timeout blocks until an event or wake.
	wait(timeout time.Duration, ready func(fd int, interest Interest) error) error
	close() error
	add(fd int, interest Interest) error
	// remove unregisters fd for interest. It returns nil if fd is not
	// registered for interest or is already closed (kqueue reports ENOENT,
	// epoll reports EBADF or ENOENT), so Loop.Unsubscribe behaves the same on
	// every platform.
	remove(fd int, interest Interest) error
	wake() error
	// invalidate keeps wait from reporting the events for fd that are still
	// waiting in the batch it is processing. Loop calls it when the last
	// subscription on fd is removed, so that if fd is closed and its number
	// reused, the new fd never gets the old fd's events. Outside wait there
	// is no batch, and invalidate does nothing. It makes no syscall, so it
	// cannot fail.
	invalidate(fd int)
}
