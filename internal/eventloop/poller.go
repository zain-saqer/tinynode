package eventloop

import (
	"time"
)

// blockForever is passed to poller.wait to block until an event or wake.
const blockForever = time.Duration(-1)

// poller waits for readiness on file descriptors. A poller is not safe for
// concurrent use, except for wake, which may be called from any goroutine to
// interrupt a blocked wait, but not after close.
type poller interface {
	// wait waits for events on file descriptors and calls ready for each one.
	// A negative timeout blocks until an event or wake.
	wait(timeout time.Duration, ready func(fd int, interest Interest) error) error
	close() error
	add(fd int, interest Interest) error
	remove(fd int, interest Interest) error
	wake() error
}
