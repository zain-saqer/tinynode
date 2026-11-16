package eventloop

import (
	"container/heap"
	"errors"
	"fmt"
	"time"
)

// blockForever is passed to poller.wait to block until an event or wake.
const blockForever = time.Duration(-1)

// ErrClosed is returned when a Loop is used after Close.
var ErrClosed = errors.New("loop is closed")

// Interest is the kind of readiness to watch a file descriptor for.
type Interest int

const (
	// Readable means the fd has data to read, or the peer closed its side
	// (a read then returns 0 bytes).
	Readable Interest = iota
	// Writable means the fd has room in its send buffer for a write.
	Writable
)

func (i Interest) String() string {
	switch i {
	case Readable:
		return "Readable"
	case Writable:
		return "Writable"
	default:
		return fmt.Sprintf("Interest(%d)", int(i))
	}
}

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

// Loop represents an event loop.
type Loop struct {
	timers      timerHeap
	now         func() time.Duration
	nextTimerID int64
	nextSeq     int64
	err         error
	batch       []*timer
	poller      poller
}

// Now returns the loop's current time from a monotonic clock.
func (l *Loop) Now() time.Duration {
	return l.now()
}

// New creates a new event loop.
func New() (*Loop, error) {
	p, err := newPoller()
	if err != nil {
		return nil, fmt.Errorf("eventloop new: %w", err)
	}
	loop := &Loop{
		now:    sinceStart,
		poller: p,
	}
	heap.Init(&loop.timers)
	return loop, nil
}

func (l *Loop) isAlive() bool {
	return len(l.timers) > 0
}

// Close releases the loop's resources. It must not be called while Run is
// running.
func (l *Loop) Close() error {
	if err := l.poller.close(); err != nil {
		return fmt.Errorf("eventloop close: %w", err)
	}
	return nil
}

// handleReady is called when an interest event in a file descriptor has occurred.
func (l *Loop) handleReady(fd int, interest Interest) error {
	return nil
}

// Run runs the loop until no timers are left. After Run returns an error, the
// Loop must not be used again. A Loop is not safe for concurrent use. Run must
// not be called from a callback.
func (l *Loop) Run() error {
	if l.err != nil {
		return l.err
	}
	for l.isAlive() {
		now := l.now()
		if err := l.runDueTimers(now); err != nil {
			l.err = err
			return err
		}

		if !l.isAlive() {
			return nil
		}

		timeout := blockForever
		if len(l.timers) > 0 {
			timeout = max(l.timers[0].runAt-l.now(), 0)
		}

		if err := l.poller.wait(timeout, l.handleReady); err != nil {
			l.err = fmt.Errorf("eventloop run: %w", err)
			return l.err
		}
	}
	return nil
}
