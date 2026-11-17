package eventloop

import (
	"container/heap"
	"errors"
	"fmt"
	"time"
)

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

// TODO: The "used after Close" contract is only half enforced:
//   - Run on a closed loop with no timers returns nil instead of ErrClosed,
//     because isAlive is false and Run never reaches the poller.
//   - AddTimeout and AddInterval after Close succeed silently; the timer can
//     never fire and ErrClosed only shows up later, from Run.
//
// Give Loop its own closed state, check it at the top of Run, and decide what
// adding a timer after Close does (panic or no-op), then document it.
//
// TODO: Enforce "Close must not be called while Run is running" and "Run must
// not be called from a callback" with a running flag that Run sets on entry
// and clears with defer:
//   - Run while running: panic (always a programming error).
//   - Close while running: return an ErrRunning error, like libuv's
//     uv_loop_close returning UV_EBUSY. A callback that wants to shut the
//     loop down will later use a separate Stop, like uv_stop.
//
// A plain bool also lets go test -race catch Close from another goroutine.

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
