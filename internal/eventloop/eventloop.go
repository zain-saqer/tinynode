package eventloop

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
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

// String returns the name of the interest, or Interest(n) for an unknown
// value.
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

// Callback is a function that the Loop runs on the goroutine running Run.
// AddTimeout and AddInterval run one when a timer is due; Post and
// Pending.Done queue one to run in the next iteration. If a Callback returns
// an error, Run stops and returns that error unchanged.
type Callback func() error

// Loop represents an event loop. A Loop is not safe for concurrent use, except
// for Post and Pending.Done, which may be called from any goroutine.
type Loop struct {
	// These fields are only used on the goroutine running Run.
	timers      timerHeap
	now         func() time.Duration
	nextTimerID int64
	nextSeq     int64
	err         error
	batch       []*timer
	poller      poller
	holds       int // number of Pendings whose Done callback has not run yet

	// mu guards tasks and closed, which Post reads and writes from other
	// goroutines. Holding mu in both Post and Close also keeps poller.wake
	// from running during or after poller.close.
	mu     sync.Mutex
	tasks  []Callback // callbacks posted since the last runTasks
	closed bool
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

// isAlive reports whether the loop still has work: a pending timer, a posted
// callback, or a Pending whose Done callback has not run yet.
func (l *Loop) isAlive() bool {
	l.mu.Lock()
	tasksCount := len(l.tasks)
	l.mu.Unlock()
	return len(l.timers) > 0 || tasksCount > 0 || l.holds > 0
}

// TODO: The "used after Close" contract is only half enforced:
//   - Run on a closed loop with no timers returns nil instead of ErrClosed,
//     because isAlive is false and Run never reaches the poller.
//   - AddTimeout and AddInterval after Close succeed silently; the timer can
//     never fire and ErrClosed only shows up later, from Run.
//
// Loop now has a closed flag (set by Close, checked by Post): check it at the
// top of Run too, and decide what adding a timer after Close does (panic or
// no-op), then document it.
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

// Close releases the loop's resources. Callbacks that were posted but have not
// run yet are dropped, and later calls to Post return ErrClosed. Close must not
// be called while Run is running. Calling Close more than once returns nil.
func (l *Loop) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.tasks = nil
	if err := l.poller.close(); err != nil {
		return fmt.Errorf("eventloop close: %w", err)
	}
	return nil
}

// handleReady is called by poller.wait for each fd that is ready. It is a stub
// until fds can be registered with handlers.
func (l *Loop) handleReady(fd int, interest Interest) error {
	return nil
}

// Run runs the loop until no timers, posted callbacks or Pendings are left.
// Each iteration runs the due timers, waits for fd events, a wake from Post,
// or the next timer, and then runs the posted callbacks.
//
// If a Callback returns an error, Run stops and returns that error unchanged.
// After Run returns an error, the Loop must not be used again. Run must not be
// called from a Callback.
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
		l.mu.Lock()
		if len(l.tasks) > 0 {
			timeout = 0
		}
		l.mu.Unlock()

		if err := l.poller.wait(timeout, l.handleReady); err != nil {
			l.err = fmt.Errorf("eventloop run: %w", err)
			return l.err
		}

		if err := l.runTasks(); err != nil {
			l.err = err
			return err
		}
	}
	return nil
}
