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

// ErrRunning is returned by Close when Run is running.
var ErrRunning = errors.New("loop is running")

const panicLoopClosed = "eventloop: loop is closed"
const panicLoopRunning = "eventloop: loop is running"

// Interest is the kind of readiness to watch a file descriptor for.
type Interest int

const (
	// Readable means the fd has data to read, or the peer closed its side
	// (a read then returns 0 bytes).
	Readable Interest = iota
	// Writable means the fd has room in its send buffer for a write.
	Writable
)

// valid reports whether i is one of the defined interests.
func (i Interest) valid() bool {
	return i == Readable || i == Writable
}

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
// AddTimeout and AddInterval run one when a timer is due; Subscribe runs one
// when a file descriptor is ready; Post and Pending.Done queue one to run in
// the next iteration. If a Callback returns an error, Run stops and returns
// that error unchanged.
type Callback func() error

// watcher holds the callbacks subscribed for one file descriptor. A nil field
// means that interest is not subscribed. The Loop deletes a watcher once both
// fields are nil.
type watcher struct {
	readable Callback
	writable Callback
}

// slot returns a pointer to the field that holds the callback for interest, so
// callers can read and set it in one place. It panics on an unknown interest;
// callers check interest.valid first.
func (w *watcher) slot(interest Interest) *Callback {
	switch interest {
	case Readable:
		return &w.readable
	case Writable:
		return &w.writable
	default:
		panic(fmt.Sprintf("eventloop: watcher.slot: unknown interest %v", interest))
	}
}

// Loop represents an event loop. A Loop is not safe for concurrent use, except
// for Post and Pending.Done, which may be called from any goroutine.
type Loop struct {
	// These fields are only used on the goroutine running Run.
	timers      timerHeap
	now         func() time.Duration
	nextTimerID int64
	nextSeq     int64
	err         error
	timerBatch  []*timer   // the batch runDueTimers is running
	taskBatch   []Callback // the batch runTasks is running; swapped with tasks each call
	poller      poller
	holds       int  // number of Pendings whose Done callback has not run yet
	running     bool // set while Run is running; guards against re-entry and Close from a Callback
	watchers    map[int]*watcher

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
		now:      sinceStart,
		poller:   p,
		watchers: make(map[int]*watcher),
	}
	heap.Init(&loop.timers)
	return loop, nil
}

// isAlive reports whether the loop still has work: a pending timer, a posted
// callback, a Pending whose Done callback has not run yet, or an fd
// subscription.
func (l *Loop) isAlive() bool {
	return l.aliveWith(l.hasTasks())
}

// aliveWith is isAlive for a caller that already read hasTasks, so it does not
// take mu again.
func (l *Loop) aliveWith(hasTasks bool) bool {
	return len(l.timers) > 0 || hasTasks || l.holds > 0 || len(l.watchers) > 0
}

// hasTasks reports whether posted callbacks are waiting to run.
func (l *Loop) hasTasks() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.tasks) > 0
}

// Close releases the loop's resources. Callbacks that were posted but have not
// run yet, timers that have not fired and subscriptions are dropped. The
// subscribed file descriptors are not closed, because the caller owns them.
// After Close, Run, Post and Pending.Done return ErrClosed, AddTimeout,
// AddInterval and Hold panic, CancelTimer does nothing, and Unsubscribe
// returns nil. If Run is running (for example, Close is called from a
// Callback), Close returns ErrRunning and does nothing, like libuv's
// uv_loop_close returning UV_EBUSY. Calling Close more than once returns nil.
func (l *Loop) Close() error {
	if l.running {
		return fmt.Errorf("eventloop close: %w", ErrRunning)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	l.tasks = nil
	for _, tmr := range l.timers {
		tmr.index = -1
		tmr.canceled = true
	}
	l.timers = nil
	l.watchers = nil
	if err := l.poller.close(); err != nil {
		return fmt.Errorf("eventloop close: %w", err)
	}
	return nil
}

// isClosed returns whether the loop has been closed. It is safe for concurrent
// use.
func (l *Loop) isClosed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// Subscribe arranges for cb to run on the loop's goroutine each time fd is
// ready for interest. Call it before Run or from a Callback. Subscribing the
// same fd and interest again replaces the callback.
//
// A subscription keeps Run running until it is removed with Unsubscribe.
//
// Subscriptions are level-triggered: cb runs again on every iteration for as
// long as fd stays ready. Unsubscribe Writable when there is nothing left to
// write, or the loop spins. Readiness is only a hint: a read or write in cb
// can still fail with EAGAIN.
//
// cb is a Callback, so an error it returns stops Run. Return an error only for
// a fatal failure. cb must handle expected I/O errors, such as ECONNRESET,
// EPIPE and EAGAIN, itself.
//
// If Subscribe returns an error, for example EBADF because fd is not open,
// nothing is subscribed. Subscribe panics if the loop is closed.
func (l *Loop) Subscribe(fd int, interest Interest, cb Callback) error {
	if !interest.valid() {
		panic(fmt.Sprintf("eventloop: Subscribe: unknown interest %v", interest))
	}
	if cb == nil {
		panic(panicNilCallback)
	}
	if l.isClosed() {
		panic(panicLoopClosed)
	}
	if err := l.poller.add(fd, interest); err != nil {
		return fmt.Errorf("eventloop subscribe: %w", err)
	}
	w := l.watchers[fd]
	if w == nil {
		w = &watcher{}
		l.watchers[fd] = w
	}
	*w.slot(interest) = cb
	return nil
}

// Unsubscribe removes the subscription for fd and interest. A subscription for
// the other interest on the same fd is not affected. Call it before Run or from
// a Callback. Once no subscriptions are left, they no longer keep Run running.
//
// After Unsubscribe returns, the callback never runs again for that
// subscription.
//
// Unsubscribe fd before closing it. Closing fd drops its registration in the
// kernel but not the subscription, so the callback never runs again and the
// subscription keeps Run running forever. And once fd is closed, the kernel can
// give its number to a new file or socket, and a late Unsubscribe would remove
// that one's registration instead.
//
// Unsubscribing something that is not subscribed does nothing and returns nil,
// and so does Unsubscribe on a closed loop. If fd is already closed, the
// subscription is still removed and Unsubscribe returns nil on every platform:
// kqueue drops a closed fd's registrations itself and cannot tell it apart
// from an fd that was never registered. For any other error from the kernel,
// the subscription is still removed and the error is returned.
func (l *Loop) Unsubscribe(fd int, interest Interest) error {
	if !interest.valid() {
		panic(fmt.Sprintf("eventloop: Unsubscribe: unknown interest %v", interest))
	}
	if l.isClosed() {
		return nil
	}
	w := l.watchers[fd]
	if w == nil {
		return nil
	}
	cb := w.slot(interest)
	if *cb == nil {
		return nil
	}
	*cb = nil
	if w.readable == nil && w.writable == nil {
		delete(l.watchers, fd)
		l.poller.invalidate(fd)
	}
	if err := l.poller.remove(fd, interest); err != nil {
		return fmt.Errorf("eventloop unsubscribe: %w", err)
	}
	return nil
}

// handleReady is called by poller.wait for each fd that is ready.
func (l *Loop) handleReady(fd int, interest Interest) error {
	w := l.watchers[fd]
	if w == nil || !interest.valid() {
		return nil
	}
	if cb := *w.slot(interest); cb != nil {
		return cb()
	}
	return nil
}

// Run runs the loop until no timers, posted callbacks, Pendings or
// subscriptions are left. Each iteration runs the due timers, waits for fd
// events, a wake from Post, or the next timer, and then runs the posted
// callbacks.
//
// If a Callback returns an error, Run stops and returns that error unchanged.
// After Run returns an error, the Loop must not be used again. Run returns
// ErrClosed if the Loop is closed. Run panics if it is called while Run is
// running, for example from a Callback.
func (l *Loop) Run() error {
	if l.running {
		panic(panicLoopRunning)
	}
	if l.err != nil {
		return l.err
	}
	if l.isClosed() {
		return ErrClosed
	}
	l.running = true
	defer func() { l.running = false }()
	for {
		now := l.now()
		if err := l.runDueTimers(now); err != nil {
			l.err = err
			return err
		}

		// Read the queue (tasks) once: the same answer decides both whether the loop
		// is alive and whether wait may block. A callback posted after this
		// read is not lost, because Post wakes the poller.
		hasTasks := l.hasTasks()
		if !l.aliveWith(hasTasks) {
			return nil
		}

		timeout := blockForever
		if len(l.timers) > 0 {
			timeout = max(l.timers[0].runAt-l.now(), 0)
		}
		if hasTasks {
			timeout = 0
		}

		// Not wrapped: wait returns a Callback's error from handleReady, and
		// Run must return that unchanged. kevent errors already carry
		// "poller wait:".
		if err := l.poller.wait(timeout, l.handleReady); err != nil {
			l.err = err
			return l.err
		}

		if err := l.runTasks(); err != nil {
			l.err = err
			return err
		}
	}
}
