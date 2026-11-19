package eventloop

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrAlreadyDone is returned by Pending.Done when Done was already called.
var ErrAlreadyDone = errors.New("pending already done")

var panicNilCallback = "eventloop: nil callback"

// pending is the state shared by all copies of a Pending.
type pending struct {
	released atomic.Bool // set by the first Done; later calls fail
	l        *Loop
}

// Pending keeps a Loop alive while work that will report back to it is still
// running, for example on another goroutine. Only Hold creates a valid
// Pending; the zero value must not be used. Copies of a Pending refer to the
// same hold.
type Pending struct {
	r *pending
}

// Done posts cb to the Loop and releases the hold once cb has run. Done may be
// called from any goroutine, but only once: later calls return ErrAlreadyDone
// and do not post cb. If the Loop is closed, Done returns an error wrapping
// ErrClosed.
func (p Pending) Done(cb Callback) error {
	if p.r == nil {
		panic("eventloop: Done on zero Pending")
	}
	if cb == nil {
		panic(panicNilCallback)
	}
	if !p.r.released.CompareAndSwap(false, true) {
		return fmt.Errorf("eventloop pending done: %w", ErrAlreadyDone)
	}
	wrapper := func() error {
		err := cb()
		p.r.l.holds--
		return err
	}
	if err := p.r.l.Post(wrapper); err != nil {
		return fmt.Errorf("eventloop pending done: %w", err)
	}

	return nil
}

// Hold returns a Pending that keeps the Loop alive until its Done is called.
// Done sends the result back to the Loop as a Callback. Hold must be called on
// the goroutine running Run (before Run, or from a Callback).
func (l *Loop) Hold() Pending {
	l.holds++
	return Pending{
		&pending{
			l: l,
		},
	}
}

// Post queues cb to run on the goroutine running Run, and wakes Run if it is
// blocked waiting. Post may be called from any goroutine, including from a
// Callback; a callback posted while callbacks are running runs in the next
// iteration. Post returns ErrClosed if the Loop is closed.
//
// A queued callback keeps the Loop alive, but nothing keeps it alive while
// another goroutine is still working on something it will Post later: Run may
// return first. To prevent that, take a Pending with Hold before starting the
// work, and report the result with Pending.Done.
//
// The queue has no size limit: if other goroutines post faster than the loop
// runs callbacks, memory grows without bound. Post never blocks to apply
// backpressure, the same as Node's setImmediate.
func (l *Loop) Post(cb Callback) error {
	if cb == nil {
		panic(panicNilCallback)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if len(l.tasks) == 0 {
		if err := l.poller.wake(); err != nil {
			return fmt.Errorf("eventloop post: %w", err)
		}
	}
	l.tasks = append(l.tasks, cb)
	return nil
}

// runTasks runs the callbacks posted so far, in the order they were posted. It
// swaps the queue out under the lock and runs the callbacks without it, so a
// callback can call Post without deadlocking; callbacks posted meanwhile wait
// for the next call. It stops at the first error, and the rest of the batch is
// dropped.
func (l *Loop) runTasks() error {
	l.mu.Lock()
	batch := l.tasks
	l.tasks = nil
	l.mu.Unlock()
	if len(batch) > 0 {
		for _, task := range batch {
			if err := task(); err != nil {
				return err
			}
		}
	}
	return nil
}
