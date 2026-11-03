// Package eventloop runs timers and file-descriptor callbacks on a single
// thread, like libuv does for node. The OS-specific polling lives in
// eventloop_<os>.go behind the poller type.
package eventloop

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Interest is the kind of readiness a callback waits for.
type Interest int

const (
	Readable Interest = iota
	Writable
)

// Callback handles a ready fd. A non-nil error stops the loop; Run returns it.
type Callback func(fd int, interest Interest) error
type Task func()

type Loop struct {
	poller      *poller
	timers      timerHeap
	callbacks   map[int]map[Interest]Callback
	cbMutex     sync.RWMutex
	pendingTask []Task
	taskMutex   sync.Mutex
}

func New() (*Loop, error) {
	p, err := newPoller()
	if err != nil {
		return nil, err
	}
	return &Loop{
		poller:    p,
		callbacks: make(map[int]map[Interest]Callback),
	}, nil
}

// Register makes fd non-blocking and calls cb whenever it is ready for interest.
func (l *Loop) Register(fd int, interest Interest, cb Callback) error {
	l.cbMutex.Lock()
	defer l.cbMutex.Unlock()

	if err := unix.SetNonblock(fd, true); err != nil {
		return err
	}
	if err := l.poller.add(fd, interest); err != nil {
		return err
	}

	if l.callbacks[fd] == nil {
		l.callbacks[fd] = make(map[Interest]Callback)
	}
	l.callbacks[fd][interest] = cb
	return nil
}

// Unregister drops every callback for fd. The caller closes fd, which also
// removes it from the poller.
func (l *Loop) Unregister(fd int) {
	l.cbMutex.Lock()
	defer l.cbMutex.Unlock()
	delete(l.callbacks, fd)
}

// RemoveInterest stops watching one interest for fd, leaving its other
// interests and the fd itself untouched.
func (l *Loop) RemoveInterest(fd int, interest Interest) error {
	l.cbMutex.Lock()
	defer l.cbMutex.Unlock()

	if err := l.poller.remove(fd, interest); err != nil {
		return err
	}

	interests := l.callbacks[fd]
	delete(interests, interest) // no-op if interests is nil
	if len(interests) == 0 {
		delete(l.callbacks, fd)
	}
	return nil
}

// QueueTask runs task on the loop thread. It is safe to call from any goroutine.
func (l *Loop) QueueTask(task Task) {
	l.taskMutex.Lock()
	l.pendingTask = append(l.pendingTask, task)
	l.taskMutex.Unlock()

	l.poller.wake()
}

func (l *Loop) executePendingTasks() {
	l.taskMutex.Lock()
	tasks := l.pendingTask
	l.pendingTask = nil
	l.taskMutex.Unlock()

	for _, task := range tasks {
		task()
	}
}

func (l *Loop) isAlive() bool {
	l.cbMutex.RLock()
	defer l.cbMutex.RUnlock()
	return l.timers.Len() > 0 || len(l.callbacks) > 0
}

// Run processes timers and I/O until nothing is left to wait for. It stops early
// and returns the error if a callback fails, such as an uncaught JS exception.
func (l *Loop) Run() error {
	fmt.Printf("💚 tinynode event loop (%s) started...\n", pollerName)

	for l.isAlive() {
		now := Now()
		if err := l.runDueTimers(now); err != nil {
			return err
		}

		// A timer callback may have closed the last socket; with nothing left
		// to wait for, waiting with no timeout below would block forever.
		if !l.isAlive() {
			break
		}

		timeout := time.Duration(-1) // wait until an fd is ready
		if l.timers.Len() > 0 {
			timeout = max(l.timers[0].RunAt-now, 0)
		}
		if err := l.poller.wait(timeout, l.dispatch); err != nil {
			return err
		}
		l.executePendingTasks()
	}
	return nil
}

func (l *Loop) dispatch(fd int, interest Interest) error {
	l.cbMutex.RLock()
	cb, exists := l.callbacks[fd][interest]
	l.cbMutex.RUnlock()

	if !exists {
		return nil
	}
	return cb(fd, interest)
}
