package main

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrSocketClosed = errors.New("socket is closed")

type TimeoutHeap []*TimeoutCallback

func (h *TimeoutHeap) Len() int           { return len(*h) }
func (h *TimeoutHeap) Less(i, j int) bool { return (*h)[i].RunAt < (*h)[j].RunAt }
func (h *TimeoutHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *TimeoutHeap) Push(x any)         { *h = append(*h, x.(*TimeoutCallback)) }
func (h *TimeoutHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type EventCallback func(fd int, filter int16)
type Task func()

type EventLoop struct {
	TimeoutCallbacks TimeoutHeap
	kq               int
	wakeReadFd       int
	wakeWriteFd      int
	callbacks        map[int]map[int16]EventCallback
	cbMutex          sync.RWMutex
	pendingTask      []Task
	taskMutex        sync.Mutex
}

func NewEventLoop() (*EventLoop, error) {
	// Create kqueue instance
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue error: %v", err)
	}

	// Create a pipe for thread notifications (macOS alternative to eventfd)
	var p [2]int
	if err := unix.Pipe(p[:]); err != nil {
		unix.Close(kq)
		return nil, fmt.Errorf("pipe error: %v", err)
	}
	readFd, writeFd := p[0], p[1]

	el := &EventLoop{
		kq:               kq,
		wakeReadFd:       readFd,
		wakeWriteFd:      writeFd,
		callbacks:        make(map[int]map[int16]EventCallback),
		TimeoutCallbacks: make(TimeoutHeap, 0),
	}
	heap.Init(&el.TimeoutCallbacks)
	// Register the pipe's read end with kqueue
	err = el.Register(NewSocket(el, readFd), unix.EVFILT_READ, func(fd int, filter int16) {
		var buf [8]byte
		_, _ = unix.Read(fd, buf[:]) // Clear the pipe notification
		el.executePendingTasks()
	})
	if err != nil {
		unix.Close(kq)
		unix.Close(readFd)
		unix.Close(writeFd)
		return nil, err
	}

	return el, nil
}

func (el *EventLoop) Register(socket *Socket, filter int16, cb EventCallback) error {
	if socket.Closed {
		return ErrSocketClosed
	}
	el.cbMutex.Lock()
	defer el.cbMutex.Unlock()

	if err := unix.SetNonblock(socket.Fd, true); err != nil {
		return err
	}

	// Register event with kqueue
	ev := unix.Kevent_t{
		Ident:  uint64(socket.Fd),
		Filter: filter,
		Flags:  unix.EV_ADD | unix.EV_ENABLE,
	}

	_, err := unix.Kevent(el.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil {
		return err
	}

	if el.callbacks[socket.Fd] == nil {
		el.callbacks[socket.Fd] = make(map[int16]EventCallback)
	}
	el.callbacks[socket.Fd][filter] = cb
	return nil
}

func (el *EventLoop) Unregister(socket *Socket) error {
	if socket.Closed {
		return nil
	}
	el.cbMutex.Lock()
	defer el.cbMutex.Unlock()

	delete(el.callbacks, socket.Fd)

	err := socket.Destroy()
	if err != nil {
		return err
	}
	return nil
}

// RemoveFilter stops watching one filter for socket, leaving its other
// filters and the fd itself untouched.
func (el *EventLoop) RemoveFilter(socket *Socket, filter int16) error {
	if socket.Closed {
		return nil
	}
	el.cbMutex.Lock()
	defer el.cbMutex.Unlock()

	ev := unix.Kevent_t{
		Ident:  uint64(socket.Fd),
		Filter: filter,
		Flags:  unix.EV_DELETE,
	}
	_, err := unix.Kevent(el.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}

	filters := el.callbacks[socket.Fd]
	delete(filters, filter) // no-op if filters is nil
	if len(filters) == 0 {
		delete(el.callbacks, socket.Fd)
	}
	return nil
}

func (el *EventLoop) QueueTask(task Task) {
	el.taskMutex.Lock()
	el.pendingTask = append(el.pendingTask, task)
	el.taskMutex.Unlock()

	// Write to the pipe write end to wake up kevent_wait
	_, _ = unix.Write(el.wakeWriteFd, []byte{1})
}

func (el *EventLoop) executePendingTasks() {
	el.taskMutex.Lock()
	tasks := el.pendingTask
	el.pendingTask = nil
	el.taskMutex.Unlock()

	for _, task := range tasks {
		task()
	}
}

func (el *EventLoop) AddTimeoutCallback(callback *TimeoutCallback) {
	heap.Push(&el.TimeoutCallbacks, callback)
}

func (el *EventLoop) isAlive() bool {
	el.cbMutex.RLock()
	defer el.cbMutex.RUnlock()
	// -1 for the internal wake-up pipe, which is always registered.
	return el.TimeoutCallbacks.Len() > 0 || len(el.callbacks)-1 > 0
}

func (el *EventLoop) Run() {
	events := make([]unix.Kevent_t, 64)
	fmt.Println("💚 Node.go Event Loop (kqueue/macOS) started...")

	for el.isAlive() {
		now := GetProcessElapsedTime()
		// Run all Timer callbacks that are due to run
		for el.TimeoutCallbacks.Len() > 0 {
			timerCB := el.TimeoutCallbacks[0]
			if timerCB.RunAt <= now {
				heap.Pop(&el.TimeoutCallbacks)
				timerCB.Callback()
				if timerCB.Repeat {
					timerCB.RunAt += timerCB.Timeout
					heap.Push(&el.TimeoutCallbacks, timerCB)
				}
			} else {
				break
			}
		}

		// A timer callback may have closed the last socket; with nothing left
		// to wait for, a nil timeout below would block forever.
		if !el.isAlive() {
			break
		}

		// Network epoll
		var timeout *unix.Timespec
		if len(el.TimeoutCallbacks) > 0 {
			wait := max(el.TimeoutCallbacks[0].RunAt-now, 0)
			ts := unix.NsecToTimespec(wait.Nanoseconds())
			timeout = &ts
		}

		n, err := unix.Kevent(el.kq, nil, events, timeout)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			fmt.Printf("Kevent critical error: %v\n", err)
			break
		}

		for i := range n {
			fd := int(events[i].Ident)
			filter := events[i].Filter

			el.cbMutex.RLock()
			cb, exists := el.callbacks[fd][filter]
			el.cbMutex.RUnlock()

			if exists {
				cb(fd, filter)
			}
		}
	}
}
