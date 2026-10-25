package main

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"

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
	callbacks        map[int]EventCallback
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
		callbacks:        make(map[int]EventCallback),
		TimeoutCallbacks: make(TimeoutHeap, 0),
	}
	heap.Init(&el.TimeoutCallbacks)
	// Register the pipe's read end with kqueue
	err = el.Register(&Socket{Fd: readFd}, unix.EVFILT_READ, func(fd int, filter int16) {
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

	el.callbacks[socket.Fd] = cb
	return nil
}

func (el *EventLoop) Unregister(socket *Socket) error {
	if socket.Closed {
		return nil
	}
	el.cbMutex.Lock()
	defer el.cbMutex.Unlock()

	ev := unix.Kevent_t{
		Ident:  uint64(socket.Fd),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_DELETE,
	}
	_, _ = unix.Kevent(el.kq, []unix.Kevent_t{ev}, nil, nil)
	delete(el.callbacks, socket.Fd)
	err := socket.Close()
	if err != nil {
		return fmt.Errorf("socket.close: %v", err)
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
	return el.TimeoutCallbacks.Len() > 0
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
		sleepDuration := time.Duration(0)
		if len(el.TimeoutCallbacks) > 0 {
			nearestTimerCB := el.TimeoutCallbacks[0]
			if nearestTimerCB.RunAt > now {
				sleepDuration = time.Duration(nearestTimerCB.RunAt - now)
			}
		}

		// Network epoll
		kqueueTimeout := unix.NsecToTimespec(sleepDuration.Nanoseconds())
		n, err := unix.Kevent(el.kq, nil, events, &kqueueTimeout)
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
			cb, exists := el.callbacks[fd]
			el.cbMutex.RUnlock()

			if exists {
				cb(fd, filter)
			}
		}
	}
}
