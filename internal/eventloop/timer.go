package eventloop

import (
	"container/heap"
	"time"
)

const maxTimeout = (1<<31 - 1) * time.Millisecond

type Timer struct {
	// TODO: make unexport ID
	ID       int64
	schedule int64
	index    int
	timeout  time.Duration
	runAt    time.Duration
	repeat   bool
	canceled bool
	callback func() error
}

// timerHeap orders timers by RunAt, soonest first.
type timerHeap []*Timer

func (h *timerHeap) Len() int { return len(*h) }
func (h *timerHeap) Less(i, j int) bool {
	if (*h)[i].runAt == (*h)[j].runAt {
		return (*h)[i].schedule < (*h)[j].schedule
	}
	return (*h)[i].runAt < (*h)[j].runAt
}
func (h *timerHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
	(*h)[i].index = i
	(*h)[j].index = j
}
func (h *timerHeap) Push(x any) {
	n := len(*h)
	item := x.(*Timer)
	item.index = n
	*h = append(*h, x.(*Timer))
}
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	x.index = -1
	*h = old[0 : n-1]
	return x
}

// TODO: extract the duplicate code from AddInterval and AddTimeout

func (l *Loop) AddInterval(timeout time.Duration, callback func() error) *Timer {
	timeout = max(timeout, time.Millisecond)
	if timeout > maxTimeout {
		timeout = time.Millisecond
	}
	timer := &Timer{
		ID:       l.nextTimerID,
		schedule: l.nextSeq,
		timeout:  timeout,
		runAt:    l.Now() + timeout,
		callback: callback,
		repeat:   true,
		canceled: false,
	}
	l.nextTimerID++
	l.nextSeq++
	heap.Push(&l.timers, timer)
	return timer
}

func (l *Loop) AddTimeout(timeout time.Duration, callback func() error) *Timer {
	timeout = max(timeout, time.Millisecond)
	if timeout > maxTimeout {
		timeout = time.Millisecond
	}
	timer := &Timer{
		ID:       l.nextTimerID,
		schedule: l.nextSeq,
		timeout:  timeout,
		runAt:    l.Now() + timeout,
		callback: callback,
		repeat:   false,
		canceled: false,
	}
	l.nextTimerID++
	l.nextSeq++
	heap.Push(&l.timers, timer)
	return timer
}

func (l *Loop) CancelTimer(timer *Timer) {
	if timer.canceled {
		return
	}
	timer.canceled = true
	if timer.index != -1 {
		heap.Remove(&l.timers, timer.index)
	}
}

func (l *Loop) reschedule(timer *Timer) {
	if timer.canceled {
		return
	}
	timer.schedule = l.nextSeq
	l.nextSeq++
	timer.runAt = l.Now() + timer.timeout
	heap.Push(&l.timers, timer)
}

func (l *Loop) runDueTimers(now time.Duration) error {
	timers := make([]*Timer, 0)
	for len(l.timers) > 0 && l.timers[0].runAt <= now {
		timer := heap.Pop(&l.timers).(*Timer)
		timers = append(timers, timer)
	}
	for _, timer := range timers {
		if timer.canceled {
			continue
		}
		if err := timer.callback(); err != nil {
			// the event loop never runs again after returning an error.
			// it is ok to lose the timers that didn't run.
			return err
		}
		if timer.repeat {
			l.reschedule(timer)
		}
	}
	return nil
}
