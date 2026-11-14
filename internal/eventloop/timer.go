package eventloop

import (
	"container/heap"
	"time"
)

const maxTimeout = (1<<31 - 1) * time.Millisecond

// Timer represents a timer that can be canceled.
type Timer struct {
	// TODO: make unexport ID
	ID       int64
	loop     *Loop
	schedule int64
	index    int
	timeout  time.Duration
	runAt    time.Duration
	repeat   bool
	canceled bool
	callback func() error
}

// timerHeap orders timers by runAt then schedule, soonest first.
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
	*h = append(*h, item)
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

// AddInterval adds a timer that will run repeatedly at the specified interval.
// It returns a Timer that can be passed to CancelTimer. It clamps interval values
// less than 1 millisecond or greater than 1<<31 - 1 milliseconds to 1 millisecond.
// It reschedules from Now().
func (l *Loop) AddInterval(interval time.Duration, callback func() error) *Timer {
	return l.addTimer(interval, true, callback)
}

// AddTimeout schedules callback to run once after timeout. It returns
// a Timer that can be passed to CancelTimer. It clamps timeout values less than
// 1 millisecond or greater than 1<<31 - 1 milliseconds to 1 millisecond.
func (l *Loop) AddTimeout(timeout time.Duration, callback func() error) *Timer {
	return l.addTimer(timeout, false, callback)
}

func (l *Loop) addTimer(timeout time.Duration, repeat bool, callback func() error) *Timer {
	timeout = max(timeout, time.Millisecond)
	if timeout > maxTimeout {
		timeout = time.Millisecond
	}
	l.nextTimerID++
	timer := &Timer{
		ID:       l.nextTimerID,
		loop:     l,
		timeout:  timeout,
		callback: callback,
		repeat:   repeat,
	}
	l.push(timer)
	return timer
}

func (l *Loop) push(timer *Timer) {
	timer.schedule = l.nextSeq
	l.nextSeq++
	timer.runAt = l.Now() + timer.timeout
	heap.Push(&l.timers, timer)
}

// CancelTimer cancels a timer. It is safe to call CancelTimer multiple times.
// It is safe to call CancelTimer from inside a callback. A nil timer or one
// from another loop is ignored.
func (l *Loop) CancelTimer(timer *Timer) {
	if timer == nil || timer.loop != l || timer.canceled {
		return
	}
	timer.canceled = true
	if timer.index != -1 {
		heap.Remove(&l.timers, timer.index)
	}
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
		if timer.repeat && !timer.canceled {
			l.push(timer)
		}
	}
	return nil
}
