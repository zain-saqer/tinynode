package eventloop

import (
	"container/heap"
	"time"
)

const maxTimeout = (1<<31 - 1) * time.Millisecond

type timer struct {
	id       int64
	loop     *Loop
	schedule int64
	index    int
	timeout  time.Duration
	runAt    time.Duration
	repeat   bool
	canceled bool
	callback Callback
}

// Timer represents a timer that can be canceled. Copies of a Timer refer to
// the same timer.
type Timer struct {
	t *timer
}

// ID returns the ID of the timer. IDs are unique within a loop.
func (h *Timer) ID() int64 {
	if h == nil || h.t == nil {
		return 0
	}
	return h.t.id
}

// timerHeap orders timers by runAt then schedule, soonest first.
type timerHeap []*timer

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
	item := x.(*timer)
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

// AddInterval schedules callback to run every interval until the timer is
// canceled. After each run, the next run is scheduled interval after Now(), so
// a slow callback delays later runs instead of making them run back to back.
// Interval values less than 1 millisecond or greater than 1<<31 - 1
// milliseconds are clamped to 1 millisecond. It returns a Timer that can be
// passed to CancelTimer. AddInterval panics if callback is nil.
func (l *Loop) AddInterval(interval time.Duration, callback Callback) *Timer {
	return l.addTimer(interval, true, callback)
}

// AddTimeout schedules callback to run once after timeout. Timeout values less
// than 1 millisecond or greater than 1<<31 - 1 milliseconds are clamped to 1
// millisecond. It returns a Timer that can be passed to CancelTimer.
// AddTimeout panics if callback is nil.
func (l *Loop) AddTimeout(timeout time.Duration, callback Callback) *Timer {
	return l.addTimer(timeout, false, callback)
}

func (l *Loop) addTimer(timeout time.Duration, repeat bool, callback Callback) *Timer {
	if callback == nil {
		panic("eventloop: nil timer callback")
	}
	timeout = max(timeout, time.Millisecond)
	if timeout > maxTimeout {
		timeout = time.Millisecond
	}
	l.nextTimerID++
	tm := &timer{
		id:       l.nextTimerID,
		loop:     l,
		timeout:  timeout,
		callback: callback,
		repeat:   repeat,
	}
	l.push(tm)
	return &Timer{tm}
}

func (l *Loop) push(tm *timer) {
	tm.schedule = l.nextSeq
	l.nextSeq++
	tm.runAt = l.Now() + tm.timeout
	heap.Push(&l.timers, tm)
}

// CancelTimer cancels a timer. It is safe to call CancelTimer multiple times.
// It is safe to call CancelTimer from inside a Callback. A nil timer or one
// from another loop is ignored.
func (l *Loop) CancelTimer(h *Timer) {
	if h == nil || h.t == nil {
		return
	}
	tm := h.t
	if tm.loop != l || tm.canceled {
		return
	}
	tm.canceled = true
	if tm.index != -1 {
		heap.Remove(&l.timers, tm.index)
	}
}

func (l *Loop) runDueTimers(now time.Duration) error {
	l.timerBatch = l.timerBatch[:0]
	defer func() { clear(l.timerBatch) }()
	for len(l.timers) > 0 && l.timers[0].runAt <= now {
		tm := heap.Pop(&l.timers).(*timer)
		l.timerBatch = append(l.timerBatch, tm)
	}
	for _, tm := range l.timerBatch {
		if tm.canceled {
			continue
		}
		if err := tm.callback(); err != nil {
			// the event loop never runs again after returning an error.
			// it is ok to lose the timers that didn't run.
			return err
		}
		if tm.repeat && !tm.canceled {
			l.push(tm)
		}
	}
	return nil
}
