package eventloop

import (
	"container/heap"
	"time"
)

type Timer struct {
	ID       int
	Timeout  time.Duration
	RunAt    time.Duration
	AddedAt  time.Duration
	Repeat   bool
	Callback func() error
}

// timerHeap orders timers by RunAt, soonest first.
type timerHeap []*Timer

func (h *timerHeap) Len() int { return len(*h) }
func (h *timerHeap) Less(i, j int) bool {
	if (*h)[i].RunAt == (*h)[j].RunAt {
		return (*h)[i].AddedAt < (*h)[j].AddedAt
	}
	return (*h)[i].RunAt < (*h)[j].RunAt
}
func (h *timerHeap) Swap(i, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *timerHeap) Push(x any)    { *h = append(*h, x.(*Timer)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (l *Loop) AddTimer(timer *Timer) {
	timer.AddedAt = l.Now()
	heap.Push(&l.timers, timer)
}

func (l *Loop) runDueTimers(now time.Duration) error {
	for len(l.timers) > 0 {
		timer := l.timers[0]
		if timer.RunAt > now {
			return nil
		}
		heap.Pop(&l.timers)
		if err := timer.Callback(); err != nil {
			return err
		}
		if timer.Repeat {
			timer.RunAt += timer.Timeout
			heap.Push(&l.timers, timer)
		}
	}
	return nil
}
