package eventloop

import (
	"container/heap"
	"time"
)

// Timer runs Callback once Loop.Now reaches RunAt, and every Timeout after that if Repeat is set.
type Timer struct {
	ID        int
	CreatedAt time.Duration
	Timeout   time.Duration
	Repeat    bool
	RunAt     time.Duration
	Callback  func() error
}

// timerHeap orders timers by RunAt, soonest first.
type timerHeap []*Timer

func (h *timerHeap) Len() int           { return len(*h) }
func (h *timerHeap) Less(i, j int) bool { return (*h)[i].RunAt < (*h)[j].RunAt }
func (h *timerHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *timerHeap) Push(x any)         { *h = append(*h, x.(*Timer)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (l *Loop) AddTimer(timer *Timer) {
	heap.Push(&l.timers, timer)
}

// runDueTimers runs every timer whose RunAt has passed, stopping at the first error.
func (l *Loop) runDueTimers(now time.Duration) error {
	for l.timers.Len() > 0 {
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
