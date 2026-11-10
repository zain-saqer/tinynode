package eventloop

import (
	"container/heap"
	"time"
)

type Loop struct {
	timers timerHeap
	now    func() time.Duration
}

func (l *Loop) Now() time.Duration {
	return l.now()
}

func New() (*Loop, error) {
	loop := &Loop{
		now: sinceStart,
	}
	heap.Init(&loop.timers)
	return loop, nil
}

func (l *Loop) isAlive() bool {
	return len(l.timers) > 0
}

func (l *Loop) Run() error {
	for l.isAlive() {
		now := l.now()
		if err := l.runDueTimers(now); err != nil {
			return err
		}
	}
	return nil
}
