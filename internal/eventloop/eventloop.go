package eventloop

import (
	"container/heap"
	"time"
)

type Loop struct {
	timers      timerHeap
	now         func() time.Duration
	nextTimerID int64
	nextSeq     int64
	err         error
}

func (l *Loop) Now() time.Duration {
	return l.now()
}

func New() (*Loop, error) {
	loop := &Loop{
		now:         sinceStart,
		nextTimerID: 1,
		nextSeq:     1,
	}
	heap.Init(&loop.timers)
	return loop, nil
}

func (l *Loop) isAlive() bool {
	return len(l.timers) > 0
}

// Run runs the loop until no timers are left. After Run returns an error, the Loop must not be used again.
func (l *Loop) Run() error {
	// after Run returns an error, the Loop must not be used again
	if l.err != nil {
		return l.err
	}
	for l.isAlive() {
		now := l.now()
		if err := l.runDueTimers(now); err != nil {
			l.err = err
			return err
		}

		var timeout time.Duration
		if len(l.timers) > 0 {
			timeout = max(l.timers[0].runAt-l.Now(), 0)
		}

		// TODO: to be replaced with kqueue poller wait
		time.Sleep(timeout)
	}
	return nil
}
