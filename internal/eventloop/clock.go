package eventloop

import "time"

var start = time.Now()

// Now returns the time since the process started. Timers are scheduled on this clock.
func Now() time.Duration {
	return time.Since(start)
}
