package eventloop

import "time"

var start = time.Now()

// sinceStart returns the time since the process started. It is the loop's
// default clock; see Loop.Now.
func sinceStart() time.Duration {
	return time.Since(start)
}
