package main

import "time"

var start = time.Now()

func GetProcessElapsedTime() time.Duration {
	return time.Since(start)
}
