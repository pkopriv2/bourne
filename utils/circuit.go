package utils

import "time"

func NewCircuitBreaker(dur time.Duration, fn func()) (<-chan bool, <-chan time.Time) {
	timer := time.After(dur)
	done := make(chan bool)

	go func() {
		fn()
		done <- true
	}()

	return done, timer
}
