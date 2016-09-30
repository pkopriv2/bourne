package utils

import "time"

func NewCircuitBreaker(dur time.Duration, fn func()) (<-chan struct{}, <-chan time.Time) {
	timer := time.After(dur)
	done := make(chan struct{})

	go func() {
		fn()
		done <- struct{}{}
	}()

	return done, timer
}
