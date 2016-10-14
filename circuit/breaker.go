package circuit

import "time"

func NewBreaker(dur time.Duration, fn func()) (<-chan struct{}, <-chan time.Time) {
	timer := time.After(dur)
	done := make(chan struct{},1)

	go func() {
		fn()
		done <- struct{}{}
	}()

	return done, timer
}
