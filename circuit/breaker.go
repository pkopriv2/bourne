package circuit

import (
	"fmt"
	"time"
)

type TimeoutError struct {
	timeout time.Duration
	msg     string
}

func NewTimeoutError(timeout time.Duration, msg string) *TimeoutError {
	return &TimeoutError{timeout, msg}
}

func (t *TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", t.timeout, t.msg)
}

func NewBreaker(dur time.Duration, fn func()) (<-chan struct{}, <-chan time.Time) {
	timer := time.After(dur)
	done := make(chan struct{}, 1)

	go func() {
		fn()
		done <- struct{}{}
	}()

	return done, timer
}
