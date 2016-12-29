package concurrent

import (
	"fmt"
	"time"
)

type TimeoutError struct {
	timeout time.Duration
	msg     string
}

func NewTimeoutError(timeout time.Duration, msg string) TimeoutError {
	return TimeoutError{timeout, msg}
}

func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", t.timeout, t.msg)
}

func NewBreaker(dur time.Duration, fn func()) (<-chan struct{}, <-chan error) {
	inner := make(chan struct{}, 1)
	timer := time.NewTimer(dur)
	go func() {
		fn()
		inner<-struct{}{}
	}()

	timeout := make(chan error, 1)
	outer := make(chan struct{}, 1)
	go func() {
		select {
		case i := <-inner:
			outer<-i
			timer.Stop()
		case <-timer.C:
			timeout<-NewTimeoutError(dur, "Concurrent:Breaker")
		}
	}()
	return outer, timeout
}
