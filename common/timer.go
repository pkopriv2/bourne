package common

import "time"

// Implements a very simple request/response object.

func NewTimer(ctrl Control, dur time.Duration) <-chan struct{} {
	sub := ctrl.Sub()

	timer := time.NewTimer(dur)
	go func() {
		defer sub.Close()

		select {
		case <-sub.Closed():
			return
		case <-timer.C:
			return
		}
	}()

	return sub.Closed()
}
