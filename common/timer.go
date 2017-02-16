package common

import "time"

func Elapsed(log Logger, fmt string, then time.Time) {
	log.Info("Elapsed: %v: %v", fmt, time.Now().Sub(then))
}

// FIXME: return the sub control in order to be able to cancel/cleanup
func NewTimer(ctrl Control, dur time.Duration) Control {
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

	return sub
}
