package concurrent

import "sync"

type Wait interface {
	Inc()
	Dec()
	Wait() <-chan struct{}
}

type wait struct {
	refs int
	cond *sync.Cond
}

func NewWait() Wait {
	return &wait{cond: sync.NewCond(&sync.Mutex{})}
}

func (w *wait) Dec() {
	w.cond.L.Lock()
	defer w.cond.Broadcast()
	defer w.cond.L.Unlock()
	if w.refs == 0 {
		panic("Negative reference counter")
	}

	w.refs--
}

func (w *wait) Inc() {
	w.cond.L.Lock()
	defer w.cond.L.Unlock()
	w.refs++
}

func (w *wait) Wait() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		w.cond.L.Lock()
		for w.refs > 0 {
			w.cond.Wait()
		}
		close(done)
		w.cond.L.Unlock()
	}()
	return done
}
