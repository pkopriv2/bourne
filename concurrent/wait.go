package concurrent

import "sync"

type Wait interface {
	Inc()
	Dec()
	Wait() <-chan struct{}
}

type wait struct {
	inner sync.WaitGroup
}

func NewWait() Wait {
	return &wait{}
}

func (w *wait) Dec() {
	w.inner.Done()
}

func (w *wait) Inc() {
	w.inner.Add(1)
}

func (w *wait) Wait() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		w.inner.Wait()
		close(done)
	}()
	return done
}
