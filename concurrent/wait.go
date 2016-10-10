package concurrent

import "sync"

type Wait interface {
	Add()
	Done()
	Wait() <- chan struct{}
}

type wait struct {
	inner sync.WaitGroup
}

func NewWait() Wait {
	return &wait{}
}

func (w *wait) Done() {
	w.inner.Done()
}

func (w *wait) Add() {
	w.inner.Add(1)
}

func (w *wait) Wait() <-chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		w.inner.Wait()
		done <- struct{}{}
	}()
	return done
}
