package concurrent

import (
	"errors"
	"sync"
	"time"
)

var PoolClosedError = errors.New("Pool closed")

type Response chan<-interface{}
type Work func(Response)

type WorkPool interface {
	Submit(Response, Work) error
	Close()
}

type pool struct {
	workers Stack
	work    chan submission
	close   chan struct{}
	wait    sync.WaitGroup
}

func NewWorkPool(size int, depth int) WorkPool {
	p := &pool{
		workers: NewArrayStack(),
		work:    make(chan submission, depth),
		close:   make(chan struct{})}

	p.wait.Add(1)
	go poolDispatch(p)

	p.wait.Add(size)
	for i := 0; i < size; i++ {
		w := newWorker(p)
		p.workers.Push(w)
	}

	return p
}

func (p *pool) ReturnWorker(w *worker) {
	p.workers.Push(w)
}

type submission struct {
	ret Response
	fn Work
}

func (p *pool) Submit(ret Response, fn Work) error {
	select {
	case <-p.close:
		return PoolClosedError
	case p.work <- submission{ret, fn}:
		return nil
	}
}

func (p *pool) Close() {
	close(p.close)
	p.wait.Wait()
}

func poolDispatch(b *pool) {
	defer b.wait.Done()
	var work submission
	for {
		select {
		case <-b.close:
			return
		case work = <-b.work:
		}

		worker := poolGetWorker(b)
		if worker == nil {
			return
		}

		worker.Run(work)
	}
}

func poolGetWorker(b *pool) *worker {
	// TODO: Replace with condition variable!
	timer := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-b.close:
			return nil
		case <-timer:
		}

		w := b.workers.Pop()
		if w != nil {
			return w.(*worker)
		}
	}
}

type worker struct {
	parent *pool
	queue  chan submission
}

func newWorker(parent *pool) *worker {
	w := &worker{parent, make(chan submission, 1)}
	go workerWork(w)
	return w
}

func (w *worker) Run(s submission) {
	select {
	case <-w.parent.close:
	case w.queue <- s:
	}
}

func workerWork(w *worker) {
	defer w.parent.wait.Done()

	var submission submission
	for {
		select {
		case <-w.parent.close:
			return
		case submission = <-w.queue:
		}

		// do NOT block the worker because the consumer isn't ready!
		submission.fn(submission.ret)
		w.parent.ReturnWorker(w)
	}
}
