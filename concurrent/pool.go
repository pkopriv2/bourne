package concurrent

import (
	"errors"
	"sync"
	"time"
)

var PoolClosedError = errors.New("Pool closed")

type Work func() interface{}

type WorkPool interface {
	Submit(chan<- interface{}, Work) error
	Close()
}

type pool struct {
	workers Stack
	work    chan workItem
	close   chan struct{}
	wait    sync.WaitGroup
}

func NewWorkPool(size int, depth int) WorkPool {
	p := &pool{
		workers: NewArrayStack(),
		work:    make(chan workItem, depth),
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

func (p *pool) Submit(res chan<- interface{}, w Work) error {
	select {
	case <-p.close:
		return PoolClosedError
	case p.work <- workItem{w, res}:
		return nil
	}
}

func (p *pool) Close() {
	close(p.close)
	p.wait.Wait()
}

func poolDispatch(b *pool) {
	defer b.wait.Done()
	var work workItem
	for {
		select {
		case <-b.close:
			return
		case work = <-b.work:
		}

		worker := poolSchedule(b)
		if worker == nil {
			return
		}

		worker.Run(work)
	}
}

func poolSchedule(b *pool) *worker {
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

type workItem struct {
	fn  Work
	ret chan<- interface{}
}

type worker struct {
	parent *pool
	queue  chan workItem
}

func newWorker(parent *pool) *worker {
	w := &worker{parent, make(chan workItem, 1)}
	go workerWork(w)
	return w
}

func (w *worker) Run(req workItem) {
	select {
	case <-w.parent.close:
	case w.queue <- req:
	}
}

func workerWork(w *worker) {
	defer w.parent.wait.Done()

	var order workItem
	for {
		select {
		case <-w.parent.close:
			return
		case order = <-w.queue:
		}

		// do NOT block the worker because the consumer isn't ready!
		order.ret <- order.fn()
		w.parent.ReturnWorker(w)
	}
}
