package common

type LifeCycle interface {
	Fail(error)
	Close()
	Closed() <-chan struct{}
	Failure() error
}

type lifecycle struct {
	parent LifeCycle
	closed chan struct{}
	closer chan struct{}
	failure error
}

func NewLifeCycle(parent LifeCycle) *lifecycle {
	l := &lifecycle{
		parent: parent,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	go func() {
		defer l.Close()
		select {
			case <-l.parent.Closed():
				return
			case <-l.closed:
				return
		}
	}()

	return l
}

func (c *lifecycle) Fail(cause error) {
	select {
	case <-c.closed:
		return
	case c.closer <- struct{}{}:
	}

	c.failure = cause
	close(c.closed)
}

func (c *lifecycle) Close() {
	c.Fail(nil)
}

func (e *lifecycle) Closed() <-chan struct{} {
	return e.closed
}

func (e *lifecycle) Failure() error {
	<-e.closed
	return e.failure
}
