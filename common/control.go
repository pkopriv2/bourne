package common

type Control interface {
	Fail(error)
	Close()
	Closed() <-chan struct{}
	IsClosed() bool
	Failure() error
	Sub() Control
}

type control struct {
	closed chan struct{}
	closer chan struct{}
	failure error
}

func NewControl(parent *control) *control {
	l := &control{
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1),
	}

	if parent != nil {
		go func() {
			defer l.Close()
			select {
			case <-parent.Closed():
				return
			case <-l.closed:
				return
			}
		}()
	}

	return l
}

func (c *control) Fail(cause error) {
	select {
	case <-c.closed:
		return
	case c.closer <- struct{}{}:
	}

	c.failure = cause
	close(c.closed)
}

func (c *control) Close() {
	c.Fail(nil)
}

func (c *control) Closed() <-chan struct{} {
	return c.closed
}

func (c *control) IsClosed() bool {
	select {
	default:
		return false
	case <-c.closed:
		return true
	}
}

func (c *control) Failure() error {
	<-c.closed
	return c.failure
}

func (c *control) Sub() Control {
	return NewControl(c)
}
