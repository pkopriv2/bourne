package common

// Implements a very simple request/response object.
type Request struct {
	body   interface{}
	resp   chan interface{}
	fail   chan error
	cancel chan struct{}
}

func NewRequest(val interface{}) *Request {
	return &Request{val, make(chan interface{}, 1), make(chan error, 1), make(chan struct{})}
}

func (r *Request) Body() interface{} {
	return r.body
}

func (r *Request) Ack(val interface{}) {
	r.resp <- val
}

func (r *Request) Cancel() {
	close(r.cancel)
}

func (r *Request) Canceled() <-chan struct{} {
	return r.cancel
}

func (r *Request) Acked() <-chan interface{} {
	return r.resp
}

func (r *Request) Failed() <-chan error {
	return r.fail
}

func (r *Request) Fail(err error) {
	r.fail <- err
}

func (r *Request) Return(val interface{}, err error) {
	if err != nil {
		r.fail <- err
	} else {
		r.resp <- val
	}
}

func (r *Request) Response() (interface{}, error) {
	select {
	case err := <-r.fail:
		return nil, err
	case val := <-r.resp:
		return val, nil
	}
}
