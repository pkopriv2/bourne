package net

import (
	"encoding/gob"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/enc"
)

var (
	ServerError       = errors.New("NET:SERVER:ERROR")
	ServerClosedError = errors.New("NET:SERVER:CLOSED")
)

// A very simple request/response server.

type Handler func(Request) Response

type Server interface {
	io.Closer
	Client(...ClientOptionsFn) (Client, error)
}

type Client interface {
	io.Closer
	Send(Request) (Response, error)
}

type Request interface {
	enc.Writable

	Type() int
	Body() enc.Message
}

type Response interface {
	enc.Writable

	Error() error
	Body() enc.Message
}

type ServerOptions struct {
	WorkPoolSize int

	SendTimeout time.Duration
	RecvTimeout time.Duration
}

type ClientOptions struct {
	SendTimeout time.Duration
	RecvTimeout time.Duration
}

type ServerOptionsFn func(*ServerOptions)

type ClientOptionsFn func(*ClientOptions)

func NewRequest(typ int, body enc.Message) Request {
	return &request{typ, body}
}

func NewSuccessResponse(body enc.Message) Response {
	return &response{body: body}
}

func NewErrorResponse(err error) Response {
	return &response{err: err}
}

func DefaultServerOptions() *ServerOptions {
	return &ServerOptions{10, 5 * time.Second, 5 * time.Second}
}

func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{5 * time.Second, 5 * time.Second}
}

func NewClient(conn Connection, fns ...ClientOptionsFn) Client {
	opts := DefaultClientOptions()
	for _, fn := range fns {
		fn(opts)
	}

	return &client{
		conn:        conn,
		enc:         gob.NewEncoder(conn),
		dec:         gob.NewDecoder(conn),
		sendTimeout: opts.SendTimeout,
		recvTimeout: opts.RecvTimeout}
}

func NewServer(listener Listener, handler Handler, fns ...ServerOptionsFn) Server {
	opts := DefaultServerOptions()
	for _, fn := range fns {
		fn(opts)
	}

	s := &server{
		listener:    listener,
		handler:     handler,
		pool:        concurrent.NewWorkPool(opts.WorkPoolSize),
		sendTimeout: opts.SendTimeout,
		recvTimeout: opts.RecvTimeout,
		closed:      make(chan struct{}),
		closer:      make(chan struct{}, 1)}

	s.startListener()
	return s
}

func NewTCPClient(addr string) (Client, error) {
	conn, err := ConnectTCP(addr)
	if err != nil {
		return nil, err
	}

	return NewClient(conn), nil
}

func NewTCPServer(port int, handler Handler, fns ...ServerOptionsFn) (Server, error) {
	listener, err := ListenTCP(port)
	if err != nil {
		return nil, err
	}

	return NewServer(listener, handler, fns...), nil
}

type request struct {
	typ  int
	body enc.Message
}

func (r *request) Type() int {
	return r.typ
}

func (r *request) Body() enc.Message {
	return r.body
}

func (r *request) Write(w enc.Writer) {
	w.Write("type", r.typ)
	w.Write("body", r.body)
}

type response struct {
	err error
	body enc.Message
}

func (r *response) Error() error {
	return r.err
}

func (r *response) Body() enc.Message {
	return r.body
}

func (r *response) Write(w enc.Writer) {
	w.Write("error", r.err)
	w.Write("body", r.body)
}

type client struct {
	conn Connection

	enc *gob.Encoder
	dec *gob.Decoder

	sendTimeout time.Duration
	recvTimeout time.Duration
}

func (s *client) Close() error {
	return s.conn.Close()
}

func (s *client) Send(req Request) (Response, error) {
	var res Response
	if err := s.send(req); err != nil {
		return res, err
	}

	return s.recv()
}

func (s *client) send(req Request) error {
	var err error
	done, timeout := concurrent.NewBreaker(s.sendTimeout, func() interface{} {
		err = s.enc.Encode(req)
		return nil
	})

	select {
	case <-done:
		return err
	case <-timeout:
		return concurrent.NewTimeoutError(s.sendTimeout, "client:send")
	}
}

func (s *client) recv() (Response, error) {
	var resp Response
	var err error
	done, timeout := concurrent.NewBreaker(s.recvTimeout, func() interface{} {
		err = s.dec.Decode(&resp)
		return nil
	})

	select {
	case <-done:
		return resp, err
	case <-timeout:
		return resp, concurrent.NewTimeoutError(s.sendTimeout, "client:recv")
	}
}

type server struct {
	handler  Handler
	listener Listener

	pool   concurrent.WorkPool
	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup

	sendTimeout time.Duration
	recvTimeout time.Duration
}

func (s *server) Client(fns ...ClientOptionsFn) (Client, error) {
	select {
	case <-s.closed:
		return nil, ServerClosedError
	default:
	}

	conn, err := s.listener.Conn()
	if err != nil {
		return nil, err
	}

	return NewClient(conn, fns...), nil
}

func (s *server) Close() error {
	select {
	case <-s.closed:
		return ServerClosedError
	case s.closer <- struct{}{}:
	}

	close(s.closed)

	var err error
	err = s.listener.Close()
	err = s.pool.Close()
	s.wait.Wait()
	return err
}

func (s *server) startListener() {
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				return
			}

			err = s.pool.Submit(s.newWorker(conn))
			if err != nil {
				s.killConnection(conn, err)
				continue
			}
		}
	}()
}

func (s *server) killConnection(conn Connection, err error) error {
	defer conn.Close()
	return s.send(gob.NewEncoder(conn), NewErrorResponse(err))
}

func (s *server) newWorker(conn Connection) func() {
	return func() {
		defer conn.Close()

		dec := gob.NewDecoder(conn)
		enc := gob.NewEncoder(conn)

		for {
			req, err := s.recv(dec)
			if err != nil {
				return
			}

			res, err := s.handle(req)
			if err != nil {
				return
			}

			if err = s.send(enc, res); err != nil {
				return
			}
		}
	}
}

func (s *server) handle(req Request) (Response, error) {
	val := make(chan Response, 1)
	go func() {
		val <- s.handler(req)
	}()

	var resp Response
	select {
	case <-s.closed:
		return resp, ServerClosedError
	case resp = <-val:
		return resp, nil
	}
}

func (s *server) recv(dec *gob.Decoder) (Request, error) {
	var err error
	var req Request
	done, timer := concurrent.NewBreaker(s.recvTimeout, func() interface{} {
		err = dec.Decode(&req)
		return nil
	})

	select {
	case <-s.closed:
		return req, ServerClosedError
	case <-timer:
		return req, concurrent.NewTimeoutError(s.sendTimeout, "server:recv")
	case <-done:
		return req, err
	}
}

func (s *server) send(enc *gob.Encoder, res Response) error {
	var err error
	done, timer := concurrent.NewBreaker(s.sendTimeout, func() interface{} {
		err = enc.Encode(res)
		return nil
	})

	select {
	case <-s.closed:
		return ServerClosedError
	case <-timer:
		return concurrent.NewTimeoutError(s.sendTimeout, "server:send")
	case <-done:
		return err
	}
}
