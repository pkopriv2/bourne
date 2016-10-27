package net

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/concurrent"
)

var (
	ServerError       = errors.New("NET:SERVER:ERROR")
	ServerClosedError = errors.New("NET:SERVER:CLOSED")
)

type Handler func(Request) Response

type Server interface {
	io.Closer
	Client(...ClientOptionsFn) (Client, error)
}

type Client interface {
	io.Closer
	Send(Request) (Response, error)
}

type RequestType int

type Request struct {
	Type RequestType
	Body interface{}
}

type Response struct {
	Success bool
	Message string
	Body    interface{}
}

type UnknownRequestError struct {
	request RequestType
}

func (u *UnknownRequestError) Error() string {
	return fmt.Sprintf("Unknown request type [%v]", u.request)
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

func NewRequest(t RequestType, body interface{}) Request {
	return Request{Type: t, Body: body}
}

func NewSuccessResponse(body interface{}) Response {
	return Response{Success: true, Body: body}
}

func NewErrorResponse(err error) Response {
	return Response{Message: err.Error()}
}

func DefaultServerOptions() *ServerOptions {
	return &ServerOptions{10, 5 * time.Second, 5 * time.Second}
}

func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{5 * time.Second, 5 * time.Second}
}

func NewUnknownRequestError(request RequestType) *UnknownRequestError {
	return &UnknownRequestError{request}
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
		pool:        concurrent.NewWorkPool(opts.WorkPoolSize),
		handler:     handler,
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
