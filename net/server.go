package net

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/enc"
)

var (
	ServerError       = errors.New("NET:SERVER:ERROR")
	ServerClosedError = errors.New("NET:SERVER:CLOSED")
)

const (
	ConfServerSendTimeout    = "bourne.net.server.send.timeout"
	DefaultServerSendTimeout = 30 * time.Second
	ConfServerRecvTimeout    = "bourne.net.server.recv.timeout"
	DefaultServerRecvTimeout = 30 * time.Second
	ConfServerPoolSize       = "bourne.net.server.pool.size"
	DefaultServerPoolSize    = 10

	ConfClientSendTimeout    = "bourne.net.client.send.timeout"
	DefaultClientSendTimeout = 30 * time.Second
	ConfClientRecvTimeout    = "bourne.net.client.recv.timeout"
	DefaultClientRecvTimeout = 30 * time.Second
	ConfClientEncoding       = "bourne.net.client.encoding"
	DefaultClientEncoding    = "json"
)

// A very simple request/response server.

type Handler func(Request) Response

type Server interface {
	io.Closer
	Client() (Client, error)
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

// Multiple encoding support (intended to help troubleshoot live systems)
type Encoding byte

const (
	Json Encoding = 0
	Gob           = 2
)

type UnsupportedEncodingError struct {
	encoding string
}

func (m *UnsupportedEncodingError) Error() string {
	return fmt.Sprintf("Unsupported encoding", m.encoding)
}

func EncodingToString(encoding Encoding) string {
	switch encoding {
	default:
		return fmt.Sprintf("%v", encoding)
	case Json:
		return "json"
	case Gob:
		return "gob"
	}
}

func EncodingFromString(name string) (Encoding, error) {
	switch name {
	default:
		return 0, &UnsupportedEncodingError{name}
	case "json":
		return Json, nil
	case "gob":
		return Gob, nil
	}
}



func NewRequest(typ int, body enc.Message) Request {
	return &request{typ, body}
}

func NewResponse(err error, body enc.Message) Response {
	return &response{err, body}
}

func NewEmptyResponse() Response {
	return NewResponse(nil, nil)
}

func NewStandardResponse(body enc.Message) Response {
	return NewResponse(nil, body)
}

func NewErrorResponse(err error) Response {
	return NewResponse(err, nil)
}

func DecodeRequest(dec enc.Decoder) (Request, error) {
	msg, err := enc.DecodeMessage(dec)
	if err != nil {
		return nil, err
	}

	var typ int
	if err := msg.Read("type", &typ); err != nil {
		return nil, err
	}

	var body enc.Message
	if _, err := msg.ReadOptional("body", &body); err != nil {
		return nil, err
	}

	return NewRequest(typ, body), nil
}

func DecodeResponse(dec enc.Decoder) (Response, error) {
	msg, err := enc.DecodeMessage(dec)
	if err != nil {
		return nil, err
	}

	var erro string
	if _, err := msg.ReadOptional("error", &erro); err != nil {
		return nil, err
	}

	var body enc.Message
	if _, err := msg.ReadOptional("body", &body); err != nil {
		return nil, err
	}

	return NewResponse(enc.ParseError(erro), body), nil
}

func ReadEncoding(conn Connection) (Encoding, error) {
	var buf = []byte{0}
	if _, err := conn.Read(buf); err != nil {
		return 0, err
	}

	return Encoding(buf[0]), nil
}

func WriteEncoding(conn Connection, enc Encoding) error {
	_, err := conn.Write([]byte{byte(enc)})
	return err
}

func NewClient(ctx common.Context, conn Connection) (Client, error) {
	config := ctx.Config()
	encoding, err := EncodingFromString(config.Optional(ConfClientEncoding, DefaultClientEncoding))
	if err != nil {
		return nil, err
	}

	var encoder enc.Encoder
	var decoder enc.Decoder
	switch encoding {
	default:
		return nil, &UnsupportedEncodingError{EncodingToString(encoding)}
	case Json:
		decoder = json.NewDecoder(conn)
		encoder = json.NewEncoder(conn)
	case Gob:
		decoder = gob.NewDecoder(conn)
		encoder = gob.NewEncoder(conn)
	}

	if err := WriteEncoding(conn, encoding); err != nil {
		return nil, err
	}

	return &client{
		logger:      ctx.Logger(),
		conn:        conn,
		enc:         encoder,
		dec:         decoder,
		sendTimeout: config.OptionalDuration(ConfClientSendTimeout, DefaultClientSendTimeout),
		recvTimeout: config.OptionalDuration(ConfClientRecvTimeout, DefaultClientRecvTimeout)}, nil
}

func NewServer(ctx common.Context, listener Listener, handler Handler) (Server, error) {
	config := ctx.Config()
	logger := ctx.Logger()

	sendTimeout := config.OptionalDuration(ConfServerSendTimeout, DefaultServerSendTimeout)
	recvTimeout := config.OptionalDuration(ConfServerRecvTimeout, sendTimeout)
	s := &server{
		context:     ctx,
		logger:      logger,
		listener:    listener,
		handler:     handler,
		pool:        concurrent.NewWorkPool(config.OptionalInt(ConfServerPoolSize, DefaultServerPoolSize)),
		sendTimeout: sendTimeout,
		recvTimeout: recvTimeout,
		closed:      make(chan struct{}),
		closer:      make(chan struct{}, 1)}

	s.startListener()
	return s, nil
}

func NewTcpClient(ctx common.Context, addr string) (Client, error) {
	conn, err := ConnectTcp(addr)
	if err != nil {
		return nil, err
	}

	return NewClient(ctx, conn)
}

func NewTcpServer(ctx common.Context, port int, handler Handler) (Server, error) {
	listener, err := ListenTcp(port)
	if err != nil {
		return nil, err
	}

	return NewServer(ctx, listener, handler)
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
	err  error
	body enc.Message
}

func (r *response) Error() error {
	return r.err
}

func (r *response) Body() enc.Message {
	return r.body
}

func (r *response) Write(w enc.Writer) {
	w.Write("body", r.body)
	if r.err != nil {
		w.Write("error", r.err.Error())
	}
}

type client struct {
	conn Connection

	enc enc.Encoder
	dec enc.Decoder

	logger common.Logger

	sendTimeout time.Duration
	recvTimeout time.Duration
}

func (s *client) Close() error {
	return s.conn.Close()
}

func (s *client) Send(req Request) (Response, error) {

	var res Response
	var err error

	if err = s.send(req); err != nil {
		s.logger.Error("Error sending request: %v", err)
		return res, err
	}

	if res, err = s.recv(); err != nil {
		s.logger.Error("Error receiving response: %v", err)
		return res, err
	}

	return res, err
}

func (s *client) send(req Request) error {
	var err error
	done, timeout := concurrent.NewBreaker(s.sendTimeout, func() interface{} {
		err = enc.EncodeWritable(s.enc, req)
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
		resp, err = DecodeResponse(s.dec)
		return nil
	})

	select {
	case <-done:
		s.logger.Debug("Received response: %v", resp)
		return resp, err
	case <-timeout:
		return resp, concurrent.NewTimeoutError(s.sendTimeout, "client:recv")
	}
}

type server struct {
	handler  Handler
	listener Listener
	context  common.Context
	logger   common.Logger

	pool   concurrent.WorkPool
	closer chan struct{}
	closed chan struct{}
	wait   sync.WaitGroup

	sendTimeout time.Duration
	recvTimeout time.Duration
}

func (s *server) Client() (Client, error) {
	select {
	case <-s.closed:
		return nil, ServerClosedError
	default:
	}

	conn, err := s.listener.Conn()
	if err != nil {
		return nil, err
	}

	return NewClient(s.context, conn)
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
		s.logger.Debug("Processing connection: %v", conn)

		var encoder enc.Encoder
		var decoder enc.Decoder

		encoding, err := ReadEncoding(conn)
		if err != nil {
			return
		}

		switch encoding {
		default:
			return // TODO: respond with error!
		case Json:
			decoder = json.NewDecoder(conn)
			encoder = json.NewEncoder(conn)
		case Gob:
			decoder = gob.NewDecoder(conn)
			encoder = gob.NewEncoder(conn)
		}

		for {
			req, err := s.recv(decoder)
			if err != nil {
				s.logger.Error("Error receiving request [%v]", err)
				return
			}

			res, err := s.handle(req)
			if err != nil {
				s.logger.Error("Error handling request [%v]", err)
				return
			}

			if err = s.send(encoder, res); err != nil {
				s.logger.Error("Error sending response [%v]", res)
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

func (s *server) recv(dec enc.Decoder) (Request, error) {
	var req Request
	var err error
	done, timer := concurrent.NewBreaker(s.recvTimeout, func() interface{} {
		req, err = DecodeRequest(dec)
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

func (s *server) send(encoder enc.Encoder, res Response) error {
	var err error
	done, timer := concurrent.NewBreaker(s.sendTimeout, func() interface{} {
		err = enc.EncodeWritable(encoder, res)
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
