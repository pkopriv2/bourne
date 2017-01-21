package net

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
)

// These set of classes implement a very simple "embeddable", protocol
// agnostic request/response server.
//
// Currently provides implementation over tcp. Considering udp support.
//
// Currently has support for json/gob encoding. Future versions should
// see the encoding implementations externalized, but leaving out for now.
//
// To create a TCP server, simply invoke:
//
// server, err := NewTcpServer(ctx, 8080, myHandler)
// defer server.Close()
//
var (
	ServerError       = errors.New("NET:SERVER:ERROR")
	ServerClosedError = errors.New("NET:SERVER:CLOSED")
)

const (
	ConfServerSendTimeout = "bourne.net.server.send.timeout"
	ConfServerRecvTimeout = "bourne.net.server.recv.timeout"
	ConfServerPoolSize    = "bourne.net.server.pool.size"
	ConfClientSendTimeout = "bourne.net.client.send.timeout"
	ConfClientRecvTimeout = "bourne.net.client.recv.timeout"
	ConfClientEncoding    = "bourne.net.client.encoding"
)

const (
	DefaultServerSendTimeout = 30 * time.Second
	DefaultServerRecvTimeout = 30 * time.Second
	DefaultServerPoolSize    = 30
	DefaultClientSendTimeout = 30 * time.Second
	DefaultClientRecvTimeout = 30 * time.Second
	DefaultClientEncoding    = "json"
)

// Each server manages a single handler and invokes the handler for
// each request it receives.  Handler implementations must be
// thread-safe if they have access to any external resources.
type Handler func(Request) Response

// A server manages the resources used to serve requests in the
// background.  Clients may be spawned from the server implementation.
// This is extremely useful in testing scenarios.
type Server interface {
	io.Closer
	Client() (Client, error)
}

// A client gives consumers access to invoke a server's handlers.
type Client interface {
	io.Closer
	Send(Request) (Response, error)
}

type ClientPool interface {
	io.Closer
	Max() int
	TakeTimeout(time.Duration) Client
	Return(Client)
	Fail(Client)
}

// A request is a writable message asking the server to invoke a handler.
type Request interface {
	scribe.Writable

	// Request header information
	Meta() scribe.Reader

	// The body of the message.  Never nil
	Body() scribe.Reader
}

// A response is a writable message telling the consumer the result of
// invoking the handler.
type Response interface {
	scribe.Writable

	// Whether or not an error occurred.
	Error() error

	// The body of the message.  Never nil
	Body() scribe.Reader
}

// Request/response intiailization functions.
func NewRequest(meta scribe.Message, body scribe.Message) Request {
	if meta == nil {
		meta = scribe.EmptyMessage
	}

	if body == nil {
		body = scribe.EmptyMessage
	}

	return &request{meta, body}
}

func NewEmptyRequest(meta scribe.Message) Request {
	return NewRequest(meta, nil)
}

func NewStandardRequest(body scribe.Message) Request {
	return NewRequest(nil, body)
}

func NewResponse(err error, body scribe.Message) Response {
	if body == nil {
		body = scribe.EmptyMessage
	}

	return &response{err, body}
}

func NewEmptyResponse() Response {
	return NewResponse(nil, nil)
}

func NewStandardResponse(body scribe.Message) Response {
	return NewResponse(nil, body)
}

func NewErrorResponse(err error) Response {
	return NewResponse(err, nil)
}

func readRequest(dec scribe.Decoder) (Request, error) {
	msg, err := scribe.Decode(dec)
	if err != nil {
		return nil, err
	}

	var meta scribe.Message
	if err := msg.ReadMessage("meta", &meta); err != nil {
		return nil, err
	}

	var body scribe.Message
	if err := msg.ReadMessage("body", &body); err != nil {
		return nil, err
	}

	return NewRequest(meta, body), nil
}

func readResponse(dec scribe.Decoder) (Response, error) {
	msg, err := scribe.Decode(dec)
	if err != nil {
		return nil, err
	}

	var erro string
	if err := msg.ReadString("error", &erro); err != nil {
		switch err.(type) {
		default:
			return nil, err
		case *scribe.MissingFieldError:
		}
	}

	var body scribe.Message
	if err := msg.ReadMessage("body", &body); err != nil {
		return nil, err
	}

	return NewResponse(parseError(erro), body), nil
}

// request/response structs
type request struct {
	meta scribe.Message
	body scribe.Message
}

func (r *request) Meta() scribe.Reader {
	return r.meta
}

func (r *request) Body() scribe.Reader {
	return r.body
}

func (r *request) Write(w scribe.Writer) {
	w.WriteMessage("meta", r.meta)
	w.WriteMessage("body", r.body)
}

func (r *request) String() string {
	return fmt.Sprintf("Meta: %v :: Body: %v", r.meta, r.body)
}

type response struct {
	err  error
	body scribe.Message
}

func (r *response) Error() error {
	return r.err
}

func (r *response) Body() scribe.Reader {
	return r.body
}

func (r *response) Write(w scribe.Writer) {
	w.WriteMessage("body", r.body)
	if r.err != nil {
		w.WriteString("error", r.err.Error())
	}
}

func (r *response) String() string {
	return fmt.Sprintf("Err: %v :: Body: %v", r.err, r.body)
}

// Support for multiple encodings (intended to help troubleshoot live systems)
type Encoding byte

func (e Encoding) String() string {
	return EncodingToString(e)
}

const (
	Json Encoding = 0
	Gob           = 1
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

func readEncoding(conn Connection) (Encoding, error) {
	var buf = []byte{0}
	if _, err := conn.Read(buf); err != nil {
		return 0, err
	}

	return Encoding(buf[0]), nil
}

func writeEncoding(conn Connection, enc Encoding) error {
	_, err := conn.Write([]byte{byte(enc)})
	return err
}

// Client implementation

type client struct {
	conn        Connection
	enc         Encoding
	logger      common.Logger
	sendTimeout time.Duration
	recvTimeout time.Duration
}

func NewClient(ctx common.Context, log common.Logger, conn Connection) (Client, error) {
	config := ctx.Config()

	enc, err := EncodingFromString(config.Optional(ConfClientEncoding, DefaultClientEncoding))
	if err != nil {
		return nil, err
	}

	switch enc {
	default:
		return nil, &UnsupportedEncodingError{EncodingToString(enc)}
	case Json,Gob:
	}

	return &client{
		logger:      ctx.Logger(),
		conn:        conn,
		enc:         enc,
		sendTimeout: config.OptionalDuration(ConfClientSendTimeout, DefaultClientSendTimeout),
		recvTimeout: config.OptionalDuration(ConfClientRecvTimeout, DefaultClientRecvTimeout)}, nil
}

func (s *client) String() string {
	return fmt.Sprintf("%v-->%v", s.conn.LocalAddr(), s.conn.RemoteAddr())
}

func (s *client) Close() error {
	return s.conn.Close()
}

func (s *client) Send(req Request) (res Response, err error) {
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

func (s *client) send(req Request) (err error) {
	done, timeout := concurrent.NewBreaker(s.sendTimeout, func() {
		var encoder scribe.Encoder
		switch s.enc {
		case Json:
			encoder = json.NewEncoder(s.conn)
		case Gob:
			encoder = gob.NewEncoder(s.conn)
		}

		if err := writeEncoding(s.conn, s.enc); err != nil {
			err = errors.Wrapf(err, "Error writing encoding header")
			return
		}

		err = scribe.Encode(encoder, req)
	})

	select {
	case <-done:
		return
	case e := <-timeout:
		return errors.Wrapf(e, "client:send")
	}
}

func (s *client) recv() (resp Response, err error) {
	var decoder scribe.Decoder
	switch s.enc {
	case Json:
		decoder = json.NewDecoder(s.conn)
	case Gob:
		decoder = gob.NewDecoder(s.conn)
	}

	done, timeout := concurrent.NewBreaker(s.recvTimeout, func() {
		resp, err = readResponse(decoder)
	})

	select {
	case <-done:
		return
	case e := <-timeout:
		return resp, errors.Wrapf(e, "client:recv")
	}
}

// Client pooling implementation

type clientPool struct {
	ctx common.Context
	log common.Logger
	raw ConnectionPool
}

func NewClientPool(ctx common.Context, log common.Logger, raw ConnectionPool) *clientPool {
	return &clientPool{ctx, log, raw}
}

func (c *clientPool) Close() error {
	return c.raw.Close()
}

func (c *clientPool) Max() int {
	return c.raw.Max()
}

func (c *clientPool) TakeTimeout(dur time.Duration) Client {
	conn := c.raw.TakeTimeout(dur)
	if conn == nil {
		return nil
	}

	client, err := NewClient(c.ctx, c.log, conn)
	if err != nil {
		panic(err)
	}

	return client
}

func (c *clientPool) Return(cl Client) {
	c.raw.Return(cl.(*client).conn)
}

func (c *clientPool) Fail(cl Client) {
	c.raw.Fail(cl.(*client).conn)
}

// Server implementation
func NewServer(ctx common.Context, logger common.Logger, listener Listener, handler Handler) (Server, error) {
	config := ctx.Config()

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

type server struct {
	handler     Handler
	listener    Listener
	context     common.Context
	logger      common.Logger
	pool        concurrent.WorkPool
	closer      chan struct{}
	closed      chan struct{}
	wait        sync.WaitGroup
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

	return NewClient(s.context, s.logger, conn)
}

func (s *server) Close() error {
	select {
	case <-s.closed:
		return ServerClosedError
	case s.closer <- struct{}{}:
	}

	s.logger.Info("Closing server.")
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
				s.logger.Error("Error accepting connection: %v", conn)
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

		for {
			enc, err := readEncoding(conn)
			if err != nil {
				return
			}
			// s.logger.Debug("Determined encoding for request [%v]: %v", enc, conn.RemoteAddr())

			var encoder scribe.Encoder
			var decoder scribe.Decoder
			switch enc {
			default:
				return // TODO: respond with error!
			case Json:
				decoder = json.NewDecoder(conn)
				encoder = json.NewEncoder(conn)
			case Gob:
				decoder = gob.NewDecoder(conn)
				encoder = gob.NewEncoder(conn)
			}

			req, err := s.recv(decoder)
			if err != nil {
				if err != io.EOF {
					s.logger.Error("Error receiving request [%v]: %v", err, conn.RemoteAddr())
				}
				return
			}

			res, err := s.handle(req)
			if err != nil {
				s.logger.Error("Error handling request [%v]: %v", err, conn.RemoteAddr())
				return
			}

			if err = s.send(encoder, res); err != nil {
				s.logger.Error("Error sending response [%v]: %v", err, conn.RemoteAddr())
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

func (s *server) recv(dec scribe.Decoder) (req Request, err error) {
	done, timeout := concurrent.NewBreaker(s.recvTimeout, func() {
		req, err = readRequest(dec)
	})

	select {
	case <-s.closed:
		return req, ServerClosedError
	case e := <-timeout:
		return req, errors.Wrapf(e, "server:recv")
	case <-done:
		return req, err
	}
}

func (s *server) send(encoder scribe.Encoder, res Response) (err error) {
	done, timeout := concurrent.NewBreaker(s.sendTimeout, func() {
		err = scribe.Encode(encoder, res)
	})

	select {
	case <-s.closed:
		return ServerClosedError
	case e := <-timeout:
		return errors.Wrapf(e, "server:send")
	case <-done:
		return err
	}
}

// Tcp support
func NewTcpClient(ctx common.Context, log common.Logger, addr string) (Client, error) {
	conn, err := ConnectTcp(addr)
	if err != nil {
		return nil, err
	}

	return NewClient(ctx, log, conn)
}

func NewTcpServer(ctx common.Context, log common.Logger, port string, handler Handler) (Server, error) {
	listener, err := ListenTcp(port)
	if err != nil {
		return nil, err
	}

	return NewServer(ctx, log, listener, handler)
}

var empty string

func parseError(msg string) error {
	if msg == empty {
		return nil
	}

	return errors.New(msg)
}

// pooling support
