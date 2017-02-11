package net

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
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

// Each server manages a single handler and invokes the handler for
// each request it receives.  Handler implementations must be
// thread-safe if they have access to any external resources.
type Handler func(Request) Response

// A server manages the resources used to serve requests in the
// background.  Clients may be spawned from the server implementation.
// This is extremely useful in testing scenarios.
type Server interface {
	io.Closer
	Client(Encoding) (Client, error)
}

// A client gives consumers access to invoke a server's handlers.
type Client interface {
	io.Closer
	Local() net.Addr
	Remote() net.Addr
	Send(Request) (Response, error)
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

// Support for multiple encodings (intended to help troubleshoot live systems)
const (
	Json Encoding = 0
	Gob           = 1
)

type Encoding byte

func (e Encoding) String() string {
	switch e {
	default:
		return "unknown"
	case Json:
		return "json"
	case Gob:
		return "gob"
	}
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


// Client implementation

type client struct {
	conn        Connection
	enc         Encoding
	logger      common.Logger
	sendTimeout time.Duration
	recvTimeout time.Duration
}

func NewClient(ctx common.Context, conn Connection, enc Encoding) (Client, error) {
	return &client{
		logger: ctx.Logger(),
		conn:   conn,
		enc:    enc}, nil
}

func (s *client) String() string {
	return fmt.Sprintf("%v-->%v", s.conn.Local(), s.conn.Remote())
}

func (s *client) Close() error {
	return s.conn.Close()
}

func (s *client) Local() net.Addr {
	return s.conn.Local()
}

func (s *client) Remote() net.Addr {
	return s.conn.Remote()
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
	var encoder scribe.Encoder
	switch s.enc {
	case Json:
		encoder = json.NewEncoder(s.conn)
	case Gob:
		encoder = gob.NewEncoder(s.conn)
	}

	if err := writeEncoding(s.conn, s.enc); err != nil {
		return errors.Wrapf(err, "Error writing encoding header")
	}

	return scribe.Encode(encoder, req)
}

func (s *client) recv() (resp Response, err error) {
	var decoder scribe.Decoder
	switch s.enc {
	case Json:
		decoder = json.NewDecoder(s.conn)
	case Gob:
		decoder = gob.NewDecoder(s.conn)
	}
	return readResponse(decoder)
}

// Server implementation
func NewServer(ctx common.Context, listener Listener, handler Handler, workers int) (Server, error) {
	ctx = ctx.Sub("Server(%v)", listener.Addr().String())
	ctx.Logger().Info("Starting")

	ctrl := ctx.Control()
	ctrl.Defer(func(error) {
		listener.Close()
	})

	pool := common.NewWorkPool(ctrl, workers)
	ctrl.Defer(func(error) {
		pool.Close()
	})

	ctrl.Defer(func(cause error) {
		ctx.Logger().Info("Shutting down server: %+v", cause)
	})

	s := &server{
		context:  ctx,
		ctrl:     ctx.Control(),
		logger:   ctx.Logger(),
		listener: listener,
		handler:  handler,
		pool:     pool}

	s.start()
	return s, nil
}

type server struct {
	handler  Handler
	listener Listener
	context  common.Context
	logger   common.Logger
	pool     common.WorkPool
	ctrl     common.Control
}

func (s *server) Client(enc Encoding) (Client, error) {
	if s.ctrl.IsClosed() {
		return nil, errors.WithStack(common.ClosedError)
	}

	conn, err := s.listener.Conn()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to retrieve connection")
	}

	return NewClient(s.context, conn, enc)
}

func (s *server) Close() error {
	return s.ctrl.Close()
}

func (s *server) start() {
	go func() {
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
					s.logger.Error("Error receiving request [%v]: %v", err, conn.Remote())
				}
				return
			}

			res, err := s.handle(req)
			if err != nil {
				s.logger.Error("Error handling request [%v]: %v", err, conn.Remote())
				return
			}

			if err = s.send(encoder, res); err != nil {
				s.logger.Error("Error sending response [%v]: %v", err, conn.Remote())
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
	case <-s.ctrl.Closed():
		return resp, errors.WithStack(common.ClosedError)
	case resp = <-val:
		return resp, nil
	}
}

func (s *server) recv(dec scribe.Decoder) (req Request, err error) {
	return readRequest(dec)
}

func (s *server) send(encoder scribe.Encoder, res Response) (err error) {
	return scribe.Encode(encoder, res)
}

var empty string

func parseError(msg string) error {
	if msg == empty {
		return nil
	}

	return errors.New(msg)
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

// pooling support
