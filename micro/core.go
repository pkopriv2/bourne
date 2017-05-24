package micro

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

// These set of classes implement a very simple "embeddable", protocol
// agnostic request/response server.
//
// Currently provides implementation over tcp. Considering udp support.
//
// Currently has support for json/gob encoding. Future versions could
// see the encoding implementations externalized, but leaving out for now.
//
var (
	UnknownEncodingError = errors.New("Netter:UnknownEncoding")
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
type Request struct {
	Body interface{}
}

// A response is a writable message telling the consumer the result of
// invoking the handler.
type Response struct {

	//
	Ok bool

	// the error string of the response.  (Set if ! Ok)
	Err string

	// The body of the message.  Possibly nil
	Body interface{}
}

func (r Response) Error() error {
	if r.Ok {
		return nil
	}

	return errors.New(r.Err)
}

// A universal wrapper over the gob/json encoders.  Messages can be encoded
// onto streams of these formats for free.
type Encoder interface {
	Encode(interface{}) error
}

// A universal wrapper over the gob/json decoders.  Messages can be decoded
// onto streams of these formats for free.
type Decoder interface {
	Decode(interface{}) error
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
func NewRequest(body interface{}) Request {
	return Request{body}
}

func NewResponse(err error, body interface{}) Response {
	ok := err == nil

	var str string
	if !ok {
		str = err.Error()
	}

	return Response{ok, str, body}
}

func NewEmptyResponse() Response {
	return NewResponse(nil, nil)
}

func NewStandardResponse(body interface{}) Response {
	return NewResponse(nil, body)
}

func NewErrorResponse(err error) Response {
	return NewResponse(err, nil)
}

// Client implementation
type client struct {
	conn        net.Connection
	enc         Encoding
	logger      common.Logger
	sendTimeout time.Duration
	recvTimeout time.Duration
}

func NewClient(ctx common.Context, conn net.Connection, enc Encoding) (Client, error) {
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

func (s *client) send(req Request) error {
	// s.logger.Info("Writing encoding: %v", s.enc)
	if err := writeEncoding(s.conn, s.enc); err != nil {
		return errors.WithStack(err)
	}

	var encoder Encoder
	switch s.enc {
	case Json:
		encoder = json.NewEncoder(s.conn)
	case Gob:
		encoder = gob.NewEncoder(s.conn)
	}

	return errors.WithStack(encoder.Encode(req))
}

func (s *client) recv() (resp Response, err error) {
	var decoder Decoder
	switch s.enc {
	case Json:
		decoder = json.NewDecoder(s.conn)
	case Gob:
		decoder = gob.NewDecoder(s.conn)
	}

	err = errors.WithStack(decoder.Decode(&resp))
	return
}

// Server implementation
func NewServer(ctx common.Context, listener net.Listener, handler Handler, workers int) (Server, error) {
	ctx = ctx.Sub("Server(%v)", listener.Addr().String())
	ctx.Logger().Info("Starting server: %v", workers)

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
	listener net.Listener
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

			if err := s.pool.Submit(s.newWorker(conn)); err != nil {
				s.killConnection(conn, err)
				continue
			}
		}
	}()
}

func (s *server) killConnection(conn net.Connection, err error) error {
	defer conn.Close()
	return s.send(gob.NewEncoder(conn), NewErrorResponse(err))
}

func (s *server) newWorker(conn net.Connection) func() {
	return func() {
		defer conn.Close()

		for {
			enc, err := readEncoding(conn)
			if err != nil {
				return
			}
			// s.logger.Debug("Determined encoding for request [%v]: %v", enc, conn.Remote())

			var encoder Encoder
			var decoder Decoder
			switch enc {
			default:
				s.send(json.NewEncoder(conn), NewErrorResponse(errors.WithMessage(UnknownEncodingError, "Unknown encoding")))
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

func (s *server) recv(dec Decoder) (req Request, err error) {
	err = errors.WithStack(dec.Decode(&req))
	return
}

func (s *server) send(encoder Encoder, res Response) (err error) {
	err = errors.WithStack(encoder.Encode(res))
	return
}

var empty string

func parseError(msg string) error {
	if msg == empty {
		return nil
	}

	return errors.New(msg)
}

func readEncoding(conn net.Connection) (Encoding, error) {
	var buf = []byte{0}
	if _, err := conn.Read(buf); err != nil {
		return 0, err
	}

	return Encoding(buf[0]), nil
}

func writeEncoding(conn net.Connection, enc Encoding) error {
	_, err := conn.Write([]byte{byte(enc)})
	return err
}
