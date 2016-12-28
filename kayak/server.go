package kayak

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	actAppendEvents = "kayak.appendEvents"
	actRequestVote  = "kayak.requestVote"
	actClientAppend = "kayak.client.append"
)

// Meta messages
var (
	metaAppendEvents = serverNewMeta(actAppendEvents)
	metaRequestVote  = serverNewMeta(actRequestVote)
	metaClientAppend = serverNewMeta(actClientAppend)
)

type server struct {
	ctx common.Context

	// the root server logger.
	logger common.Logger

	// the member
	self *member
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, port string, self *member) (net.Server, error) {
	server := &server{
		ctx:    ctx,
		logger: logger.Fmt("Server"),
		self:   self,
	}

	return net.NewTcpServer(ctx, server.logger, port, serverInitHandler(server))
}

func serverInitHandler(s *server) func(net.Request) net.Response {
	return func(req net.Request) net.Response {
		action, err := serverReadMeta(req.Meta())
		if err != nil {
			return net.NewErrorResponse(errors.Wrap(err, "Error parsing action"))
		}

		switch action {
		default:
			return net.NewErrorResponse(errors.Errorf("Unknown action %v", action))
		case actAppendEvents:
			return s.AppendEvents(req)
		case actRequestVote:
			return s.RequestVote(req)
		case actClientAppend:
			return s.ClientAppend(req)
		}
	}
}

func (s *server) AppendEvents(req net.Request) net.Response {
	append, err := readAppendEventsRequest(req.Body(), s.self.Parser())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.RequestAppendEvents(append.id, append.term, append.prevLogIndex, append.prevLogTerm, append.events, append.commit)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newResponseResponse(resp)
}

func (s *server) RequestVote(req net.Request) net.Response {
	rv, err := readRequestVoteRequest(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.RequestVote(rv.id, rv.term, rv.maxLogIndex, rv.maxLogTerm)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newResponseResponse(resp)
}

func (s *server) ClientAppend(req net.Request) net.Response {
	append, err := readClientAppendRequest(req.Body(), s.self.Parser())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	err = s.self.RequestClientAppend(append.events)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return net.NewEmptyResponse()
}

// Helper functions

func serverNewMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.Write("action", action)
	})
}

func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.Read("action", &ret)
	return
}

func newRequestVoteRequest(r requestVoteRequest) net.Request {
	return net.NewRequest(metaRequestVote, scribe.Build(func(w scribe.Writer) {
		r.Write(w)
	}))
}

func newAppendEventsRequest(a appendEventsRequest) net.Request {
	return net.NewRequest(metaAppendEvents, scribe.Build(func(w scribe.Writer) {
		a.Write(w)
	}))
}

func newClientAppendRequest(a clientAppendRequest) net.Request {
	return net.NewRequest(metaClientAppend, scribe.Build(func(w scribe.Writer) {
		a.Write(w)
	}))
}

func newResponseResponse(res response) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		res.Write(w)
	}))
}

func readResponseResponse(res net.Response) (response, error) {
	err := res.Error()
	if err != nil {
		return response{}, err
	}

	return readResponse(res.Body())
}

type requestVoteRequest struct {
	id          uuid.UUID
	term        int
	maxLogIndex int
	maxLogTerm  int
}

func readRequestVoteRequest(r scribe.Reader) (requestVoteRequest, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return requestVoteRequest{}, err
	}

	ret := requestVoteRequest{id: id}

	err = common.Or(err, r.Read("term", &ret.term))
	err = common.Or(err, r.Read("maxLogIndex", &ret.maxLogIndex))
	err = common.Or(err, r.Read("maxLogTerm", &ret.maxLogTerm))
	return ret, err
}

func (r requestVoteRequest) Write(w scribe.Writer) {
	scribe.WriteUUID(w, "id", r.id)
	w.Write("term", r.term)
	w.Write("maxLogTerm", r.maxLogTerm)
	w.Write("maxLogIndex", r.maxLogIndex)
}

type appendEventsRequest struct {
	id           uuid.UUID
	term         int
	events       []event
	prevLogIndex int
	prevLogTerm  int
	commit       int
}

func (a appendEventsRequest) Write(w scribe.Writer) {
	scribe.WriteUUID(w, "id", a.id)
	w.Write("term", a.term)
	w.Write("events", a.events)
	w.Write("prevLogIndex", a.prevLogIndex)
	w.Write("prevLogTerm", a.prevLogTerm)
	w.Write("commit", a.commit)
}

func readAppendEventsRequest(r scribe.Reader, parse Parser) (appendEventsRequest, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return appendEventsRequest{}, err
	}

	ret := &appendEventsRequest{id: id}

	var msgs []scribe.Message
	err = common.Or(err, r.Read("term", &ret.term))
	err = common.Or(err, r.Read("events", &msgs))
	err = common.Or(err, r.Read("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.Read("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.Read("commit", &ret.commit))
	if err != nil {
		return appendEventsRequest{}, err
	}

	events := make([]event, 0, len(msgs))
	for _, m := range msgs {
		event, err := parse(m)
		if err != nil {
			return appendEventsRequest{}, err
		}
		events = append(events, event)
	}

	ret.events = events
	return *ret, err
}

type clientAppendRequest struct {
	events []event
}

func (a clientAppendRequest) Write(w scribe.Writer) {
	w.Write("events", a.events)
}

func readClientAppendRequest(r scribe.Reader, parse Parser) (clientAppendRequest, error) {
	var msgs []scribe.Message
	if err := r.Read("events", &msgs); err != nil {
		return clientAppendRequest{}, err
	}

	events := make([]event, 0, len(msgs))
	for _, m := range msgs {
		event, err := parse(m)
		if err != nil {
			return clientAppendRequest{}, err
		}
		events = append(events, event)
	}

	return clientAppendRequest{events: events}, nil
}
