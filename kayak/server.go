package kayak

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	actAppendEvents = "/appendEvents"
	actRequestVote  = "/requestVote"
	actClientAppend = "/client/append"
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
func newServer(ctx common.Context, logger common.Logger, port int, self *member) (net.Server, error) {
	server := &server{
		ctx:    ctx,
		logger: logger.Fmt("Server"),
		self:   self,
	}

	return net.NewTcpServer(ctx, server.logger, strconv.Itoa(port), serverInitHandler(server))
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
	append, err := readAppendEventsRequest(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.RequestAppendEvents(append)
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

	resp, err := s.self.RequestVote(rv)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newResponseResponse(resp)
}

func (s *server) ClientAppend(req net.Request) net.Response {
	return nil
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

func newResponseResponse(res response) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		res.Write(w)
	}))
}

func readResponseResponse(res net.Response) (response, error) {
	var err error

	err = res.Error()
	if err != nil {

	}

	return readResponse(res.Body())
}

type requestVoteRequest struct {
	id            uuid.UUID
	term          int
	lastLogTerm   int
	lastLogOffset int
}

func (r requestVoteRequest) bind(ch chan<- response) requestVote {
	return requestVote{r.id, r.term, r.lastLogTerm, r.lastLogOffset, ch}
}

func readRequestVoteRequest(r scribe.Reader) (requestVoteRequest, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return requestVoteRequest{}, err
	}

	ret := requestVoteRequest{id: id}

	err = common.Or(err, r.Read("term", &ret.term))
	err = common.Or(err, r.Read("lastLogTerm", &ret.lastLogTerm))
	err = common.Or(err, r.Read("lastLogOffset", &ret.lastLogOffset))
	return ret, err
}

func (r requestVoteRequest) Write(w scribe.Writer) {
	scribe.WriteUUID(w, "id", r.id)
	w.Write("term", r.term)
	w.Write("lastLogTerm", r.lastLogTerm)
	w.Write("lastLogOffset", r.lastLogOffset)
}

type appendEventsRequest struct {
	id            uuid.UUID
	term          int
	events        []event
	prevLogOffset int
	prevLogTerm   int
	commit        int
}

func (a appendEventsRequest) bind(ch chan<- response) appendEvents {
	return appendEvents{a.id, a.term, a.events, a.prevLogOffset, a.prevLogTerm, a.commit, ch}
}

func (a appendEventsRequest) Write(w scribe.Writer) {
	scribe.WriteUUID(w, "id", a.id)
	w.Write("term", a.term)
	w.Write("events", a.events)
	w.Write("prevLogOffset", a.prevLogOffset)
	w.Write("prevLogTerm", a.prevLogTerm)
	w.Write("commit", a.commit)
}

func readAppendEventsRequest(r scribe.Reader) (appendEventsRequest, error) {
	id, err := scribe.ReadUUID(r, "id")
	if err != nil {
		return appendEventsRequest{}, err
	}

	ret := appendEventsRequest{id: id, events: make([]event, 0)}

	// var msgs []scribe.Message
	err = common.Or(err, r.Read("term", &ret.term))
	err = common.Or(err, r.Read("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.Read("prevLogOffset", &ret.prevLogOffset))
	err = common.Or(err, r.Read("commit", &ret.commit))
	// err = common.Or(err, r.Read("events", &msgs))

	// events := make([]event, 0, len(msgs))
	// for _, m := range msgs {
	// events = append(events, )
	// }
	return ret, err
}
