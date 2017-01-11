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
	actAppendEvents = "kayak.replica.appendEvents"
	actRequestVote  = "kayak.replica.requestVote"
	actAppend  = "kayak.client.append"
)

// Meta messages
var (
	metaAppendEvents = serverNewMeta(actAppendEvents)
	metaRequestVote  = serverNewMeta(actRequestVote)
	metaAppend  = serverNewMeta(actAppend)
)

type server struct {
	//
	ctx common.Context

	// the root server logger.
	logger common.Logger

	// the member
	self *replicatedLog
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, port string, self *replicatedLog) (net.Server, error) {
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
		case actAppend:
			return s.Append(req)
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

func (s *server) Append(req net.Request) net.Response {
	event, err := readAppendRequest(req.Body(), s.self.Parser())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, err := s.self.RemoteAppend(event)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newAppendResponse(item.Index, item.term)
}

// Helper functions

func serverNewMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.WriteString("action", action)
	})
}

func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.ReadString("action", &ret)
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

func newAppendRequest(e Event) net.Request {
	return net.NewRequest(metaAppend, scribe.Build(func(w scribe.Writer) {
		w.WriteMessage("event", e)
	}))
}

func newAppendResponse(index int, term int) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.WriteInt("index", index)
		w.WriteInt("term", term)
	}))
}

func readAppendRequest(r scribe.Reader, fn Parser) (e Event, err error) {
	var msg scribe.Message
	err = common.Or(err, r.ReadMessage("event", &msg))
	if err != nil {
		return
	}

	return fn(msg)
}

func readAppendResponse(res net.Response) (index int, term int, err error) {
	err = res.Error()
	err = common.Or(err, res.Body().ReadInt("index", &index))
	err = common.Or(err, res.Body().ReadInt("term", &term))
	return
}

func newResponseResponse(res response) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.WriteInt("term", res.term)
		w.WriteBool("success", res.success)
	}))
}

func readResponseResponse(res net.Response) (ret response, err error) {
	err = res.Error()
	err = common.Or(err, res.Body().ReadInt("term", &ret.term))
	err = common.Or(err, res.Body().ReadBool("success", &ret.success))
	return
}

type requestVoteRequest struct {
	id          uuid.UUID
	term        int
	maxLogIndex int
	maxLogTerm  int
}

func readRequestVoteRequest(r scribe.Reader) (ret requestVoteRequest, err error) {
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadInt("maxLogIndex", &ret.maxLogIndex))
	err = common.Or(err, r.ReadInt("maxLogTerm", &ret.maxLogTerm))
	return ret, err
}

func (r requestVoteRequest) Write(w scribe.Writer) {
	w.WriteUUID("id", r.id)
	w.WriteInt("term", r.term)
	w.WriteInt("maxLogTerm", r.maxLogTerm)
	w.WriteInt("maxLogIndex", r.maxLogIndex)
}

type appendEventsRequest struct {
	id           uuid.UUID
	term         int
	events       []Event
	prevLogIndex int
	prevLogTerm  int
	commit       int
}

func (a appendEventsRequest) Write(w scribe.Writer) {
	w.WriteUUID("id", a.id)
	w.WriteInt("term", a.term)
	w.WriteMessages("events", a.events)
	w.WriteInt("prevLogIndex", a.prevLogIndex)
	w.WriteInt("prevLogTerm", a.prevLogTerm)
	w.WriteInt("commit", a.commit)
}

func readAppendEventsRequest(r scribe.Reader, parse Parser) (ret appendEventsRequest, err error) {
	var msgs []scribe.Message
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadMessages("events", &msgs))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ReadInt("commit", &ret.commit))
	if err != nil {
		return
	}

	events := make([]Event, 0, len(msgs))
	for _, m := range msgs {
		event, err := parse(m)
		if err != nil {
			return appendEventsRequest{}, err
		}
		events = append(events, event)
	}

	ret.events = events
	return ret, err
}
