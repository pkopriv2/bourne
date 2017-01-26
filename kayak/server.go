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
	actReplicate   = "kayak.replica.replicate"
	actRequestVote = "kayak.replica.requestVote"
	actAppend      = "kayak.client.append"
)

// Meta messages
var (
	metaReplicate   = serverNewMeta(actReplicate)
	metaRequestVote = serverNewMeta(actRequestVote)
	metaAppend      = serverNewMeta(actAppend)
)

type server struct {
	ctx    common.Context
	logger common.Logger
	self   *replicatedLog
}

// Returns a new service handler for the ractlica
func newServer(ctx common.Context, logger common.Logger, port string, self *replicatedLog) (net.Server, error) {
	server := &server{ctx: ctx, logger: logger.Fmt("Server"), self: self}
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
		case actReplicate:
			return s.Replicate(req)
		case actRequestVote:
			return s.RequestVote(req)
		case actAppend:
			return s.Append(req)
		}
	}
}

func (s *server) InstallSnapshot(req net.Request) net.Response {
	append, err := readReplicateRequest(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.Replicate(append.id, append.term, append.prevLogIndex, append.prevLogTerm, append.items, append.commit)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return newResponseResponse(resp)
}

func (s *server) Replicate(req net.Request) net.Response {
	append, err := readReplicateRequest(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	resp, err := s.self.Replicate(append.id, append.term, append.prevLogIndex, append.prevLogTerm, append.items, append.commit)
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
	append, err := readAppendEventRequest(req.Body())
	if err != nil {
		return net.NewErrorResponse(err)
	}

	item, err := s.self.RemoteAppend(append.event, append.source, append.seq, append.kind)
	if err != nil {
		return net.NewErrorResponse(err)
	}

	return appendEventResponse{item.Index, item.Term}.Response()
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

type appendEventResponse struct {
	index int
	term  int
}

func (r appendEventResponse) Write(w scribe.Writer) {
	w.WriteInt("index", r.index)
	w.WriteInt("term", r.term)
}

func (r appendEventResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func readAppendEventResponse(r scribe.Reader) (ret appendEventResponse, err error) {
	err = common.Or(err, r.ReadInt("index", &ret.index))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	return
}

func readNetAppendEventResponse(res net.Response) (ret appendEventResponse, err error) {
	err = res.Error()
	if err != nil {
		return
	}

	return readAppendEventResponse(res.Body())
}

type appendEventRequest struct {
	event  Event
	source uuid.UUID
	seq    int
	kind   int
}

func readAppendEventRequest(r scribe.Reader) (ret appendEventRequest, err error) {
	err = common.Or(err, r.ReadBytes("event", (*[]byte)(&ret.event)))
	err = common.Or(err, r.ReadUUID("source", &ret.source))
	err = common.Or(err, r.ReadInt("seq", &ret.seq))
	err = common.Or(err, r.ReadInt("kind", &ret.kind))
	return ret, err
}

func readNetAppendEventRequest(req net.Request) (ret appendEventRequest, err error) {
	return readAppendEventRequest(req.Body())
}

func (r appendEventRequest) Request() net.Request {
	return net.NewRequest(metaAppend, scribe.Write(r))
}

func (r appendEventRequest) Write(w scribe.Writer) {
	w.WriteBytes("event", r.event)
	w.WriteUUID("source", r.source)
	w.WriteInt("seq", r.seq)
	w.WriteInt("kind", r.kind)
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
	w.WriteInt("maxLogIndex", r.maxLogIndex)
	w.WriteInt("maxLogTerm", r.maxLogTerm)
}

func (r requestVoteRequest) Request() net.Request {
	return net.NewRequest(metaRequestVote, scribe.Write(r))
}

type replicateRequest struct {
	id           uuid.UUID
	term         int
	items        []LogItem
	prevLogIndex int
	prevLogTerm  int
	commit       int
}

func (a replicateRequest) Write(w scribe.Writer) {
	w.WriteUUID("id", a.id)
	w.WriteInt("term", a.term)
	w.WriteMessages("items", a.items)
	w.WriteInt("prevLogIndex", a.prevLogIndex)
	w.WriteInt("prevLogTerm", a.prevLogTerm)
	w.WriteInt("commit", a.commit)
}

func (r replicateRequest) Request() net.Request {
	return net.NewRequest(metaReplicate, scribe.Write(r))
}

func readReplicateRequest(r scribe.Reader) (ret replicateRequest, err error) {
	var msgs []scribe.Message
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadMessages("items", &msgs))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ReadInt("commit", &ret.commit))
	if err != nil {
		return
	}

	items := make([]LogItem, 0, len(msgs))
	for _, m := range msgs {
		item, err := ReadLogItem(m)
		if err != nil {
			return replicateRequest{}, err
		}
		items = append(items, item.(LogItem))
	}

	ret.items = items
	return ret, err
}

type installSnapshotRequest struct {
	snapshotId   uuid.UUID
	leaderId     uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	prevConfig   []byte
	batchOffset  int
	batch        []Event
	done         bool
}

func (a installSnapshotRequest) Write(w scribe.Writer) {
	w.WriteUUID("snapshotId", a.snapshotId)
	w.WriteUUID("leaderId", a.leaderId)
	w.WriteInt("term", a.term)
	w.WriteInt("prevLogIndex", a.prevLogIndex)
	w.WriteInt("prevLogTerm", a.prevLogTerm)
	w.WriteBytes("prevConfig", a.prevConfig)
	w.WriteInt("batchOffset", a.batchOffset)
	w.WriteMessages("batch", a.batch)
	w.WriteBool("done", a.done)
}

func readIntallSnapshotRequest(r scribe.Reader) (ret installSnapshotRequest, err error) {
	err = common.Or(err, r.ReadUUID("snapshotId", &ret.snapshotId))
	err = common.Or(err, r.ReadUUID("leaderId", &ret.leaderId))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ReadBytes("prevConfig", &ret.prevConfig))
	err = common.Or(err, r.ReadInt("batchOffset", &ret.batchOffset))
	err = common.Or(err, r.ParseMessages("batch", &ret.batch, eventParser))
	err = common.Or(err, r.ReadBool("done", &ret.done))
	return
}

type updateRosterRequest struct {
	serverId   uuid.UUID
	serverAddr uuid.UUID
	join       bool
}

func (u updateRosterRequest) Write(w scribe.Writer) {
	w.WriteUUID("serverId", u.serverId)
	w.WriteUUID("serverAddr", u.serverAddr)
	w.WriteBool("join", u.join)
}

func readUpdateRosterRequest(r scribe.Reader) (ret updateRosterRequest, err error) {
	err = common.Or(err, r.ReadUUID("serverId", &ret.serverId))
	err = common.Or(err, r.ReadUUID("serverAddr", &ret.serverAddr))
	err = common.Or(err, r.ReadBool("join", &ret.join))
	return
}
