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
	event, err := readAppendRequest(req.Body())
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

func newReplicateRequest(a replicateRequest) net.Request {
	return net.NewRequest(metaReplicate, scribe.Build(func(w scribe.Writer) {
		a.Write(w)
	}))
}

func newAppendRequest(e Event) net.Request {
	return net.NewRequest(metaAppend, scribe.Build(func(w scribe.Writer) {
		w.WriteBytes("event", e.Raw())
	}))
}

func newAppendResponse(index int, term int) net.Response {
	return net.NewStandardResponse(scribe.Build(func(w scribe.Writer) {
		w.WriteInt("index", index)
		w.WriteInt("term", term)
	}))
}

func eventParser(r scribe.Reader) (interface{}, error) {
	var tmp []byte
	if e := r.ReadBytes("raw", &tmp); e != nil {
		return nil, e
	}
	return Event(tmp), nil
}

func readEvent(r scribe.Reader, field string) (e Event, err error) {
	var raw []byte
	if err = r.ReadBytes(field, &raw); err == nil {
		return Event(raw), nil
	}
	return
}

func readAppendRequest(r scribe.Reader) (e Event, err error) {
	return readEvent(r, "event")
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
	w.WriteInt("maxLogIndex", r.maxLogIndex)
	w.WriteInt("maxLogTerm", r.maxLogTerm)
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
		item, err := readLogItem(m)
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
	done bool
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
