package kayak

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

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
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ParseMessages("items", &ret.items, ReadLogItem))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ReadInt("commit", &ret.commit))
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

func (r installSnapshotRequest) Request() net.Request {
	return net.NewRequest(metaInstallSnapshot, scribe.Write(r))
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
	peer peer
	join bool
}

func (u updateRosterRequest) Request() net.Request {
	return net.NewRequest(metaUpdateRoster, scribe.Write(u))
}

func (u updateRosterRequest) Write(w scribe.Writer) {
	w.WriteMessage("peer", u.peer)
	w.WriteBool("join", u.join)
}

func readUpdateRosterRequest(r scribe.Reader) (ret updateRosterRequest, err error) {
	err = common.Or(err, r.ParseMessage("peer", &ret.peer, peerParser))
	err = common.Or(err, r.ReadBool("join", &ret.join))
	return
}

func newRosterUpdateResponse(err error) net.Response {
	if err == nil {
		return net.NewEmptyResponse()
	} else {
		return net.NewErrorResponse(err)
	}
}

func readRosterUpdateResponse(res net.Response) error {
	return res.Error()
}

type statusResponse struct {
	id     uuid.UUID
	term   term
	config []peer
}

func (u statusResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(u))
}

func (u statusResponse) Write(w scribe.Writer) {
	w.WriteUUID("id", u.id)
	w.WriteMessage("term", u.term)
	w.WriteMessage("config", peers(u.config))
}

func readStatusResponse(r scribe.Reader) (ret statusResponse, err error) {
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ParseMessage("term", &ret.term, termParser))
	err = common.Or(err, r.ParseMessage("config", &ret.config, peerParser))
	return
}

func readNetStatusResponse(res net.Response) (statusResponse, error) {
	if err := res.Error(); err != nil {
		return statusResponse{}, err
	}

	return readStatusResponse(res.Body())
}

func newStatusRequest() net.Request {
	return net.NewEmptyRequest(metaStatus)
}
