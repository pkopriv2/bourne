package kayak

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// Basic status/health information
func newStatusRequest() net.Request {
	return net.NewEmptyRequest(metaStatus)
}

type status struct {
	id     uuid.UUID
	term   term
	config []peer
}

func (u status) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(u))
}

func (u status) Write(w scribe.Writer) {
	w.WriteUUID("id", u.id)
	w.WriteMessage("term", u.term)
	w.WriteMessages("config", u.config)
}

func readStatusResponse(r scribe.Reader) (ret status, err error) {
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ParseMessage("term", &ret.term, termParser))
	err = common.Or(err, r.ParseMessages("config", &ret.config, peerParser))
	return
}

// Internal append events request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Append events ONLY come from members who are leaders. (Or think they are leaders)
type replicateEvents struct {
	id           uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	items        []LogItem
	commit       int
}

func newHeartBeat(id uuid.UUID, term int, commit int) replicateEvents {
	return replicateEvents{id, term, -1, -1, []LogItem{}, commit}
}

func newReplicateEvents(id uuid.UUID, term int, prevIndex int, prevTerm int, items []LogItem, commit int) replicateEvents {
	return replicateEvents{id, term, prevIndex, prevTerm, items, commit}
}

func (a replicateEvents) String() string {
	return fmt.Sprintf("Replicate(id=%v,prevIndex=%v,prevTerm=%v,commit=%v,items=%v)", a.id.String()[:8], a.prevLogIndex, a.prevLogTerm, a.commit, len(a.items))
}

func (a replicateEvents) Write(w scribe.Writer) {
	w.WriteUUID("id", a.id)
	w.WriteInt("term", a.term)
	w.WriteMessages("items", a.items)
	w.WriteInt("prevLogIndex", a.prevLogIndex)
	w.WriteInt("prevLogTerm", a.prevLogTerm)
	w.WriteInt("commit", a.commit)
}

func (r replicateEvents) Request() net.Request {
	return net.NewRequest(metaReplicate, scribe.Write(r))
}

func readReplicateEvents(r scribe.Reader) (ret replicateEvents, err error) {
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ParseMessages("items", &ret.items, ReadLogItem))
	err = common.Or(err, r.ReadInt("commit", &ret.commit))
	return
}

// Internal request vote.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// Request votes ONLY come from members who are candidates.
type requestVote struct {
	id          uuid.UUID
	term        int
	maxLogIndex int
	maxLogTerm  int
}

func (r requestVote) String() string {
	return fmt.Sprintf("RequestVote(id=%v,idx=%v,term=%v)", r.id.String()[:8], r.maxLogIndex, r.maxLogTerm)
}

func readRequestVote(r scribe.Reader) (ret requestVote, err error) {
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadInt("maxLogIndex", &ret.maxLogIndex))
	err = common.Or(err, r.ReadInt("maxLogTerm", &ret.maxLogTerm))
	return ret, err
}

func (r requestVote) Write(w scribe.Writer) {
	w.WriteUUID("id", r.id)
	w.WriteInt("term", r.term)
	w.WriteInt("maxLogIndex", r.maxLogIndex)
	w.WriteInt("maxLogTerm", r.maxLogTerm)
}

func (r requestVote) Request() net.Request {
	return net.NewRequest(metaRequestVote, scribe.Write(r))
}

// Client append request.  Requests are put onto the internal member
// channel and consumed by the currently active sub-machine.
//
// These come from active clients.
type appendEvent struct {
	Event  Event
	Source uuid.UUID
	Seq    int
	Kind   int
}

func readAppendEvent(r scribe.Reader) (ret appendEvent, err error) {
	err = common.Or(err, r.ReadBytes("event", (*[]byte)(&ret.Event)))
	err = common.Or(err, r.ReadUUID("source", &ret.Source))
	err = common.Or(err, r.ReadInt("seq", &ret.Seq))
	err = common.Or(err, r.ReadInt("kind", &ret.Kind))
	return ret, err
}

func readAppendEventRequest(req net.Request) (ret appendEvent, err error) {
	return readAppendEvent(req.Body())
}

func (r appendEvent) Request() net.Request {
	return net.NewRequest(metaAppend, scribe.Write(r))
}

func (r appendEvent) Write(w scribe.Writer) {
	w.WriteBytes("event", r.Event)
	w.WriteUUID("source", r.Source)
	w.WriteInt("seq", r.Seq)
	w.WriteInt("kind", r.Kind)
}

// append event response type.
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

type installSnapshot struct {
	snapshotId   uuid.UUID
	leaderId     uuid.UUID
	term         int
	prevLogIndex int
	prevLogTerm  int
	prevConfig   []byte
	batchOffset  int
	batch        []Event
}

func (a installSnapshot) Write(w scribe.Writer) {
	w.WriteUUID("snapshotId", a.snapshotId)
	w.WriteUUID("leaderId", a.leaderId)
	w.WriteInt("term", a.term)
	w.WriteInt("prevLogIndex", a.prevLogIndex)
	w.WriteInt("prevLogTerm", a.prevLogTerm)
	w.WriteBytes("prevConfig", a.prevConfig)
	w.WriteInt("batchOffset", a.batchOffset)
	w.WriteMessages("batch", a.batch)
}

func (r installSnapshot) Request() net.Request {
	return net.NewRequest(metaInstallSnapshot, scribe.Write(r))
}

func readIntallSnapshot(r scribe.Reader) (ret installSnapshot, err error) {
	err = common.Or(err, r.ReadUUID("snapshotId", &ret.snapshotId))
	err = common.Or(err, r.ReadUUID("leaderId", &ret.leaderId))
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadInt("prevLogTerm", &ret.prevLogTerm))
	err = common.Or(err, r.ReadInt("prevLogIndex", &ret.prevLogIndex))
	err = common.Or(err, r.ReadBytes("prevConfig", &ret.prevConfig))
	err = common.Or(err, r.ReadInt("batchOffset", &ret.batchOffset))
	err = common.Or(err, r.ParseMessages("batch", &ret.batch, eventParser))
	// err = common.Or(err, r.ReadBool("done", &ret.done))
	return
}

type rosterUpdate struct {
	peer peer
	join bool
}

func (u rosterUpdate) Request() net.Request {
	return net.NewRequest(metaUpdateRoster, scribe.Write(u))
}

func (u rosterUpdate) Write(w scribe.Writer) {
	w.WriteMessage("peer", u.peer)
	w.WriteBool("join", u.join)
}

func readRosterUpdate(r scribe.Reader) (ret rosterUpdate, err error) {
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

// Internal response type.  These are returned through the
// request 'ack'/'response' channels by the currently active
// sub-machine component.
type response struct {
	term    int
	success bool
}

func (r response) Write(w scribe.Writer) {
	w.WriteInt("term", r.term)
	w.WriteBool("success", r.success)
}

func (r response) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func readResponse(r scribe.Reader) (ret response, err error) {
	err = common.Or(err, r.ReadInt("term", &ret.term))
	err = common.Or(err, r.ReadBool("success", &ret.success))
	return
}

// Status request rpc types