package convoy

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	actPing      = "/health/ping"
	actPingProxy = "/health/pingProxy"
	actPushPull  = "/dissem/pushPull"
	actDirApply  = "/dir/apply"
	actDirList   = "/dir/list"
	actStorePut  = "/store/put"
)

// Meta messages
var (
	metaPing      = serverNewMeta(actPing)
	metaPingProxy = serverNewMeta(actPingProxy)
	metaDirApply  = serverNewMeta(actDirApply)
	metaDirList   = serverNewMeta(actDirList)
	metaPushPull  = serverNewMeta(actPushPull)
	metaStorePut  = serverNewMeta(actStorePut)
)

func serverNewMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.WriteString("action", action)
	})
}

func serverReadMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.ReadString("action", &ret)
	return
}

// /health/ping
func newPingRequest() net.Request {
	return net.NewRequest(metaPing, scribe.EmptyMessage)
}

type rpcPingResponse bool

func (r rpcPingResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(scribe.BoolMessage(r)))
}

// /health/pingProxy
func readRpcPingResponse(r net.Response) (bool, error) {
	return scribe.ReadBoolMessage(r.Body())
}

type rpcPingProxyRequest uuid.UUID

func (r rpcPingProxyRequest) Request() net.Request {
	return net.NewRequest(metaPingProxy, scribe.Write(scribe.UUIDMessage(r)))
}

func readRpcPingProxyRequest(r net.Request) (rpcPingProxyRequest, error) {
	id, err := scribe.ReadUUIDMessage(r.Body())
	if err != nil {
		return rpcPingProxyRequest{}, err
	}

	return rpcPingProxyRequest(id), nil
}

// /events/pull
type rpcPushPullRequest struct {
	id      uuid.UUID
	version int
	events  []event
}

func (r rpcPushPullRequest) Request() net.Request {
	return net.NewRequest(metaPushPull, scribe.Write(r))
}

func (r rpcPushPullRequest) Write(w scribe.Writer) {
	w.WriteUUID("id", r.id)
	w.WriteInt("version", r.version)
	w.WriteMessages("events", r.events)
}

func readRpcPushPullRequest(req net.Request) (ret rpcPushPullRequest, err error) {
	err = req.Body().ReadUUID("id", &ret.id)
	err = common.Or(err, req.Body().ReadInt("version", &ret.version))
	err = common.Or(err, req.Body().ParseMessages("events", &ret.events, eventParser))
	return
}

type rpcPushPullResponse struct {
	success []bool
	events  []event
}

func (r rpcPushPullResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func (r rpcPushPullResponse) Write(w scribe.Writer) {
	w.WriteBools("success", r.success)
	w.WriteMessages("events", r.events)
}

func readRpcPushPullResponse(req net.Response) (ret rpcPushPullResponse, err error) {
	err = req.Body().ReadBools("success", &ret.success)
	err = common.Or(err, req.Body().ParseMessages("events", &ret.events, eventParser))
	return
}

// /dir/list
func newDirListRequest() net.Request {
	return net.NewRequest(metaDirList, scribe.EmptyMessage)
}

type rpcDirListResponse []event

func (r rpcDirListResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func (r rpcDirListResponse) Write(w scribe.Writer) {
	w.WriteMessages("events", []event(r))
}

func readRpcDirListResponse(res net.Response) (ret rpcDirListResponse, err error) {
	err = res.Body().ParseMessages("events", (*[]event)(&ret), eventParser)
	return
}

// /dir/apply
type rpcDirApplyRequest []event

func (r rpcDirApplyRequest) Request() net.Request {
	return net.NewRequest(metaDirApply, scribe.Write(r))
}

func (r rpcDirApplyRequest) Write(w scribe.Writer) {
	w.WriteMessages("events", []event(r))
}

func readRpcDirApplyRequest(req net.Request) (ret rpcDirApplyRequest, err error) {
	err = req.Body().ParseMessages("events", (*[]event)(&ret), eventParser)
	return
}

type rpcDirApplyResponse []bool

func (r rpcDirApplyResponse) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func (r rpcDirApplyResponse) Write(w scribe.Writer) {
	w.WriteBools("flags", []bool(r))
}

func readRpcDirApplyResponse(req net.Response) (ret rpcDirApplyResponse, err error) {
	err = req.Body().ReadBools("flags", (*[]bool)(&ret))
	return
}

// /store/put
