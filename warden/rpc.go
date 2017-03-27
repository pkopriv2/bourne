package warden

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// Server endpoints
const (
	actStatus  = "warden.status"
	actLockGet = "warden.lock.get"
)

// Meta messages
var (
	metaStatus = newMeta(actStatus)
)

func newMeta(action string) scribe.Message {
	return scribe.Build(func(w scribe.Writer) {
		w.WriteString("action", action)
	})
}

func readMeta(meta scribe.Reader) (ret string, err error) {
	err = meta.ReadString("action", &ret)
	return
}

func newStatusRequest() net.Request {
	return net.NewEmptyRequest(metaStatus)
}

type statusRpc struct {
	id    uuid.UUID
	peers []string
}

func (u statusRpc) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(u))
}

func (u statusRpc) Write(w scribe.Writer) {
	w.WriteUUID("id", u.id)
	w.WriteStrings("peers", u.peers)
}

func readStatusRpc(r scribe.Reader) (ret statusRpc, err error) {
	err = r.ReadUUID("id", &ret.id)
	err = common.Or(err, r.ReadStrings("peers", &ret.peers))
	return
}
