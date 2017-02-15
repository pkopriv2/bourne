package elmer

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	actStatus  = "elmer.status"
	actIdxGet  = "elmer.idx.get"
	actIdxSwap = "elmer.idx.swap"
)

// Meta messages
var (
	metaStatus  = newMeta(actStatus)
	metaIdxGet  = newMeta(actIdxGet)
	metaIdxSwap = newMeta(actIdxSwap)
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
	err = common.Or(err, r.ReadUUID("id", &ret.id))
	err = common.Or(err, r.ReadStrings("peers", &ret.peers))
	return
}

type getRpc struct {
	Store []byte
	Key   []byte
}

func (g getRpc) Write(w scribe.Writer) {
	w.WriteBytes("store", g.Store)
	w.WriteBytes("key", g.Key)
}

func (g getRpc) Request() net.Request {
	return net.NewRequest(metaIdxGet, scribe.Write(g))
}

func readGetRpc(r scribe.Reader) (ret getRpc, err error) {
	err = r.ReadBytes("store", &ret.Store)
	err = r.ReadBytes("key", &ret.Key)
	return
}

type swapRpc struct {
	Store []byte
	Key   []byte
	Val   []byte
	Ver  int
}

func (s swapRpc) Request() net.Request {
	return net.NewRequest(metaIdxSwap, scribe.Write(s))
}

func (s swapRpc) Write(w scribe.Writer) {
	w.WriteBytes("store", s.Store)
	w.WriteBytes("key", s.Key)
	w.WriteBytes("val", s.Val)
	w.WriteInt("ver", s.Ver)
}

func readSwapRpc(r scribe.Reader) (ret swapRpc, err error) {
	err = r.ReadBytes("store", &ret.Store)
	err = common.Or(err, r.ReadBytes("key", &ret.Key))
	err = common.Or(err, r.ReadBytes("val", &ret.Val))
	err = common.Or(err, r.ReadInt("prev", &ret.Ver))
	return
}

type responseRpc struct {
	Item Item
	Ok   bool
}

func (r responseRpc) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func (s responseRpc) Write(w scribe.Writer) {
	w.WriteMessage("item", s.Item)
	w.WriteBool("ok", s.Ok)
}

func readResponseRpc(r scribe.Reader) (ret responseRpc, err error) {
	err = r.ParseMessage("item", &ret.Item, itemParser)
	err = common.Or(err, r.ReadBool("ok", &ret.Ok))
	return
}
