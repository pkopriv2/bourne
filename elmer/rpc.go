package elmer

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
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

type getRpc struct {
	Key []byte
}

func (g getRpc) Write(w scribe.Writer) {
	w.WriteBytes("key", g.Key)
}

func (g getRpc) Request() net.Request {
	return net.NewRequest(metaIdxGet, scribe.Write(g))
}

func readGetRpc(r scribe.Reader) (ret getRpc, err error) {
	err = r.ReadBytes("key", &ret.Key)
	return
}

type swapRpc struct {
	Key  []byte
	Val  []byte
	Prev int
}

func (s swapRpc) Request() net.Request {
	return net.NewRequest(metaIdxSwap, scribe.Write(s))
}

func (s swapRpc) Write(w scribe.Writer) {
	w.WriteBytes("key", s.Key)
	w.WriteBytes("val", s.Val)
	w.WriteInt("prev", s.Prev)
}

func readSwapRpc(r scribe.Reader) (ret swapRpc, err error) {
	err = r.ReadBytes("key", &ret.Key)
	err = common.Or(err, r.ReadBytes("val", &ret.Val))
	err = common.Or(err, r.ReadInt("prev", &ret.Prev))
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
