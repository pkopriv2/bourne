package elmer

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/scribe"
	uuid "github.com/satori/go.uuid"
)

// server endpoints
const (
	actStatus        = "elmer.status"
	actStoreInfo     = "elmer.store.exists"
	actStoreDelete   = "elmer.store.delete"
	actStoreCreate   = "elmer.store.create"
	actStoreItemRead = "elmer.store.item.read"
	actStoreItemSwap = "elmer.store.item.swap"
)

// Meta messages
var (
	metaStatus        = newMeta(actStatus)
	metaStoreInfo     = newMeta(actStoreInfo)
	metaStoreDelete   = newMeta(actStoreDelete)
	metaStoreCreate   = newMeta(actStoreCreate)
	metaStoreItemSwap = newMeta(actStoreItemSwap)
	metaStoreItemRead = newMeta(actStoreItemRead)
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

type partialStoreRpc struct {
	Parent []segment
	Child  []byte
}

func (s partialStoreRpc) Request() net.Request {
	return net.NewRequest(metaStoreInfo, scribe.Write(s))
}

func (s partialStoreRpc) Write(w scribe.Writer) {
	w.WriteMessages("parent", s.Parent)
	w.WriteBytes("child", s.Child)
}

func readPartialStoreRpc(r scribe.Reader) (ret partialStoreRpc, err error) {
	err = r.ParseMessages("parent", &ret.Parent, segmentParser)
	err = common.Or(err, r.ReadBytes("child", &ret.Child))
	return
}

type storeInfoRpc struct {
	Path    []segment
	Enabled bool
	Found   bool
}

func (s storeInfoRpc) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(s))
}

func (s storeInfoRpc) Write(w scribe.Writer) {
	w.WriteMessages("path", s.Path)
	w.WriteBool("enabled", s.Enabled)
	w.WriteBool("found", s.Found)
}

func readStoreInfoRpc(r scribe.Reader) (ret storeInfoRpc, err error) {
	err = r.ParseMessages("path", &ret.Enabled, segmentParser)
	err = common.Or(err, r.ReadBool("ok", &ret.Enabled))
	return
}

type storeRpc struct {
	Store []segment
}

func (s storeRpc) Delete() net.Request {
	return net.NewRequest(metaStoreDelete, scribe.Write(s))
}

func (s storeRpc) Create() net.Request {
	return net.NewRequest(metaStoreCreate, scribe.Write(s))
}

func (s storeRpc) Write(w scribe.Writer) {
	w.WriteMessages("path", s.Store)
}

func readStoreRequestRpc(r scribe.Reader) (ret storeRpc, err error) {
	err = r.ParseMessages("store", &ret.Store, segmentParser)
	return
}

type itemReadRpc struct {
	Store []segment
	Key   []byte
}

func (g itemReadRpc) Write(w scribe.Writer) {
	w.WriteMessages("store", g.Store)
	w.WriteBytes("key", g.Key)
}

func (g itemReadRpc) Request() net.Request {
	return net.NewRequest(metaStoreItemRead, scribe.Write(g))
}

func readItemReadRpc(r scribe.Reader) (ret itemReadRpc, err error) {
	err = r.ParseMessages("store", &ret.Store, segmentParser)
	err = common.Or(err, r.ReadBytes("key", &ret.Key))
	return
}

type swapRpc struct {
	Store []segment
	Swap  Item
}

func (s swapRpc) Request() net.Request {
	return net.NewRequest(metaStoreItemSwap, scribe.Write(s))
}

func (s swapRpc) Write(w scribe.Writer) {
	w.WriteMessages("store", s.Store)
	w.WriteMessage("swap", s.Swap)
}

func readSwapRpc(r scribe.Reader) (ret swapRpc, err error) {
	err = r.ParseMessages("store", &ret.Store, segmentParser)
	err = common.Or(err, r.ParseMessage("swap", &ret.Swap, itemParser))
	return
}

type itemRpc struct {
	Item Item
	Ok   bool
}

func (r itemRpc) Response() net.Response {
	return net.NewStandardResponse(scribe.Write(r))
}

func (s itemRpc) Write(w scribe.Writer) {
	w.WriteMessage("item", s.Item)
	w.WriteBool("ok", s.Ok)
}

func readItemRpc(r scribe.Reader) (ret itemRpc, err error) {
	err = r.ParseMessage("item", &ret.Item, itemParser)
	err = common.Or(err, r.ReadBool("ok", &ret.Ok))
	return
}
