package elmer

import "github.com/pkopriv2/bourne/scribe"

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

type swapRpc struct {
	Key  []byte
	Val  []byte
	Prev int
}

func (s swapRpc) Write(w scribe.Writer) {
	w.WriteBytes("key", s.Key)
	w.WriteBytes("val", s.Val)
	w.WriteInt("prev", s.Prev)
}

type responseRpc struct {
	Item Item
	Ok   bool
}
