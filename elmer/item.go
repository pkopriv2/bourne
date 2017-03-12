package elmer

import (
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// An item in a store.
type Item struct {
	Key []byte
	Val []byte
	Ver int
	Del bool
	seq int
	// ttl   time.Duration
}

func (i Item) String() string {
	return fmt.Sprintf("Item(key=%v,ver=%v): %v bytes", i.Key, i.Ver, len(i.Val))
}

func (i Item) Write(w scribe.Writer) {
	w.WriteBytes("key", i.Key)
	w.WriteBytes("val", i.Val)
	w.WriteInt("ver", i.Ver)
	w.WriteBool("del", i.Del)
	w.WriteInt("seq", i.seq)
}

func (i Item) Bytes() []byte {
	return scribe.Write(i).Bytes()
}

func readItem(r scribe.Reader) (item Item, err error) {
	err = common.Or(err, r.ReadBytes("key", &item.Key))
	err = common.Or(err, r.ReadBytes("val", &item.Val))
	err = common.Or(err, r.ReadInt("ver", &item.Ver))
	err = common.Or(err, r.ReadBool("del", &item.Del))
	err = common.Or(err, r.ReadInt("seq", &item.seq))
	return
}

func itemParser(r scribe.Reader) (interface{}, error) {
	return readItem(r)
}

func parseItemBytes(bytes []byte) (item Item, err error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return Item{}, err
	}

	return readItem(msg)
}
