package elmer

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

type epoch struct {
	ctrl   common.Control
	logger common.Logger
	idx    amoeba.Index
	log    kayak.Log
	sync   kayak.Sync
	num    int
}

func openEpoch(ctx common.Context, log kayak.Log, sync kayak.Sync, num int) (*epoch, error) {
	ctx = ctx.Sub("Epoch(%v)", num)

	start, ss, err := log.Snapshot()
	if err != nil {
		return nil, err
	}

	idx, err := build(ss)
	if err != nil {
		return nil, err
	}

	e := &epoch{
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		idx:    idx,
		log:    log,
		sync:   sync,
		num:    num,
	}

	if err := e.Start(start); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *epoch) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	val, err := e.sync.Barrier(cancel)
	if err != nil {
		return Item{}, false, err
	}
	item, ok := read(e.idx, key)
	return item, ok, nil
}

func (e *epoch) Swap(cancel <-chan struct{}, key []byte, val []byte, exp int) (Item, bool, error) {
	return Item{}, false
}

func (e *epoch) Start(index int) error {
	l, err := e.log.Listen(index, 1024)
	if err != nil {
		return err
	}

	go func() {
		defer l.Close()
		for index := -1; ; {
			var e kayak.Entry
			select {
			case <-e.ctrl.Closed():
				return
			case <-l.Ctrl().Closed():
				e.ctrl.Fail(l.Ctrl().Failure())
				return
			case e = <-l.Data():
			}

			item, err := parseItemBytes(e.Event)
			if err != nil {
				e.logger.Error("Error parsing item from event stream [%v]: %v", index)
				continue
			}

			swap(e.idx, item.Key, item.Val, item.Ver)
			e.sync.Ack(entry.Index)
		}
	}()
	return nil
}

func swap(idx amoeba.Index, key []byte, val []byte, exp int) (item Item, ok bool) {
	bytesKey := amoeba.BytesKey(item.Key)

	item = Item{key, val, exp + 1}
	idx.Update(func(u amoeba.Update) {
		raw := u.Get(bytesKey)
		if raw == nil {
			if ok = exp == 0; ok {
				u.Put(bytesKey, item)
			}
			return
		}

		if cur := raw.(Item); cur.Ver == exp {
			u.Put(bytesKey, item)
			ok = true
			return
		}
	})
	return
}

func read(idx amoeba.Index, key []byte) (item Item, ok bool) {
	idx.Update(func(u amoeba.Update) {
		raw := u.Get(amoeba.BytesKey(key))
		if raw == nil {
			return
		}

		item, ok = raw.(Item), true
	})
	return
}

func build(st kayak.EventStream) (amoeba.Index, error) {
	defer st.Close()

	idx := amoeba.NewBTreeIndex(32)
	for {
		evt, err := st.Next()
		if err != nil || evt == nil {
			return nil, err
		}

		item, err := parseItemBytes(evt)
		if err != nil {
			return nil, err
		}

		idx.Update(func(u amoeba.Update) {
			u.Put(amoeba.BytesKey(item.Key), item)
		})
	}
}
