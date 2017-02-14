package elmer

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

type indexer struct {
	ctx    common.Context
	ctrl   common.Control
	logger common.Logger
	peer   kayak.Host
	addr   string
	read   chan *common.Request
	swap   chan *common.Request
	pool   common.WorkPool
}

func newIndexer(ctx common.Context, peer kayak.Host, workers int) (*indexer, error) {
	ctx = ctx.Sub("Indexer")

	m := &indexer{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		logger: ctx.Logger(),
		peer:   peer,
		read:   make(chan *common.Request),
		swap:   make(chan *common.Request),
		pool:   common.NewWorkPool(ctx.Control(), workers),
	}

	// bind the indexer's lifecycle to the kayak host
	peer.Context().Control().Defer(func(e error) {
		m.ctrl.Fail(e)
	})

	m.start()
	return m, nil
}

func (m *indexer) Close() error {
	return m.ctrl.Close()
}

func (s *indexer) start() {
	go func() {
		defer s.ctrl.Close()

		for iter := 0; !s.ctrl.IsClosed(); iter++ {
			s.logger.Info("Starting epoch [%v]", iter)

			log, sync, err := s.getLog()
			if err != nil {
				s.logger.Error("Error retrieving log: %+v", err)
				continue
			}

			epoch, err := openEpoch(s.ctx, s, log, sync, iter)
			if err != nil {
				s.logger.Error("Error retrieving log: %+v", err)
				continue
			}

			select {
			case <-s.ctrl.Closed():
				return
			case <-epoch.ctrl.Closed():
			}

			cause := common.Extract(epoch.ctrl.Failure(), common.ClosedError)
			if cause == nil || cause == common.ClosedError {
				return
			}
		}
	}()
}

func (h *indexer) sendRequest(ch chan<- *common.Request, cancel <-chan struct{}, val interface{}) (interface{}, error) {
	req := common.NewRequest(val)
	defer req.Cancel()

	select {
	case <-h.ctrl.Closed():
		return nil, errors.WithStack(common.ClosedError)
	case <-cancel:
		return nil, errors.WithStack(common.CanceledError)
	case ch <- req:
		select {
		case <-h.ctrl.Closed():
			return nil, errors.WithStack(common.CanceledError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, e
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

func (s *indexer) Read(cancel <-chan struct{}, read getRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.read, cancel, read)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *indexer) Swap(cancel <-chan struct{}, swap swapRpc) (Item, bool, error) {
	raw, err := s.sendRequest(s.swap, cancel, swap)
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *indexer) Update(cancel <-chan struct{}, key []byte, fn func([]byte) []byte) (Item, error) {
	for !common.IsCanceled(cancel) {
		item, _, err := s.Read(cancel, getRpc{key})
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		new := fn(item.Val)
		if bytes.Equal(item.Val, new) {
			return item, nil
		}

		item, ok, err := s.Swap(cancel, swapRpc{key, new, item.Ver})
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if ok {
			return item, nil
		}
	}
	return Item{}, errors.WithStack(common.CanceledError)
}

func (s *indexer) Roster(cancel <-chan struct{}) ([]string, error) {
	item, ok, err := s.Read(cancel, getRpc{[]byte("elmer.roster")})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !ok {
		return []string{}, nil
	}

	return parseRosterBytes(item.Val)
}

func (s *indexer) UpdateRoster(cancel <-chan struct{}, fn func([]string) []string) error {
	_, err := s.Update(cancel, []byte("elmer.roster"), func(cur []byte) []byte {
		roster, err := parseRosterBytes(cur)
		if err != nil || roster == nil {
			roster = []string{}
		}

		next := fn(roster)
		if next == nil {
			return []byte{}
		}

		return peers(next).Bytes()
	})
	return err
}

func (s *indexer) getLog() (kayak.Log, kayak.Sync, error) {
	sync, err := s.peer.Sync()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error retrieving peer syncer [%v]", s.peer.Id())
	}
	log, err := s.peer.Log()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error retrieving peer log [%v]", s.peer.Id())
	}
	return log, sync, nil
}

// an epoch represents a single birth-death cycle of an indexer implementation.
type epoch struct {
	ctx      common.Context
	ctrl     common.Control
	logger   common.Logger
	parent   *indexer
	idx      amoeba.Index
	log      kayak.Log
	sync     kayak.Sync
	requests common.WorkPool
}

func openEpoch(ctx common.Context, parent *indexer, log kayak.Log, sync kayak.Sync, cycle int) (*epoch, error) {
	ctx = ctx.Sub("Epoch(%v)", cycle)

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
		parent: parent,
		idx:    idx,
		log:    log,
		sync:   sync,
	}

	if err := e.start(start); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *epoch) Get(cancel <-chan struct{}, key []byte) (Item, bool, error) {
	val, err := e.sync.Barrier(cancel)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	if err := e.sync.Sync(cancel, val); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	item, ok := read(e.idx, key)
	return item, ok, nil
}

func (e *epoch) Swap(cancel <-chan struct{}, item Item) (Item, bool, error) {
	entry, err := e.log.Append(cancel, item.Bytes())
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	// TODO: Does this break linearizability???  Technically, another conflicting item
	// can come in immediately after we sync and update the value - and we can't tell
	// whether our update was accepted or not..

	if err := e.sync.Sync(cancel, entry.Index); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	actual, ok := read(e.idx, item.Key)
	if !ok {
		return Item{}, false, nil
	}

	if !actual.Equal(item) {
		return Item{}, false, nil
	}

	return item, true, nil
}

func (e *epoch) TryUpdate(cancel <-chan struct{}, key []byte, fn func([]byte) []byte) (Item, bool, error) {
	item, _, err := e.Get(cancel, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	new := fn(item.Val)
	if bytes.Equal(item.Val, new) {
		return item, true, nil
	}

	return e.Swap(cancel, Item{key, new, item.Ver})
}

func (e *epoch) Update(cancel <-chan struct{}, key []byte, fn func([]byte) []byte) (Item, error) {
	for !common.IsCanceled(cancel) {
		item, ok, err := e.TryUpdate(cancel, key, fn)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if ok {
			return item, nil
		}
	}
	return Item{}, errors.WithStack(common.CanceledError)
}
//
// func (e *epoch) Roster(cancel <-chan struct{}) ([]string, error) {
	// item, ok, err := e.Get(cancel, []byte("elmer.roster"))
	// if err != nil {
		// return nil, errors.WithStack(err)
	// }
//
	// if !ok {
		// return []string{}, nil
	// }
//
	// return parseRosterBytes(item.Val)
// }
//
// func (e *epoch) TryUpdateRoster(cancel <-chan struct{}, fn func([]string) []string) (bool, error) {
	// _, ok, err := e.TryUpdate(cancel, []byte("elmer.roster"), func(cur []byte) []byte {
		// roster, err := parseRosterBytes(cur)
		// if err != nil || roster == nil {
			// roster = []string{}
		// }
//
		// next := fn(roster)
		// if next == nil {
			// return []byte{}
		// }
//
		// return peers(next).Bytes()
	// })
	// return ok, err
// }

func (e *epoch) start(index int) error {
	l, err := e.log.Listen(index, 1024)
	if err != nil {
		return err
	}
	e.ctrl.Defer(func(error) {
		l.Close()
	})

	// start the request router
	go func() {
		defer e.ctrl.Close()
		for {
			select {
			case <-e.ctrl.Closed():
				return
			case req := <-e.parent.swap:
				e.handleSwap(req)
			case req := <-e.parent.read:
				e.handleRead(req)
			}
		}
	}()

	// start the indexer routine
	go func() {
		defer e.ctrl.Close()
		for {
			var entry kayak.Entry
			select {
			case <-e.ctrl.Closed():
				return
			case <-l.Ctrl().Closed():
				e.ctrl.Fail(l.Ctrl().Failure())
				return
			case entry = <-l.Data():
			}

			if err := indexEntry(e.idx, entry); err != nil {
				e.logger.Error("Error parsing item from event stream [%v]: %+v", entry.Index, err)
				continue
			}

			e.sync.Ack(entry.Index)
		}
	}()

	return nil
}

func (e *epoch) handleRead(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(getRpc)

		item, ok, err := e.Get(req.Canceled(), rpc.Key)
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(responseRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleSwap(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(swapRpc)

		item, ok, err := e.Swap(req.Canceled(), Item{rpc.Key, rpc.Val, rpc.Prev})
		if err != nil {
			req.Fail(err)
			return
		}

		req.Ack(responseRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func indexEntry(idx amoeba.Index, entry kayak.Entry) error {
	item, err := parseItemBytes(entry.Event)
	if err != nil {
		return errors.WithStack(err)
	}

	swap(idx, item.Key, item.Val, item.Ver)
	return nil
}

func swap(idx amoeba.Index, key []byte, val []byte, prev int) (item Item, ok bool) {
	bytesKey := amoeba.BytesKey(item.Key)

	item = Item{key, val, prev + 1}
	idx.Update(func(u amoeba.Update) {
		raw := u.Get(bytesKey)
		if raw == nil {
			if ok = prev == 0; ok {
				u.Put(bytesKey, item)
			}
			return
		}

		if cur := raw.(Item); cur.Ver == prev {
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
		var evt kayak.Event
		select {
		case <-st.Ctrl().Closed():
			break
		case evt = <-st.Data():
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
