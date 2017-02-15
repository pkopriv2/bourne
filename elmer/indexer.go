package elmer

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
)

// The indexer is the primary interface to the catalog and stores
type indexer struct {
	ctx           common.Context
	ctrl          common.Control
	logger        common.Logger
	peer          kayak.Host
	storeEnsure   chan *common.Request
	storeExists   chan *common.Request
	storeDel      chan *common.Request
	storeItemRead chan *common.Request
	storeItemSwap chan *common.Request
	workers       int
}

func newIndexer(ctx common.Context, peer kayak.Host, workers int) (*indexer, error) {
	ctx = ctx.Sub("Indexer")

	m := &indexer{
		ctx:           ctx,
		ctrl:          ctx.Control(),
		logger:        ctx.Logger(),
		peer:          peer,
		storeEnsure:   make(chan *common.Request),
		storeExists:   make(chan *common.Request),
		storeDel:      make(chan *common.Request),
		storeItemRead: make(chan *common.Request),
		storeItemSwap: make(chan *common.Request),
		workers:       workers,
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

			epoch, err := openEpoch(s.ctx, s, log, sync, s.workers, iter)
			if err != nil {
				s.logger.Error("Error opening epoch: %+v", err)
				continue
			}

			select {
			case <-s.ctrl.Closed():
				return
			case <-epoch.ctrl.Closed():
			}

			s.logger.Error("Epoch died: %+v", epoch.ctrl.Failure())
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
			return nil, errors.WithStack(e)
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

func (s *indexer) StoreDel(cancel <-chan struct{}, store []byte) error {
	_, err := s.sendRequest(s.storeDel, cancel, store)
	return err
}

func (s *indexer) StoreEnsure(cancel <-chan struct{}, store []byte) error {
	_, err := s.sendRequest(s.storeEnsure, cancel, store)
	return err
}

func (s *indexer) StoreExists(cancel <-chan struct{}, store []byte) (bool, error) {
	raw, err := s.sendRequest(s.storeExists, cancel, store)
	if err != nil {
		return false, err
	}
	return raw.(bool), nil
}

func (s *indexer) StoreReadItem(cancel <-chan struct{}, store []byte, key []byte) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemRead, cancel, getRpc{store, key})
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *indexer) StoreSwapItem(cancel <-chan struct{}, store []byte, key []byte, val []byte, ver int) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemSwap, cancel, swapRpc{store, key, val, ver})
	if err != nil {
		return Item{}, false, err
	}

	rpc := raw.(responseRpc)
	return rpc.Item, rpc.Ok, nil
}

func (s *indexer) StoreTryUpdateItem(cancel <-chan struct{}, store []byte, key []byte, fn func([]byte) []byte) (bool, error) {
	item, _, err := s.StoreReadItem(cancel, store, key)
	if err != nil {
		return false, errors.WithStack(err)
	}

	new := fn(item.Val)
	if new == nil {
		return true, nil
	}

	_, ok, err := s.StoreSwapItem(cancel, store, key, new, item.Ver)
	if err != nil {
		return false, errors.WithStack(err)
	}

	return ok, nil
}

func (s *indexer) StoreUpdateItem(cancel <-chan struct{}, store []byte, key []byte, fn func([]byte) []byte) (Item, error) {
	for !common.IsCanceled(cancel) {
		item, _, err := s.StoreReadItem(cancel, store, key)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		new := fn(item.Val)
		if bytes.Equal(item.Val, new) {
			return item, nil
		}

		item, ok, err := s.StoreSwapItem(cancel, store, key, new, item.Ver)
		if err != nil {
			return Item{}, errors.WithStack(err)
		}

		if ok {
			return item, nil
		}
	}
	return Item{}, errors.WithStack(common.CanceledError)
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
	catalog  *catalog
	log      kayak.Log
	sync     kayak.Sync
	requests common.WorkPool
}

func openEpoch(ctx common.Context, parent *indexer, log kayak.Log, sync kayak.Sync, workers int, cycle int) (*epoch, error) {
	ctx = ctx.Sub("Epoch(%v)", cycle)

	last, ss, err := log.Snapshot()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to retrieve snapshot")
	}

	catalogue, err := build(ss)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to build catalog")
	}

	e := &epoch{
		ctrl:     ctx.Control(),
		logger:   ctx.Logger(),
		parent:   parent,
		catalog:  catalogue,
		log:      log,
		sync:     sync,
		requests: common.NewWorkPool(ctx.Control(), workers),
	}

	if err := e.start(last + 1); err != nil {
		return nil, errors.Wrap(err, "Error starting epoch lifecycle")
	}

	return e, nil
}

func (e *epoch) StoreExists(store []byte) bool {
	s := e.catalog.Get(store)
	return s != nil
}

func (e *epoch) StoreDel(store []byte) {
	e.catalog.Del(store)
}

func (e *epoch) StoreEnsure(store []byte) {
	e.catalog.Ensure(store)
}

func (e *epoch) StoreGetItem(cancel <-chan struct{}, store []byte, key []byte) (Item, bool, error) {
	val, err := e.sync.Barrier(cancel)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	if err := e.sync.Sync(cancel, val); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	s := e.catalog.Get(store)
	if s == nil {
		return Item{}, false, errors.WithStack(NoStoreError)
	}

	item, ok := s.Get(key)
	return item, ok, nil
}

func (e *epoch) StoreSwapItem(cancel <-chan struct{}, store []byte, key []byte, val []byte, ver int) (Item, bool, error) {
	entry, err := e.log.Append(cancel, Item{store, key, val, ver}.Bytes())
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	// TODO: Does this break linearizability???  Technically, another conflicting item
	// can come in immediately after we sync and update the value - and we can't tell
	// whether our update was accepted or not..
	s := e.catalog.Ensure(store)
	if err := e.sync.Sync(cancel, entry.Index); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	item, ok := s.Get(key)
	return item, ok && item.Ver == ver+1, nil
}

func (e *epoch) start(index int) error {
	// start the request router
	go func() {
		defer e.ctrl.Close()
		defer e.logger.Info("Request router shutting down")
		e.logger.Info("Starting request router")

		for {
			select {
			case <-e.ctrl.Closed():
				return
			case req := <-e.parent.storeItemSwap:
				e.handleStoreSwapItem(req)
			case req := <-e.parent.storeItemRead:
				e.handleStoreGetItem(req)
			case req := <-e.parent.storeExists:
				e.handleStoreExists(req)
			case req := <-e.parent.storeDel:
				e.handleStoreDel(req)
			case req := <-e.parent.storeEnsure:
				e.handleStoreEnsure(req)
			}
		}
	}()

	// start the indexing routine
	go func() {
		defer e.ctrl.Close()
		defer e.logger.Info("Indexer shutting down")
		e.logger.Info("Starting indexer")

		l, err := e.log.Listen(index, 1024)
		if err != nil {
			e.ctrl.Fail(err)
			return
		}

		defer l.Close()
		for {
			select {
			case <-e.ctrl.Closed():
				return
			case <-l.Ctrl().Closed():
				e.ctrl.Fail(l.Ctrl().Failure())
				return
			case entry := <-l.Data():
				if err := e.handleEntry(entry); err != nil {
					e.logger.Error("Error parsing item from event stream [%v]: %+v", entry.Index, err)
				}
			}
		}
	}()

	return nil
}

func (e *epoch) handleStoreGetItem(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(getRpc)

		item, ok, err := e.StoreGetItem(req.Canceled(), rpc.Store, rpc.Key)
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

func (e *epoch) handleStoreSwapItem(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(swapRpc)

		item, ok, err := e.StoreSwapItem(req.Canceled(), rpc.Store, rpc.Key, rpc.Val, rpc.Ver)
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

func (e *epoch) handleStoreExists(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		req.Ack(e.StoreExists(req.Body().([]byte)))
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreDel(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		e.StoreDel(req.Body().([]byte))
		req.Ack(nil)
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreEnsure(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		e.StoreEnsure(req.Body().([]byte))
		req.Ack(nil)
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleEntry(entry kayak.Entry) error {
	defer e.sync.Ack(entry.Index)

	e.logger.Debug("Indexing entry: %v", entry)
	if entry.Kind != kayak.Std {
		return nil
	}

	item, err := parseItemBytes(entry.Event)
	if err != nil {
		return errors.WithStack(err)
	}

	// TODO: build a store cache
	store := e.catalog.Ensure(item.Store)
	store.Put(item.Key, item.Val, item.Ver)
	return nil
}

func build(st kayak.EventStream) (*catalog, error) {
	defer st.Close()

	catalog := newCatalog()
	for !st.Ctrl().IsClosed() {

		var evt kayak.Event
		select {
		case <-st.Ctrl().Closed():
		case evt = <-st.Data():
			fmt.Println("EVENT: ", evt)
			// evt.Raw()
		}

		// item, err := parseItemBytes(evt)
		// if err != nil {
		// return nil, errors.WithStack(err)
		// }
		//
		// store := catalogue.Init(item.Store)
		// store.Put(item.Key, item.Val, item.Ver)
	}
	return catalog, st.Ctrl().Failure()
}
