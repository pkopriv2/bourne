package elmer

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

type command struct {
	Path []segment
	Raw  Item
}

func newStoreEnableCommand(path []segment) command {
	return command{path, Item{[]byte{}, []byte{}, 0, false}}
}

func newStoreDisableCommand(path []segment) command {
	return command{path, Item{[]byte{}, []byte{}, 0, true}}
}

func newStoreItemSwapCommand(path []segment, swap Item) command {
	return command{path, swap}
}

func (c command) Write(w scribe.Writer) {
	w.WriteMessage("path", path(c.Path))
	w.WriteMessage("raw", c.Raw)
}

func (c command) Bytes() []byte {
	return scribe.Write(c).Bytes()
}

func readCommand(r scribe.Reader) (c command, e error) {
	e = r.ParseMessage("path", (*path)(&c.Path), pathParser)
	e = common.Or(e, r.ParseMessage("raw", &c.Raw, itemParser))
	return
}

func parseCommandBytes(bytes []byte) (command, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return command{}, err
	}

	return readCommand(msg)
}

type partialStorePath struct {
	Parent []segment
	Child  []byte
}

type storeItemRead struct {
	Path []segment
	Key  []byte
}

type storeItemSwap struct {
	Path []segment
	Swap Item
}

type storeItemResponse struct {
	Item Item
	Ok   bool
}

// FIXME: the created indices MUST be expressable as kayak events...catalog
// updates are not currently expressed

// The indexer is the primary interface to the catalog and stores
type indexer struct {
	ctx           common.Context
	ctrl          common.Control
	logger        common.Logger
	peer          kayak.Host
	storeInfo     chan *common.Request
	storeEnable   chan *common.Request
	storeDisable  chan *common.Request
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
		storeEnable:   make(chan *common.Request),
		storeDisable:  make(chan *common.Request),
		storeInfo:     make(chan *common.Request),
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

func (s *indexer) StoreInfo(cancel <-chan struct{}, parent []segment, child []byte) (int, bool, bool, error) {
	raw, err := s.sendRequest(s.storeInfo, cancel, partialStorePath{parent, child})
	if err != nil || raw == nil {
		return 0, false, false, err
	}

	info := raw.(storeInfo)
	return info.Ver, info.Store != nil, true, nil
}

func (s *indexer) StoreEnableOrCreate(cancel <-chan struct{}, path []segment) (bool, error) {
	raw, err := s.sendRequest(s.storeEnable, cancel, path)
	if err != nil {
		return false, err
	}

	return raw.(bool), nil
}

func (s *indexer) StoreDisable(cancel <-chan struct{}, path []segment) (bool, error) {
	raw, err := s.sendRequest(s.storeDisable, cancel, path)
	if err != nil {
		return false, err
	}

	return raw.(bool), nil
}

func (s *indexer) StoreItemRead(cancel <-chan struct{}, path []segment, key []byte) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemRead, cancel, storeItemRead{path, key})
	if err != nil {
		return Item{}, false, err
	}

	resp := raw.(storeItemResponse)
	return resp.Item, resp.Ok, nil
}

func (s *indexer) StoreItemSwap(cancel <-chan struct{}, path []segment, swap Item) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemSwap, cancel, storeItemSwap{path, swap})
	if err != nil {
		return Item{}, false, err
	}

	resp := raw.(storeItemResponse)
	return resp.Item, resp.Ok, nil
}

func (s *indexer) StoreTryUpdateItem(cancel <-chan struct{}, store []segment, key []byte, fn func([]byte) ([]byte, bool)) (Item, bool, error) {
	item, _, err := s.StoreItemRead(cancel, store, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	val, del := fn(item.Val)
	if val == nil {
		return Item{}, true, nil
	}

	item, ok, err := s.StoreItemSwap(cancel, store, Item{key, val, item.Ver, del})
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return item, ok, nil
}

func (s *indexer) StoreUpdateItem(cancel <-chan struct{}, store []segment, key []byte, fn func([]byte) ([]byte, bool)) (Item, bool, error) {
	for !common.IsCanceled(cancel) {
		item, ok, _ := s.StoreTryUpdateItem(cancel, store, key, fn)
		if ok {
			return item, ok, nil
		}
	}
	return Item{}, false, errors.WithStack(common.CanceledError)
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
	root     *store
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

	root, size, err := build(ss)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to build catalog")
	}

	e := &epoch{
		ctrl:     ctx.Control(),
		logger:   ctx.Logger(),
		parent:   parent,
		root:     root,
		log:      log,
		sync:     sync,
		requests: common.NewWorkPool(ctx.Control(), workers),
	}

	if err := e.start(last+1, size); err != nil {
		return nil, errors.Wrap(err, "Error starting epoch lifecycle")
	}

	return e, nil
}

func (e *epoch) appendAndSync(cancel <-chan struct{}, c command) error {
	entry, err := e.log.Append(cancel, c.Bytes())
	if err != nil {
		return err
	}

	// TODO: Does this break linearizability???  Technically, another conflicting item
	// can come in immediately after we sync and update the value - and we can't tell
	// whether our update was accepted or not..
	return e.sync.Sync(cancel, entry.Index)
}

func (e *epoch) StoreGet(path []segment) *store {
	return storeTraverse(e.root, path)
}

// FIXME: should query for barrier?
func (e *epoch) StoreInfo(parent []segment, name []byte) (storeInfo, bool) {
	store := e.StoreGet(parent)
	if store == nil {
		return storeInfo{}, false
	}

	return store.ChildInfo(name)
}

func (e *epoch) StoreEnableOrCreate(cancel <-chan struct{}, path []segment) (bool, error) {
	if err := e.appendAndSync(cancel, newStoreEnableCommand(path)); err != nil {
		return false, err
	}

	parent, leaf := path[:len(path)], path[len(path)-1]

	info, found := e.StoreInfo(parent, leaf.Elem)
	if !found || info.Store == nil {
		return false, nil
	}
	return leaf.Ver == info.Ver, nil
}

func (e *epoch) StoreDisable(cancel <-chan struct{}, path []segment) (bool, error) {
	if err := e.appendAndSync(cancel, newStoreDisableCommand(path)); err != nil {
		return false, err
	}

	parent, leaf := path[:len(path)], path[len(path)-1]

	info, found := e.StoreInfo(parent, leaf.Elem)
	if !found || info.Store != nil {
		return false, nil
	}
	return leaf.Ver == info.Ver, nil
}

func (e *epoch) StoreItemRead(cancel <-chan struct{}, path []segment, key []byte) (Item, bool, error) {
	val, err := e.sync.Barrier(cancel)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	if err := e.sync.Sync(cancel, val); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	store := e.StoreGet(path)
	if store == nil {
		return Item{}, false, errors.Wrapf(InvariantError, "No such store [%v]", path)
	}
	item, ok := store.Get(key)
	return item, ok, nil
}

func (e *epoch) StoreItemSwap(cancel <-chan struct{}, path []segment, item Item) (Item, bool, error) {
	if err := e.appendAndSync(cancel, newStoreItemSwapCommand(path, item)); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	actual, ok, err := e.StoreItemRead(cancel, path, item.Key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}
	if !ok || actual.Ver != item.Ver+1 {
		return Item{}, false, nil
	}
	return actual, true, nil
}

func (e *epoch) start(index int, size int) error {
	// start the request router
	go func() {
		defer e.ctrl.Close()
		defer e.logger.Info("Request router shutting down")
		e.logger.Info("Starting request router")

		for {
			select {
			case <-e.ctrl.Closed():
				return
			case req := <-e.parent.storeInfo:
				e.handleStoreInfo(req)
			case req := <-e.parent.storeEnable:
				e.handleStoreEnable(req)
			case req := <-e.parent.storeDisable:
				e.handleStoreDisable(req)
			case req := <-e.parent.storeItemRead:
				e.handleStoreItemRead(req)
			case req := <-e.parent.storeItemSwap:
				e.handleStoreItemSwap(req)
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

	// // start the compacting routine
	// go func() {
	// defer e.ctrl.Close()
	// defer e.logger.Info("Compactor shutting down")
	// e.logger.Info("Starting compactor")
	//
	// for lastIndex, lastSize := index, size; ; {
	// if err := e.sync.Sync(e.ctrl.Closed(), lastIndex+2*lastSize); err != nil {
	// e.ctrl.Fail(err)
	// return
	// }
	//
	// }
	// }()

	return nil
}

func (e *epoch) handleStoreInfo(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		// req.Ack(e.StoreExists(req.Body().([]segment)))
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreEnable(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		req.Return(e.StoreEnableOrCreate(req.Canceled(), req.Body().([]segment)))
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreDisable(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		req.Return(e.StoreDisable(req.Canceled(), req.Body().([]segment)))
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreItemRead(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(storeItemRead)
		item, ok, err := e.StoreItemRead(req.Canceled(), rpc.Path, rpc.Key)
		if err != nil {
			req.Fail(err)
			return
		}
		req.Ack(itemRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleStoreItemSwap(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(storeItemSwap)
		item, ok, err := e.StoreItemSwap(req.Canceled(), rpc.Path, rpc.Swap)
		if err != nil {
			req.Fail(err)
			return
		}
		req.Ack(itemRpc{item, ok})
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) handleEntry(entry kayak.Entry) error {
	defer e.sync.Ack(entry.Index)
	if entry.Kind != kayak.Std {
		return nil
	}

	e.logger.Debug("Indexing entry: %v", entry)
	cmd, err := parseCommandBytes(entry.Event)
	if err != nil {
		return errors.WithStack(err)
	}

	return apply(e.root, cmd)
}

func build(stream kayak.EventStream) (*store, int, error) {
	defer stream.Close()

	root, size := newStore([]segment{}), 0

Outer:
	for ; !stream.Ctrl().IsClosed(); size++ {
		var evt kayak.Event
		select {
		case <-stream.Ctrl().Closed():
			break Outer
		case evt = <-stream.Data():
		}

		cmd, err := parseCommandBytes(evt)
		if err != nil {
			return nil, 0, errors.WithStack(err)
		}

		apply(root, cmd)
	}

	return root, size, stream.Ctrl().Failure()
}

func apply(root *store, cmd command) error {
	return nil
	// if bytes.Equal(cmd.Raw.Key, []byte{}) {
	// if !cmd.Raw.Del {
	// storeCreate(root, cmd.Path)
	// }
	//
	// if err := storeDelete(root, cmd.Path); err != nil {
	// return errors.WithStack(err)
	// } else {
	// return nil
	// }
	// }
	//
	// if _, _, err := swapStoreItem(root, cmd.Path, cmd.Raw.Key, cmd.Raw.Val, cmd.Raw.Ver); err != nil {
	// return errors.WithStack(err)
	// } else {
	// return nil
	// }
}
