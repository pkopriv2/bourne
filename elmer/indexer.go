package elmer

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/kayak"
	"github.com/pkopriv2/bourne/scribe"
)

type command struct {
	Path path
	Raw  Item
}

func newStoreEnableCommand(path []segment) command {
	return command{path, Item{[]byte{}, []byte{}, 0, false, 0}}
}

func newStoreDisableCommand(path []segment) command {
	return command{path, Item{[]byte{}, []byte{}, 0, true, 0}}
}

func newStoreItemSwapCommand(path []segment, key []byte, val []byte, ver int, del bool) command {
	return command{path, Item{key, val, ver, del, 0}}
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

type storeItemRead struct {
	Path []segment
	Key  []byte
}

type storeItemSwap struct {
	Path []segment
	Key  []byte
	Val  []byte
	Ver  int
	Del  bool
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
			return nil, errors.WithStack(common.ClosedError)
		case r := <-req.Acked():
			return r, nil
		case e := <-req.Failed():
			return nil, errors.WithStack(e)
		case <-cancel:
			return nil, errors.WithStack(common.CanceledError)
		}
	}
}

func (s *indexer) StoreInfo(cancel <-chan struct{}, parent path, child []byte) (storeInfo, bool, error) {
	raw, err := s.sendRequest(s.storeInfo, cancel, partialPath{parent, child})
	if err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}

	if raw == nil {
		return storeInfo{}, false, nil
	}

	return raw.(storeInfo), true, nil
}

func (s *indexer) StoreEnable(cancel <-chan struct{}, prev path) (storeInfo, bool, error) {
	raw, err := s.sendRequest(s.storeEnable, cancel, prev)
	if err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}

	if raw == nil {
		return storeInfo{}, false, nil
	}

	return raw.(storeInfo), true, nil
}

func (s *indexer) StoreDisable(cancel <-chan struct{}, path path) (storeInfo, bool, error) {
	raw, err := s.sendRequest(s.storeDisable, cancel, path)
	if err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}

	if raw == nil {
		return storeInfo{}, false, nil
	}

	return raw.(storeInfo), true, nil
}

func (s *indexer) StoreItemRead(cancel <-chan struct{}, path path, key []byte) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemRead, cancel, storeItemRead{path, key})
	if err != nil {
		return Item{}, false, err
	}

	if raw == nil {
		return Item{}, false, nil
	}

	return raw.(Item), true, nil
}

func (s *indexer) StoreItemSwap(cancel <-chan struct{}, path path, key []byte, val []byte, ver int, del bool) (Item, bool, error) {
	raw, err := s.sendRequest(s.storeItemSwap, cancel, storeItemSwap{path, key, val, ver, del})
	if err != nil {
		return Item{}, false, err
	}

	if raw == nil {
		return Item{}, false, nil
	}

	return raw.(Item), true, nil
}

func (s *indexer) StoreTryUpdateItem(cancel <-chan struct{}, store path, key []byte, fn func([]byte) ([]byte, bool)) (Item, bool, error) {
	item, _, err := s.StoreItemRead(cancel, store, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	val, del := fn(item.Val)
	if val == nil {
		return Item{}, true, nil
	}

	item, ok, err := s.StoreItemSwap(cancel, store, key, val, item.Ver, del)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return item, ok, nil
}

func (s *indexer) StoreUpdateItem(cancel <-chan struct{}, store path, key []byte, fn func([]byte) ([]byte, bool)) (Item, bool, error) {
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

func (e *epoch) AppendAndSync(cancel <-chan struct{}, c command) (int, error) {
	entry, err := e.log.Append(cancel, c.Bytes())
	if err != nil {
		return 0, err
	}

	// TODO: Does this break linearizability???  Technically, another conflicting item
	// can come in immediately after we sync and update the value - and we can't tell
	// whether our update was accepted or not..
	return entry.Index, e.sync.Sync(cancel, entry.Index)
}

// FIXME: should query for barrier? (YES)
func (e *epoch) StoreInfo(parent path, child []byte) (storeInfo, bool, error) {
	return e.root.RecurseInfo(parent, child)
}

func (e *epoch) StoreEnableAndSync(cancel <-chan struct{}, path path) (storeInfo, bool, error) {
	e.logger.Debug("Enabling store [%v]", path)

	if _, err := e.AppendAndSync(cancel, newStoreEnableCommand(path)); err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}

	name, prev := path.Tail()

	info, found, err := e.root.RecurseInfo(path.Parent(), name)
	if err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}
	if !found || !info.Enabled {
		return storeInfo{}, false, nil
	}
	return info, info.Version() == prev+1, nil
}

func (e *epoch) StoreDisableAndSync(cancel <-chan struct{}, path path) (storeInfo, bool, error) {
	if _, err := e.AppendAndSync(cancel, newStoreDisableCommand(path)); err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}

	name, prev := path.Tail()

	info, found, err := e.root.RecurseInfo(path.Parent(), name)
	if err != nil {
		return storeInfo{}, false, errors.WithStack(err)
	}
	if !found {
		return storeInfo{}, false, nil
	}
	return info, info.Version() == prev+1, nil
}

func (e *epoch) StoreSyncAndRead(cancel <-chan struct{}, path path, key []byte) (Item, bool, error) {
	val, err := e.sync.Barrier(cancel)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	if err := e.sync.Sync(cancel, val); err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	item, ok, err := e.root.RecurseItemRead(path, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	return item, ok, nil
}

func (e *epoch) StoreItemSwapAndSync(cancel <-chan struct{}, path path, key []byte, val []byte, ver int, del bool) (Item, bool, error) {
	index, err := e.AppendAndSync(cancel, newStoreItemSwapCommand(path, key, val, ver, del))
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	e.logger.Debug("Appended swap: %v", index)

	actual, ok, err := e.root.RecurseItemRead(path, key)
	if err != nil {
		return Item{}, false, errors.WithStack(err)
	}

	e.logger.Debug("Read swap: %v", actual.seq)

	if ! ok {
		return actual, false, nil
	}

	return actual, actual.seq == index, nil
}

func (e *epoch) start(index int, size int) error {
	// start the Requestuest router
	go func() {
		defer e.ctrl.Close()
		defer e.logger.Info("Request router shutting down")
		e.logger.Info("Starting Requestuest router")

		for {
			select {
			case <-e.ctrl.Closed():
				return
			case reqStoreInfo := <-e.parent.storeInfo:
				e.reqStoreInfo(reqStoreInfo)
			case req := <-e.parent.storeEnable:
				e.reqStoreEnable(req)
			case req := <-e.parent.storeDisable:
				e.reqStoreDisable(req)
			case req := <-e.parent.storeItemRead:
				e.reqStoreItemRead(req)
			case req := <-e.parent.storeItemSwap:
				e.reqStoreItemSwap(req)
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
				if err := e.processEntry(entry); err != nil {
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

func (e *epoch) reqStoreInfo(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		path := req.Body().(partialPath)

		info, ok, err := e.root.RecurseInfo(path.Parent, path.Child)
		if err != nil {
			req.Fail(err)
			return
		}

		if ok {
			req.Ack(info)
		} else {
			req.Ack(nil)
		}
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) reqStoreEnable(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		info, ok, err := e.StoreEnableAndSync(req.Canceled(), req.Body().(path))
		if err != nil {
			req.Fail(err)
			return
		}

		if ok {
			req.Ack(info)
		} else {
			req.Ack(nil)
		}
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) reqStoreDisable(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		info, ok, err := e.StoreDisableAndSync(req.Canceled(), req.Body().(path))
		if err != nil {
			req.Fail(err)
			return
		}

		if ok {
			req.Ack(info)
		} else {
			req.Ack(nil)
		}
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) reqStoreItemRead(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(storeItemRead)
		item, ok, err := e.StoreSyncAndRead(req.Canceled(), rpc.Path, rpc.Key)
		if err != nil {
			req.Fail(err)
			return
		}

		if ok {
			req.Ack(item)
		} else {
			req.Ack(nil)
		}
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) reqStoreItemSwap(req *common.Request) {
	err := e.requests.SubmitOrCancel(req.Canceled(), func() {
		rpc := req.Body().(storeItemSwap)
		item, ok, err := e.StoreItemSwapAndSync(req.Canceled(), rpc.Path, rpc.Key, rpc.Val, rpc.Ver, rpc.Del)
		if err != nil {
			req.Fail(err)
			return
		}

		if ok {
			req.Ack(item)
		} else {
			req.Ack(nil)
		}
	})
	if err != nil {
		req.Fail(errors.Wrapf(err, "Error submitting to machine."))
	}
}

func (e *epoch) processEntry(entry kayak.Entry) error {
	defer e.sync.Ack(entry.Index)
	if entry.Kind != kayak.Std {
		return nil
	}

	e.logger.Debug("Indexing entry: %v", entry)
	cmd, err := parseCommandBytes(entry.Event)
	if err != nil {
		return errors.WithStack(err)
	}

	return e.processCommand(entry.Index, cmd)
}

func (e *epoch) processCommand(index int, cmd command) error {
	// Handle: store create/delete
	if bytes.Equal(cmd.Raw.Key, []byte{}) {
		if !cmd.Raw.Del {
			e.logger.Debug("Enabling store: %v", cmd.Path)
			_, _, err := e.root.RecurseEnable(cmd.Path)
			return err
		} else {
			e.logger.Debug("Disabling store: %v", cmd.Path)
			_, _, err := e.root.RecurseDisable(cmd.Path)
			return err
		}
	}

	// Handle: item swap.
	e.logger.Debug("Swapping item [%v]: (key=%v,seq=%v,prev=%v)", cmd.Path, cmd.Raw.Key, index, cmd.Raw.Ver)
	_, _, err := e.root.RecurseItemSwap(cmd.Path, cmd.Raw.Key, cmd.Raw.Val, cmd.Raw.Del, index, cmd.Raw.Ver)
	return err
}

func build(stream kayak.EventStream) (*store, int, error) {
	defer stream.Close()

	root, size := newStore([]segment{}), 0

	// Outer:
	// for ; !stream.Ctrl().IsClosed(); size++ {
	// var evt kayak.Event
	// select {
	// case <-stream.Ctrl().Closed():
	// break Outer
	// case evt = <-stream.Data():
	// }
	//
	// cmd, err := parseCommandBytes(evt)
	// if err != nil {
	// return nil, 0, errors.WithStack(err)
	// }
	//
	// apply(root, cmd)
	// }

	return root, size, stream.Ctrl().Failure()
}
