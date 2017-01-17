package kayak

// // FIXME: compacting + listening is dangerous if the listener is significantly
// // behind.  Generally, this is OKAY for us because the only time we'll be snapshotting
// // will be AFTER the consumer state machine has safely received the values we'll be
// // removing as part of the compaction.
// //
// // May need to more tightly coordinate compacting with listeners...or change
// // the listening architecture to allow for more consistent listeners.
//
// // The event log maintains a set of events sorted (ascending) by
// // insertion time.
//
// type eventLog struct {
// // the currently active segment.
// active *segment
//
// // lock around the active segment.  Protects against concurrent updates during compactions.
// activeLock sync.RWMutex
//
// // // index of last item inserted. (initialized to -1)
// head *position
//
// // index of last item committed.(initialized to -1)
// commit *position
//
// // closing utilities.
// closed chan struct{}
//
// // closing lock.  (a buffered channel of 1 entry.)
// closer chan struct{}
// }
//
// func newEventLog(ctx common.Context) *eventLog {
// return &eventLog{
// active: newSegment([]Event{}, -1, -1),
// head:   newPosition(-1),
// commit: newPosition(-1),
// closed: make(chan struct{}),
// closer: make(chan struct{}, 1)}
// }
//
// func (e *eventLog) Close() error {
// select {
// case <-e.closed:
// return ClosedError
// case e.closer <- struct{}{}:
// }
//
// close(e.closed)
// e.commit.Close()
// e.head.Close()
// return nil
// }
//
// func (e *eventLog) Closed() bool {
// select {
// case <-e.closed:
// return true
// default:
// return false
// }
// }
//
// func (e *eventLog) Committed() (pos int) {
// return e.commit.Get()
// }
//
// func (e *eventLog) Active() *segment {
// e.activeLock.RLock()
// defer e.activeLock.RUnlock()
// return e.active
// }
//
// func (e *eventLog) UpdateActive(fn func(s *segment) *segment) {
// e.activeLock.Lock()
// defer e.activeLock.Unlock()
// e.active = fn(e.active)
// }
//
// func (e *eventLog) ListenCommits(from int, buf int) *positionListener {
// return newPositionListener(e, e.commit, from, buf)
// }
//
// func (e *eventLog) Commit(pos int) {
// e.commit.Update(func(cur int) int {
// return common.Min(pos, e.head.Get())
// })
// }
//
// func (e *eventLog) Head() int {
// return e.head.Get()
// }
//
// func (e *eventLog) Size() int {
// return e.Active().raw.Size()
// }
//
// func (e *eventLog) Get(index int) (LogItem, bool) {
// return e.Active().Get(index)
// }
//
// func (e *eventLog) Scan(start int, end int) []LogItem {
// return e.Active().Scan(start, end)
// }
//
// func (e *eventLog) Append(batch []Event, term int) int {
// head := e.Active().Append(batch, term)
// return e.head.Set(head)
// }
//
// func (e *eventLog) Insert(batch []LogItem) int {
// head := e.Active().Insert(batch)
// return e.head.Set(head)
// }
//
// func (e *eventLog) Snapshot() (index int, term int, commit int) {
// // must take commit prior to taking the end of the log
// // to ensure invariant commit <= head
// commit = e.Committed()
// last := e.Active().Last()
// return last.Index, last.term, commit
// }
//
// func (e *eventLog) Compact(snapshot []Event, head int) (err error) {
// e.UpdateActive(func(s *segment) (new *segment) {
// new, err = s.Compact(snapshot, head)
// return
// })
// return
// }
//
// type segment struct {
// snapshot      []Event
// snapshotIndex int
// snapshotTerm  int
//
// // the maximum position in the segment.
// max int
//
// // the raw segment data
// raw amoeba.Index
// }
//
// func newSegment(snapshot []Event, prev int, term int) *segment {
// return &segment{snapshot, prev, term, -1, amoeba.NewBTreeIndex(32)}
// }
//
// func (d *segment) Head() (head int) {
// d.raw.Read(func(u amoeba.View) {
// head = d.max
// })
// return
// }
//
// func (d *segment) get(u amoeba.View, index int) (item LogItem, ok bool) {
// val := u.Get(amoeba.IntKey(index))
// if val == nil {
// return
// }
//
// return val.(LogItem), true
// }
//
// func (d *segment) Last() (item LogItem) {
// d.raw.Read(func(u amoeba.View) {
// head := d.max
// if head < 0 {
// item = LogItem{Index: -1, term: -1}
// return
// }
//
// item, _ = d.get(u, head)
// })
// return
// }
//
// func (d *segment) Get(index int) (item LogItem, ok bool) {
// d.raw.Read(func(u amoeba.View) {
// item, ok = d.get(u, index)
// })
// return
// }
//
// // scans from [start,end], inclusive on start and end
// func (d *segment) Scan(start int, end int) (batch []LogItem) {
// d.raw.Read(func(u amoeba.View) {
// batch = make([]LogItem, 0, end-start)
// u.ScanFrom(amoeba.IntKey(start), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
// index := int(k.(amoeba.IntKey))
// if index > end {
// s.Stop()
// return
// }
//
// batch = append(batch, i.(LogItem))
// })
// })
// return
// }
//
// func (d *segment) Append(batch []Event, term int) (max int) {
// d.raw.Update(func(u amoeba.Update) {
// max = d.max
// for _, e := range batch {
// max++
// u.Put(amoeba.IntKey(max), LogItem{max, e, term})
// }
//
// d.max = max
// })
// return
// }
//
// func (d *segment) Insert(batch []LogItem) (max int) {
// d.raw.Update(func(u amoeba.Update) {
// max = d.max
// for _, item := range batch {
// u.Put(amoeba.IntKey(item.Index), item)
// if item.Index > max {
// max = item.Index
// }
// }
//
// d.max = max
// })
// return
// }
//
// func (d *segment) Compact(snapshot []Event, head int) (*segment, error) {
// // grab the last item.
// raw, ok := d.Get(head)
// if !ok {
// return nil, errors.Wrapf(StateError, "Unable to compact to [%v].  It doesn't exist!", head)
// }
//
// // copy from current segment from head + 1 on.
// // build the new segment
// new := newSegment(snapshot, head, raw.term)
// new.Insert(d.Scan(head+1, d.Head()))
// return new, nil
// }
//
// type positionListener struct {
// log     *eventLog
// segment *segment
// pos     *position
// ch      chan LogItem
// closed  chan struct{}
// closer  chan struct{}
// }
//
// func newPositionListener(log *eventLog, pos *position, from int, buf int) *positionListener {
// l := &positionListener{log, log.Active(), pos, make(chan LogItem, buf), make(chan struct{}), make(chan struct{}, 1)}
// l.start(from)
// return l
// }
//
// func (l *positionListener) start(from int) {
// go func() {
// defer l.Close()
//
// for cur := from; ; {
// if l.isClosed() {
// return
// }
//
// next, ok := l.pos.WaitForGreaterThanOrEqual(cur)
// if !ok {
// return
// }
//
// for cur <= next {
//
// // if we've moved beyond the current segment, reset to active.
// head := l.segment.Head()
// if cur > head {
// l.segment = l.log.Active()
// }
//
// // scan the next batch
// batch := l.segment.Scan(cur, common.Min(head, next))
//
// // start emitting
// for _, i := range batch {
// select {
// case <-l.log.closed:
// return
// case <-l.closed:
// return
// case l.ch <- i:
// }
// }
//
// // update current
// cur = cur + len(batch)
// }
// }
// }()
// }
//
// func (l *positionListener) isClosed() bool {
// select {
// default:
// return false
// case <-l.closed:
// return true
// }
// }
//
// func (l *positionListener) Closed() <-chan struct{} {
// return l.closed
// }
//
// func (l *positionListener) Items() <-chan LogItem {
// return l.ch
// }
//
// func (l *positionListener) Close() error {
// select {
// case <-l.closed:
// return ClosedError
// case l.closer <- struct{}{}:
// }
//
// // FIXME: This does NOT work!
// close(l.closed)
// l.pos.Notify()
// return nil
// }
//
// type position struct {
// val  int
// lock *sync.Cond
// dead bool
// }
//
// func newPosition(val int) *position {
// return &position{val, &sync.Cond{L: &sync.Mutex{}}, false}
// }
//
// func (c *position) WaitForGreaterThanOrEqual(pos int) (commit int, alive bool) {
// c.lock.L.Lock()
// defer c.lock.L.Unlock()
// if c.dead {
// return pos, false
// }
//
// for commit, alive = c.val, !c.dead; commit < pos; commit, alive = c.val, !c.dead {
// c.lock.Wait()
// if c.dead {
// return pos, true
// }
// }
// return
// }
//
// func (c *position) Notify() {
// c.lock.Broadcast()
// }
//
// func (c *position) Close() {
// c.lock.L.Lock()
// defer c.lock.Broadcast()
// defer c.lock.L.Unlock()
// c.dead = true
// }
//
// func (c *position) Update(fn func(int) int) int {
// c.lock.L.Lock()
// defer c.lock.Broadcast()
// defer c.lock.L.Unlock()
// c.val = common.Max(fn(c.val), c.val)
// return c.val
// }
//
// func (c *position) Set(pos int) (new int) {
// c.lock.L.Lock()
// defer c.lock.Broadcast()
// defer c.lock.L.Unlock()
// c.val = common.Max(pos, c.val)
// return c.val
// }
//
// func (c *position) Get() (pos int) {
// c.lock.L.Lock()
// defer c.lock.L.Unlock()
// return c.val
// }
