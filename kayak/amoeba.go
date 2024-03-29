package kayak

// func NewAmoebaLogStore() LogStore {
// return &amoebaLogStore{}
// }
//
// type amoebaLogStore struct{}
//
// func (a *amoebaLogStore) Get(uuid.UUID) (StoredLog, error) {
// return nil, nil
// }
//
// func (a *amoebaLogStore) New(id uuid.UUID, config []byte) (StoredLog, error) {
// return &amoebaLog{id: id, commit: -1, active: newAmoebaSegment(newAmoebaSnapshot([]Event{}, config), -1, -1)}, nil
// }
//
// func (a *amoebaLogStore) Get(id uuid.UUID) (StoredLog, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLogStore) New(uuid.UUID, []byte) (StoredLog, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLogStore) NewSnapshot(int, int, <-chan Event, int, []byte) (StoredSnapshot, error) {
// panic("not implemented")
// }
//
//
// type amoebaLog struct {
// id         uuid.UUID
// store      *amoebaLogStore
// commit     int
// commitLock sync.RWMutex
// active     *amoebaSegment
// activeLock sync.RWMutex
// }
//
// func (a *amoebaLog) Id() uuid.UUID {
// return a.id
// }
//
// func (a *amoebaLog) Store() (LogStore, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Last() (int, int, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Truncate(start int) error {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Scan(beg int, end int) ([]Entry, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Append(Event, int, Kind) (Entry, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Get(index int) (Entry, bool, error) {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Insert([]Entry) error {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Install(StoredSnapshot) error {
// panic("not implemented")
// }
//
// func (a *amoebaLog) Snapshot() (StoredSnapshot, error) {
// panic("not implemented")
// }
//
//
// func (a *amoebaLog) GetCommit() (int, error) {
// a.commitLock.RLock()
// defer a.commitLock.RUnlock()
// return a.commit, nil
// }
//
// func (a *amoebaLog) SetCommit(pos int) (int, error) {
// a.commitLock.Lock()
// defer a.commitLock.Unlock()
// a.commit = common.Max(a.commit, pos)
// return a.commit, nil
// }
//
// func (a *amoebaLog) Swap(cur StoredSegment, new StoredSegment) (bool, error) {
// if new.PrevIndex() < cur.PrevIndex() {
// return false, errors.Wrapf(SwapError, "New segment [%v] is older than current. [%v]", new.PrevIndex(), cur.PrevTerm())
// }
//
// a.activeLock.Lock()
// defer a.activeLock.Unlock()
// if cur != a.active {
// return false, errors.Wrapf(SwapError, "Unexpected segment.")
// }
//
// newHead, _ := new.Head()
// curHead, _ := cur.Head()
// if newHead >= curHead {
// return true, nil
// }
//
// copied, _ := cur.Scan(newHead+1, curHead)
// new.Insert(copied)
// return true, nil
// }
//
// type amoebaSegment struct {
// snapshot  *amoebaSnapshot
// prevIndex int
// prevTerm  int
// head      int
// raw       amoeba.Index
// }
//
// func newAmoebaSegment(snapshot *amoebaSnapshot, prevIndex int, prevTerm int) *amoebaSegment {
// return &amoebaSegment{snapshot, prevIndex, prevTerm, prevIndex, amoeba.NewBTreeIndex(32)}
// }
//
// func (a *amoebaSegment) PrevIndex() int {
// return a.prevIndex
// }
//
// func (a *amoebaSegment) PrevTerm() int {
// return a.prevTerm
// }
//
// func (a *amoebaSegment) Snapshot() (StoredSnapshot, error) {
// return a.snapshot, nil
// }
//
// func (a *amoebaSegment) Head() (head int, err error) {
// a.raw.Read(func(u amoeba.View) {
// head = a.head
// })
// return
// }
//
// func (a *amoebaSegment) Get(index int) (item LogItem, ok bool, err error) {
// a.raw.Read(func(u amoeba.View) {
// item, ok = a.get(u, index)
// })
// return
// }
//
// func (a *amoebaSegment) Scan(beg int, end int) (batch []LogItem, err error) {
// a.raw.Read(func(u amoeba.View) {
// batch = make([]LogItem, 0, end-beg)
// u.ScanFrom(amoeba.IntKey(beg), func(s amoeba.Scan, k amoeba.Key, i interface{}) {
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
// func (a *amoebaSegment) Append(e Event, t int, s uuid.UUID, seq int, k int) (head int, err error) {
// a.raw.Update(func(u amoeba.Update) {
// head = a.head + 1
// u.Put(amoeba.IntKey(head), NewLogItem(head, e, t, s, seq, k))
// a.head = head
// })
// return
// }
//
// func (a *amoebaSegment) Insert(batch []LogItem) (head int, err error) {
// a.raw.Update(func(u amoeba.Update) {
// head = a.head
// for _, item := range batch {
// u.Put(amoeba.IntKey(item.Index), item)
// head = common.Max(item.Index, head)
// }
// a.head = head
// })
// return
// }
//
// func (a *amoebaSegment) Compact(until int, ch <-chan Event, size int, config []byte) (StoredSegment, error) {
//
// // create snapshot
// events, err := CollectEvents(ch, size)
// if err != nil {
// return nil, errors.Wrap(err, "Error collecting snapshot events")
// }
//
// // grab the last item.
// prev, ok, err := a.Get(until)
// if !ok || err != nil {
// return nil, common.Or(err, errors.Wrapf(AccessError, "Unable to compact to [%v].  It doesn't exist!", until))
// }
//
// return newAmoebaSegment(newAmoebaSnapshot(events, config), prev.Index, prev.Term), nil
// }
//
// func (a *amoebaSegment) Delete() error {
// return nil
// }
//
// func (d *amoebaSegment) get(u amoeba.View, index int) (item LogItem, ok bool) {
// val := u.Get(amoeba.IntKey(index))
// if val == nil {
// return
// }
//
// return val.(LogItem), true
// }
//
// type amoebaSnapshot struct {
// events []Event
// config []byte
// }
//
// func newAmoebaSnapshot(events []Event, config []byte) *amoebaSnapshot {
// return &amoebaSnapshot{events, config}
// }
//
// func (a *amoebaSnapshot) Size() int {
// return len(a.events)
// }
//
// func (a *amoebaSnapshot) Config() []byte {
// return a.config
// }
//
// func (a *amoebaSnapshot) Scan(beg int, end int) ([]Event, error) {
// end = common.Min(end, len(a.events))
// return a.events[beg : end+1], nil
// }
//
// func (a *amoebaSnapshot) Delete() error {
// return nil
// }
