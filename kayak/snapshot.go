package kayak

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	latestBucket    = []byte("kayak.latest.segment")
	segmentsBucket  = []byte("kayak.segments")
	itemsBucket     = []byte("kayak.seg.items")
	headsBucket     = []byte("kayak.heads")
	commitsBucket   = []byte("kayak.commits")
	snapshotsBucket = []byte("kayak.snapshots")
	eventsBucket    = []byte("kayak.events")
)

type durableLog struct {
	id uuid.UUID

	// the stash instanced used for durability
	stash stash.Stash

	// the currently active segment.
	active durableSegment

	// lock around the active segment.  Protects against concurrent updates during compactions.
	activeLock sync.RWMutex

	// // index of last item inserted. (initialized to -1)
	head *position

	// index of last item committed.(initialized to -1)
	commit *position

	// closing utilities.
	closed chan struct{}

	// closing lock.  (a buffered channel of 1 entry.)
	closer chan struct{}
}

func openLog(id uuid.UUID, stash stash.Stash) *durableLog {
	return nil
}

func (e *durableLog) Close() error {
	select {
	case <-e.closed:
		return ClosedError
	case e.closer <- struct{}{}:
	}

	close(e.closed)
	e.commit.Close()
	e.head.Close()
	return nil
}

func (e *durableLog) Closed() bool {
	select {
	case <-e.closed:
		return true
	default:
		return false
	}
}

func (e *durableLog) Committed() (pos int) {
	return e.commit.Get()
}

func (e *durableLog) Active() durableSegment {
	e.activeLock.RLock()
	defer e.activeLock.RUnlock()
	return e.active
}

func (e *eventLog) UpdateActive(fn func(s durableSegment) durableSegment) {
	e.activeLock.Lock()
	defer e.activeLock.Unlock()
	e.active = fn(e.active)
}


func setPos(bucket *bolt.Bucket, logId uuid.UUID, pos int) error {
	return bucket.Put(stash.NewUUIDKey(logId), stash.IntBytes(pos))
}

func getPos(bucket *bolt.Bucket, logId uuid.UUID) (int, error) {
	val := bucket.Get(stash.NewUUIDKey(logId))
	if val == nil {
		return -1, nil
	}

	return stash.ParseInt(val)
}

func parseItem(bytes []byte) (LogItem, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return LogItem{}, err
	}

	raw, err := readLogItem(msg)
	if err != nil {
		return LogItem{}, err
	}

	return raw.(LogItem), nil
}

func parseDurableSegment(bytes []byte) (*durableSegment, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return nil, err
	}

	raw, err := readDurableSegment(msg)
	if err != nil {
		return nil, err
	}

	return raw.(*durableSegment), nil
}

func parseDurableItem(bytes []byte) (durableItem, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return durableItem{}, err
	}

	raw, err := readDurableItem(msg)
	if err != nil {
		return durableItem{}, err
	}

	return raw.(durableItem), nil
}

func readDurableItem(r scribe.Reader) (interface{}, error) {
	var ret durableItem
	var err error
	err = common.Or(err, r.ReadUUID("segmentId", &ret.segmentId))
	err = common.Or(err, r.ParseMessage("raw", &ret.raw, readLogItem))
	return ret, err
}

type durableItem struct {
	segmentId uuid.UUID
	raw       LogItem
}

func (d durableItem) Write(w scribe.Writer) {
	w.WriteUUID("segmentId", d.segmentId)
	w.WriteMessage("raw", d.raw)
}

func (d durableItem) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func readDurableSegment(r scribe.Reader) (interface{}, error) {
	seg := &durableSegment{}
	err := r.ReadUUID("id", &seg.id)
	err = common.Or(err, r.ReadUUID("prevSnapshot", &seg.prevSnapshot))
	err = common.Or(err, r.ReadInt("prevIndex", &seg.prevIndex))
	err = common.Or(err, r.ReadInt("prevTerm", &seg.prevTerm))
	return seg, err
}

type durableSegment struct {
	id uuid.UUID

	prevSnapshot uuid.UUID
	prevIndex    int
	prevTerm     int
}

func initDurableSegment(tx *bolt.Tx) (durableSegment, error) {
	return createDurableSegment(tx, []Event{}, []byte{}, -1, -1)
}

func createDurableSegment(tx *bolt.Tx, snapshotEvents []Event, snapshotConfig []byte, prevIndex int, prevTerm int) (durableSegment, error) {
	snapshot, err := createDurableSnapshot(tx, snapshotEvents, snapshotConfig)
	if err != nil {
		return durableSegment{}, nil
	}

	return durableSegment{uuid.NewV1(), snapshot.id, prevIndex, prevTerm}, nil
}

func (d durableSegment) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteUUID("prevSnapshot", d.prevSnapshot)
	w.WriteInt("prevIndex", d.prevIndex)
	w.WriteInt("PrevTerm", d.prevTerm)
}

func (d durableSegment) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d durableSegment) Key() stash.Key {
	return stash.NewUUIDKey(d.id)
}

func (d durableSegment) Compact(tx *bolt.Tx, until int, events []Event, config []byte) (durableSegment, error) {
	item, found, err := d.Get(tx, until)
	if err != nil {
		return durableSegment{}, err
	}

	if !found {
		return durableSegment{}, errors.Wrapf(EventError, "Cannot compact. Until [%v] index doesn't exist.", until)
	}

	head, err := d.Head(tx)
	if err != nil {
		return durableSegment{}, err
	}

	// copy over any items after the new segment start,  (FIXME: Copy in chunks, so we don't exhaust memory)
	items, err := d.Scan(tx, until, head)
	if err != nil {
		return durableSegment{}, err
	}

	segment, err := createDurableSegment(tx, events, config, item.Index, item.term)
	if err != nil {
		return durableSegment{}, err
	}

	segment.Insert(tx, items)
	return segment, nil
}

func (d durableSegment) SetHead(tx *bolt.Tx, head int) error {
	return tx.Bucket(headsBucket).Put(d.Key(), stash.IntBytes(head))
}

func (d durableSegment) Head(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(headsBucket).Get(d.Key())
	if raw == nil {
		return 0, nil
	}

	return stash.ParseInt(raw)
}

func (d durableSegment) Get(tx *bolt.Tx, index int) (LogItem, bool, error) {
	val := tx.Bucket(itemsBucket).Get(d.Key().ChildInt(index))
	if val == nil {
		return LogItem{}, false, nil
	}
	item, err := parseDurableItem(val)
	if err != nil {
		return LogItem{}, false, err
	}
	return item.raw, true, nil
}

// Scan inclusive of start and end
func (d durableSegment) Scan(tx *bolt.Tx, start int, end int) (batch []LogItem, err error) {
	cursor := tx.Bucket(itemsBucket).Cursor()

	batch = make([]LogItem, 0, end-start)

	for _, v := cursor.Seek(d.Key().ChildInt(0).Raw()); v != nil; _, v = cursor.Next() {
		i, e := parseDurableItem(v)
		if e != nil {
			return []LogItem{}, e
		}

		if i.segmentId == d.id {
			batch = append(batch, i.raw)
		}
	}

	return batch, nil

}

func (d durableSegment) Append(tx *bolt.Tx, batch []Event, term int) (int, error) {
	index, err := d.Head(tx)
	if err != nil {
		return 0, nil
	}

	bucket := tx.Bucket(itemsBucket)
	for _, e := range batch {
		index++

		item := durableItem{d.id, newEventLogItem(index, term, e)}
		if err := bucket.Put(d.Key().ChildInt(index).Raw(), item.Bytes()); err != nil {
			return index, err
		}
	}

	return index, nil
}

func (d durableSegment) Insert(tx *bolt.Tx, batch []LogItem) (int, error) {
	index, err := d.Head(tx)
	if err != nil {
		return 0, nil
	}

	bucket := tx.Bucket(itemsBucket)
	for _, i := range batch {
		if err := bucket.Put(d.Key().ChildInt(i.Index).Raw(), durableItem{d.id, i}.Bytes()); err != nil {
			return index, err
		}

		if i.Index > index {
			index = i.Index
		}
	}

	return index, nil
}

func storeDurableSnapshot(tx *bolt.Tx, val durableSnapshot) error {
	snapshots := tx.Bucket(snapshotsBucket)
	return snapshots.Put(val.id.Bytes(), val.Bytes())
}

func storeDurableSnapshotEvents(tx *bolt.Tx, val durableSnapshot, snapshot []Event) error {
	events := tx.Bucket(eventsBucket)
	for i, e := range snapshot {
		if err := events.Put(val.Key().ChildInt(i).Raw(), e.Raw()); err != nil {
			return err
		}
	}
	return nil
}

func createDurableSnapshot(tx *bolt.Tx, snapshot []Event, config []byte) (durableSnapshot, error) {
	ret := durableSnapshot{uuid.NewV1(), len(snapshot), config}
	storeDurableSnapshot(tx, ret)
	storeDurableSnapshotEvents(tx, ret, snapshot)
	return ret, nil
}

func readDurableSnapshot(r scribe.Reader) (interface{}, error) {
	var ret durableSnapshot
	var err error
	err = r.ReadUUID("id", &ret.id)
	err = r.ReadInt("id", &ret.len)
	err = r.ReadBytes("config", &ret.config)
	return ret, err
}

type durableSnapshot struct {
	id     uuid.UUID
	len    int
	config []byte
}

func (d durableSnapshot) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteInt("len", d.len)
	w.WriteBytes("config", d.config)
}

func (d durableSnapshot) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d durableSnapshot) Key() stash.Key {
	return stash.NewUUIDKey(d.id)
}

func (d *durableSnapshot) Scan(tx *bolt.Tx, start int, end int) (batch []Event, err error) {
	cursor := tx.Bucket(eventsBucket).Cursor()

	batch = make([]Event, 0, end-start)

	cur := 0
	for _, v := cursor.Seek(d.Key().ChildInt(start).Raw()); v != nil; _, v = cursor.Next() {
		if cur > end {
			return batch, nil
		}

		batch = append(batch, Event(v))
		cur++
	}

	return batch, nil

}
