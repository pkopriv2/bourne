package kayak

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Public Error Types
var (
	ExistsError  = errors.New("Kayak:SegmentExists")
	DeletedError = errors.New("Kayak:DeletedError")
)

// BoltDB buckets.
var (
	segmentsBucket  = []byte("kayak.seg")
	activeBucket    = []byte("kayak.seg.active")
	itemsBucket     = []byte("kayak.seg.items")
	headsBucket     = []byte("kayak.seg.heads")
	snapshotsBucket = []byte("kayak.seg.snapshots")
	eventsBucket    = []byte("kayak.seg.snapshots.events")
	commitsBucket   = []byte("kayak.log.commits")
	clocksBucket    = []byte("kayak.clock.commits")
)

func initBuckets(tx *bolt.Tx) (err error) {
	var e error
	_, e = tx.CreateBucketIfNotExists(segmentsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(activeBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(itemsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(headsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(snapshotsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(eventsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(commitsBucket)
	err = common.Or(err, e)
	return
}

type durableLog struct {
	id uuid.UUID
}

func (d durableLog) start() error {
	return nil
}

func (d durableLog) Active(tx *bolt.Tx) (durableSegment, error) {
	return durableSegment{}, nil
}

func (d durableLog) SwapActive(tx *bolt.Tx, cur int, next int) (bool, error) {
	return false, nil
}

type durableSegment struct {
	id uuid.UUID

	num int

	prevSnapshot uuid.UUID
	prevIndex    int
	prevTerm     int
}

func initDurableSegment(tx *bolt.Tx, logId uuid.UUID) (durableSegment, error) {
	return createDurableSegment(tx, logId, 0, []Event{}, []byte{}, -1, -1)
}

func createDurableSegment(tx *bolt.Tx, logId uuid.UUID, num int, snapshotEvents []Event, snapshotConfig []byte, prevIndex int, prevTerm int) (durableSegment, error) {
	snapshot, err := createDurableSnapshot(tx, snapshotEvents, snapshotConfig)
	if err != nil {
		return durableSegment{}, err
	}

	bucket := tx.Bucket(segmentsBucket)

	seg := durableSegment{logId, num, snapshot.id, prevIndex, prevTerm}
	cur := bucket.Get(seg.Key())
	if cur != nil {
		return durableSegment{}, ExistsError
	}

	return seg, bucket.Put(seg.Key(), seg.Bytes())
}

func openDurableSegment(tx *bolt.Tx, logId uuid.UUID, num int) (durableSegment, bool, error) {
	val := tx.Bucket(segmentsBucket).Get(stash.UUID(logId).ChildInt(num))
	return parseDurableSegment(val)
}

func parseDurableSegment(bytes []byte) (durableSegment, bool, error) {
	if bytes == nil {
		return durableSegment{}, false, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return durableSegment{}, false, errors.Wrapf(err, "Error parsing scribe message")
	}

	raw, err := readDurableSegment(msg)
	if err != nil {
		return durableSegment{}, false, errors.Wrapf(err, "Error reading durable segment")
	}

	return raw.(durableSegment), true, nil
}

func readDurableSegment(r scribe.Reader) (interface{}, error) {
	seg := durableSegment{}
	err := r.ReadUUID("id", &seg.id)
	err = common.Or(err, r.ReadUUID("prevSnapshot", &seg.prevSnapshot))
	err = common.Or(err, r.ReadInt("prevIndex", &seg.prevIndex))
	err = common.Or(err, r.ReadInt("prevTerm", &seg.prevTerm))
	return seg, err
}

func (d durableSegment) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteUUID("prevSnapshot", d.prevSnapshot)
	w.WriteInt("prevIndex", d.prevIndex)
	w.WriteInt("prevTerm", d.prevTerm)
}

func (d durableSegment) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d durableSegment) Key() stash.Key {
	return stash.UUID(d.id).ChildInt(d.num)
}

// generates a new segment but does NOT edit the existing in any way
func (d durableSegment) CompactOnly(tx *bolt.Tx, until int, events []Event, config []byte) (durableSegment, error) {
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

	segment, err := createDurableSegment(tx, d.id, d.num+1, events, config, item.Index, item.term)
	if err != nil {
		return durableSegment{}, err
	}

	segment.Insert(tx, items)
	return segment, nil
}

// this is NOT commutative, but rather last move wins.
func (d durableSegment) MoveHead(tx *bolt.Tx, head int) error {
	return tx.Bucket(headsBucket).Put(d.Key(), stash.IntBytes(head))
}

func (d durableSegment) Head(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(headsBucket).Get(d.Key())
	if raw == nil {
		return -1, nil
	}

	return stash.ParseInt(raw)
}

func (d durableSegment) Snapshot(tx *bolt.Tx) (durableSnapshot, error) {
	snapshot, ok, err := openDurableSnapshot(tx, d.prevSnapshot)
	if err != nil {
		return durableSnapshot{}, err
	}

	if !ok {
		return durableSnapshot{}, errors.Wrapf(DeletedError, "Segment deleted [%v,%v]", d.id)
	}

	return snapshot, nil
}

func (d durableSegment) Get(tx *bolt.Tx, index int) (LogItem, bool, error) {
	val := tx.Bucket(itemsBucket).Get(d.Key().ChildInt(index))
	if val == nil {
		return LogItem{}, false, nil
	}

	item, err := parseItem(val)
	if err != nil {
		return LogItem{}, false, err
	}

	return item, true, nil
}

// Scan inclusive of start and end
func (d durableSegment) Scan(tx *bolt.Tx, start int, end int) (batch []LogItem, err error) {
	cursor := tx.Bucket(itemsBucket).Cursor()

	if start <= d.prevIndex {
		return nil, SlowConsumerError
	}

	head, err := d.Head(tx)
	if err != nil {
		return nil, err
	}

	if start > head {
		return []LogItem{}, nil
	}

	// start scanning
	var cur int
	cur = start
	end = common.Min(end, end)

	batch = make([]LogItem, 0, end-start+1)
	for k, v := cursor.Seek(d.Key().ChildInt(cur).Raw()); v != nil && cur <= end; k, v = cursor.Next() {
		if !d.Key().ChildInt(cur).Equals(k) {
			return nil, errors.Wrapf(DeletedError, "Segment deleted [%v]", d.id)
		}

		i, e := parseItem(v)
		if e != nil {
			return nil, e
		}

		batch = append(batch, i)
		cur++
	}

	if len(batch) == 0 {
		return nil, errors.Wrapf(DeletedError, "Segment deleted [%v]", d.id)
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

		item := newEventLogItem(index, term, e)
		if err := bucket.Put(d.Key().ChildInt(index).Raw(), item.Bytes()); err != nil {
			return index, err
		}
	}

	d.MoveHead(tx, index)
	return index, nil
}

// expects a continguous batch, but doesn't currently enforce.
func (d durableSegment) Insert(tx *bolt.Tx, batch []LogItem) (int, error) {
	index, err := d.Head(tx)
	if err != nil {
		return 0, nil
	}

	bucket := tx.Bucket(itemsBucket)
	for _, i := range batch {
		if err := bucket.Put(d.Key().ChildInt(i.Index).Raw(), i.Bytes()); err != nil {
			return index, err
		}

		if i.Index > index {
			index = i.Index
		}
	}

	d.MoveHead(tx, index)
	return index, nil
}

func (d durableSegment) Delete(tx *bolt.Tx) error {
	items := tx.Bucket(itemsBucket)

	// the head position
	head, err := d.Head(tx)
	if err != nil {
		return err
	}

	// delete all items (batches of 1024)
	cursor := items.Cursor()
	for i := 0; i < head; {
		dead := make([][]byte, 0, 1024)

		k, _ := cursor.Seek(d.Key().ChildInt(i))
		for cur := 0; i < head && cur < 1024; i, cur = i+1, cur+1 {
			dead = append(dead, k)
			k, _ = cursor.Next()
		}

		for _, k := range dead {
			if err := items.Delete(k); err != nil {
				return err
			}
		}
	}

	// delete the previous snapshot
	snapshot, err := d.Snapshot(tx)
	if err != nil {
		return err
	}

	if err := snapshot.Delete(tx); err != nil {
		return err
	}

	// finally, delete the segment itself
	return tx.Bucket(snapshotsBucket).Delete(d.Key())
}

type durableSnapshot struct {
	id     uuid.UUID
	len    int
	config []byte
}

func createDurableSnapshot(tx *bolt.Tx, snapshot []Event, config []byte) (durableSnapshot, error) {
	ret := durableSnapshot{uuid.NewV1(), len(snapshot), config}
	storeDurableSnapshot(tx, ret)
	storeDurableSnapshotEvents(tx, ret, snapshot)
	return ret, nil
}

func openDurableSnapshot(tx *bolt.Tx, id uuid.UUID) (durableSnapshot, bool, error) {
	snapshots := tx.Bucket(snapshotsBucket)
	return parseDurableSnapshot(snapshots.Get(stash.UUID(id)))
}

func storeDurableSnapshot(tx *bolt.Tx, val durableSnapshot) error {
	snapshots := tx.Bucket(snapshotsBucket)
	return snapshots.Put(val.id.Bytes(), val.Bytes())
}

func storeDurableSnapshotEvents(tx *bolt.Tx, val durableSnapshot, snapshot []Event) error {
	events := tx.Bucket(eventsBucket)
	for i, e := range snapshot {
		if err := events.Put(val.Key().ChildInt(i).Raw(), e); err != nil {
			return err
		}
	}
	return nil
}

func readDurableSnapshot(r scribe.Reader) (interface{}, error) {
	var ret durableSnapshot
	var err error
	err = r.ReadUUID("id", &ret.id)
	err = r.ReadInt("id", &ret.len)
	err = r.ReadBytes("config", &ret.config)
	return ret, err
}

func parseDurableSnapshot(bytes []byte) (durableSnapshot, bool, error) {
	if bytes == nil {
		return durableSnapshot{}, false, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return durableSnapshot{}, false, err
	}

	raw, err := readDurableSnapshot(msg)
	if err != nil {
		return durableSnapshot{}, false, err
	}

	return raw.(durableSnapshot), true, nil
}

func (d durableSnapshot) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteInt("len", d.len)
	w.WriteBytes("config", d.config)
}

func (d durableSnapshot) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d durableSnapshot) String() string {
	return fmt.Sprintf("Snapshot(%v)", d.id.String()[0:8])
}

func (d durableSnapshot) Config() []byte {
	return d.config
}

func (d durableSnapshot) Key() stash.Key {
	return stash.UUID(d.id)
}

func (d durableSnapshot) Events(tx *bolt.Tx) (batch []Event, err error) {
	if d.len == 0 {
		return []Event{}, nil
	}

	return d.Scan(tx, 0, d.len-1)
}

func (d durableSnapshot) Scan(tx *bolt.Tx, start int, end int) (batch []Event, err error) {
	cursor := tx.Bucket(eventsBucket).Cursor()

	var cur int
	end = common.Min(end, d.len-1)
	cur = start

	batch = make([]Event, 0, end-start+1)
	for k, v := cursor.Seek(d.Key().ChildInt(start).Raw()); v != nil; k, v = cursor.Next() {
		if cur > end {
			return batch, nil
		}

		if !d.Key().ChildInt(cur).Equals(k) {
			return nil, errors.Wrapf(DeletedError, "Snapshot deleted [%v]", d.id) // already deleted
		}

		batch = append(batch, Event(v))
		cur++
	}

	if len(batch) != end-start+1 {
		return nil, errors.Wrapf(DeletedError, "Snapshot deleted [%v]", d.id) // already deleted
	}

	return batch, nil
}

func (d durableSnapshot) Delete(tx *bolt.Tx) error {
	bucket := tx.Bucket(eventsBucket)

	// delete all events (batches of 1024)
	cursor := bucket.Cursor()
	for i := 0; i < d.len; {
		dead := make([][]byte, 0, 1024)

		k, _ := cursor.Seek(d.Key().ChildInt(i))
		for cur := 0; i < d.len && cur < 1024; i, cur = i+1, cur+1 {
			if !d.Key().ChildInt(cur).Equals(k) {
				return errors.Wrapf(DeletedError, "Snapshot deleted [%v]", d.id) // already deleted
			}

			dead = append(dead, k)
			k, _ = cursor.Next()
		}

		for _, k := range dead {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
	}

	// finally, delete the snapshot itself
	return tx.Bucket(snapshotsBucket).Delete(d.Key())
}
//
// func setActiveSegment(bucket *bolt.Bucket, logId uuid.UUID, activeId uuid.UUID) error {
	// return bucket.Put(stash.UUID(logId), stash.UUID(activeId))
// }
//
// func getActiveSegment(bucket *bolt.Bucket, logId uuid.UUID) (durableSegment, error) {
	// raw := bucket.Get(stash.UUID(logId))
	// if raw == nil {
		// return durableSegment{id: logId, prevIndex: -1, prevTerm: -1}, nil
	// }
//
	// return parseDurableSegment(raw)
// }

func setPos(bucket *bolt.Bucket, logId uuid.UUID, pos int) error {
	return bucket.Put(stash.UUID(logId), stash.IntBytes(pos))
}

func getPos(bucket *bolt.Bucket, logId uuid.UUID) (int, error) {
	val := bucket.Get(stash.UUID(logId))
	if val == nil {
		return -1, nil
	}

	return stash.ParseInt(val)
}
