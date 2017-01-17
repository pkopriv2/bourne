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
	ExistsError      = errors.New("Kayak:SegmentExists")
	DeletedError     = errors.New("Kayak:DeletedError")
	EndOfStreamError = errors.New("Kayak:EndOfStream")
	OutOfBoundsError = errors.New("Kayak:OutOfBounds")
)

// BoltDB buckets.
var (
	segmentsBucket  = []byte("kayak.seg")
	itemsBucket     = []byte("kayak.seg.items")
	headsBucket     = []byte("kayak.seg.heads")
	commitsBucket   = []byte("kayak.log.commits")
	activeBucket    = []byte("kayak.log.actives")
	snapshotsBucket = []byte("kayak.snapshots")
	eventsBucket    = []byte("kayak.snapshots.events")
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

	// active (transient)
	active *durableSegment
}

func (d *durableLog) activeSegmentNum(tx *bolt.Tx) (int, error) {
	cur := tx.Bucket(activeBucket).Get(stash.UUID(d.id).Raw())
	return stash.ParseInt(cur)
}

func (d *durableLog) Active(tx *bolt.Tx) (durableSegment, error) {
	return durableSegment{}, nil
	// cur, err := d.activeSegmentNum(tx)
	// cur := tx.Bucket(activeBucket).Get(stash.UUID(d.id).Raw())
	// return durableSegment{}, nil
}

func (d *durableLog) SwapActive(tx *bolt.Tx, cur int, next int) (bool, error) {
	return false, nil
}

func (d *durableLog) Compact(until int, snapshot []Event, config []byte, cur int, next int) (bool, error) {
	return false, nil
	// seg, err := d.Active(tx)
	// if err != nil {
	// return false, err
	// }
	//
	// new, err := seg.CompactOnly(tx, until, snapshot, config)
	// if err != nil {
	// return false, err
	// }
	//
	// return d.SwapActive(tx, seg.num, new.num)
}

type durableSegment struct {
	id uuid.UUID

	prevSnapshot uuid.UUID
	prevIndex    int
	prevTerm     int
}

func initDurableSegment(db stash.Stash) (durableSegment, error) {
	return createDurableSegment(db, NewEventChannel([]Event{}), 0, []byte{}, -1, -1)
}

func createDurableSegment(db stash.Stash, snapshotEvents <-chan Event, snapshotSize int, snapshotConfig []byte, prevIndex int, prevTerm int) (seg durableSegment, err error) {
	snapshot, err := createDurableSnapshot(db, snapshotEvents, snapshotSize, snapshotConfig)
	if err != nil {
		return
	}
	defer common.RunIf(func() { snapshot.Delete(db) })(err)

	seg = durableSegment{uuid.NewV1(), snapshot.id, prevIndex, prevTerm}
	err = db.Update(func(tx *bolt.Tx) error {
		e := tx.Bucket(segmentsBucket).Put(seg.Key(), seg.Bytes())
		if e != nil {
			return e
		}

		return tx.Bucket(headsBucket).Put(seg.Key(), stash.Int(prevIndex).Raw())

	})
	return seg, err
}

func deleteDurableSegment(db stash.Stash, id uuid.UUID) error {
	err := deleteDurableSegmentItems(db, id)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		e := tx.Bucket(headsBucket).Delete(stash.UUID(id))
		e = common.Or(e, tx.Bucket(segmentsBucket).Delete(stash.UUID(id)))
		return e
	})
}

func deleteDurableSegmentItems(db stash.Stash, id uuid.UUID) (err error) {
	prefix := stash.UUID(id)

	for contd := true; contd; {
		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(itemsBucket)
			cursor := events.Cursor()

			dead := make([][]byte, 0, 1024)
			k, _ := cursor.Seek(prefix.ChildInt(0))
			for i := 0; i < 1024; i++ {
				if k == nil || !prefix.ParentOf(k) {
					contd = false
					break
				}

				dead = append(dead, k)
				k, _ = cursor.Next()
			}

			for _, i := range dead {
				if err := events.Delete(i); err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}
	return
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
	return stash.UUID(d.id)
}

// generates a new segment but does NOT edit the existing in any way

// this is NOT commutative, but rather last move wins.

func (d durableSegment) head(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(headsBucket).Get(d.Key())
	if raw == nil {
		return -1, DeletedError
	}
	return stash.ParseInt(raw)
}

func (d durableSegment) get(tx *bolt.Tx, index int) (LogItem, bool, error) {
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

func (d durableSegment) scan(tx *bolt.Tx, beg int, end int) (batch []LogItem, err error) {
	if beg <= d.prevIndex {
		return nil, errors.Wrapf(OutOfBoundsError, "Start of range [%v] less than minimal recoverable index [%v]", beg, d.prevIndex)
	}

	// prove log isn't empty
	head, err := d.head(tx)
	if err != nil {
		return nil, err
	}

	// prove log isn't empty
	if head - d.prevIndex == 0 {
		return []LogItem{}, nil
	}

	// prove batch will be empty
	end = common.Min(head, end)
	if end - beg < 0 {
		return []LogItem{}, nil
	}

	// init the batch
	cur, cursor, batch := beg, tx.Bucket(itemsBucket).Cursor(), make([]LogItem, 0, end-beg+1)
	for k, v := cursor.Seek(d.Key().ChildInt(cur).Raw()); v != nil && cur <= end; k, v = cursor.Next() {
		if !d.Key().ChildInt(cur).Equals(k) {
			return nil, errors.Wrapf(DeletedError, "Segment deleted [%v]", d.id) // this shouldn't be possible.
		}

		i, e := parseItem(v)
		if e != nil {
			return nil, e
		}

		batch = append(batch, i)
		cur++
	}

	// should NOT be possible to get a batch smaller than end-beg+1)
	return batch, nil
}

func (d durableSegment) setHead(tx *bolt.Tx, head int) error {
	return tx.Bucket(headsBucket).Put(d.Key(), stash.IntBytes(head))
}

func (d durableSegment) append(tx *bolt.Tx, batch []Event, term int) (int, error) {
	index, err := d.head(tx)
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

	d.setHead(tx, index)
	return index, nil
}

func (d durableSegment) insert(tx *bolt.Tx, batch []LogItem) error {
	if len(batch) == 0 {
		return nil
	}

	head, err := d.head(tx)
	if err != nil {
		return err
	}

	bucket := tx.Bucket(itemsBucket)

	var prev *int
	for _, i := range batch {
		if i.Index > head+1 || (prev != nil && i.Index > *prev+1) {
			return errors.Wrapf(OutOfBoundsError, "Illegal index [%v]. Log cannot have gaps. Head [%v]", i.Index, head)
		}

		if err := bucket.Put(d.Key().ChildInt(i.Index).Raw(), i.Bytes()); err != nil {
			return err
		}

		if i.Index > head {
			head = i.Index
		}

		prev = &i.Index
	}

	return d.setHead(tx, head)
}

func (d durableSegment) Head(db stash.Stash) (h int, e error) {
	db.View(func(tx *bolt.Tx) error {
		h, e = d.head(tx)
		return e
	})
	return
}

func (d durableSegment) Get(db stash.Stash, index int) (i LogItem, o bool, e error) {
	db.View(func(tx *bolt.Tx) error {
		i, o, e = d.get(tx, index)
		return e
	})
	return
}

// Scan inclusive of start and end
func (d durableSegment) Scan(db stash.Stash, start int, end int) (batch []LogItem, err error) {
	db.View(func(tx *bolt.Tx) error {
		batch, err = d.scan(tx, start, end)
		return err
	})
	return
}

func (d durableSegment) Append(db stash.Stash, batch []Event, term int) (head int, err error) {
	db.Update(func(tx *bolt.Tx) error {
		head, err = d.append(tx, batch, term)
		return err
	})
	return
}

func (d durableSegment) Insert(db stash.Stash, batch []LogItem) error {
	return db.Update(func(tx *bolt.Tx) error {
		return d.insert(tx, batch)
	})
}

func (d durableSegment) Snapshot(db stash.Stash) (durableSnapshot, error) {
	s, ok, err := openDurableSnapshot(db, d.prevSnapshot)
	if err != nil {
		return durableSnapshot{}, err
	}

	if !ok {
		return durableSnapshot{}, errors.Wrapf(DeletedError, "Segment deleted [%v,%v]", d.id)
	}

	return s, nil
}

func (d durableSegment) Delete(db stash.Stash) error {
	snapshot, err := d.Snapshot(db)
	if err != nil {
		return err
	}
	err = snapshot.Delete(db)
	if err != nil {
		return err
	}

	return deleteDurableSegment(db, d.id)
}

func (d durableSegment) CompactAndCopy(db stash.Stash, until int, events <-chan Event, num int, config []byte) (seg durableSegment, err error) {
	head, err := d.Head(db)
	if err != nil {
		return durableSegment{}, err
	}

	if until < d.prevIndex || until > head {
		return durableSegment{}, errors.Wrapf(OutOfBoundsError, "Cannot compact. Mark [%v] out of bounds", until)
	}

	if head - d.prevIndex <= 0 {
		return createDurableSegment(db, events, num, config, d.prevIndex, d.prevTerm)
	}

	prevItem, found, err := d.Get(db, until)
	if err != nil {
		return durableSegment{}, err
	}

	if !found {
		return durableSegment{}, errors.Wrapf(DeletedError, "Segment [%v] deleted.  Cannot compact", d.id)
	}

	seg, err = createDurableSegment(db, events, num, config, prevItem.Index, prevItem.term)
	if err != nil {
		return durableSegment{}, err
	}
	defer common.RunIf(func() {seg.Delete(db)})(err)

	// perform copy
	copied, err := d.Scan(db, until+1, head)
	if err != nil {
		return durableSegment{}, err
	}

	err = seg.Insert(db, copied)
	return seg, err
}

type durableSnapshot struct {
	id     uuid.UUID
	size   int
	config []byte
}

func createDurableSnapshot(db stash.Stash, ch <-chan Event, num int, config []byte) (ret durableSnapshot, err error) {
	ret = durableSnapshot{uuid.NewV1(), num, config}
	err = storeDurableSnapshotEvents(db, ret.id, num, ch)
	if err != nil {
		return
	}
	defer common.RunIf(func() { deleteDurableSnapshotEvents(db, ret.id) })(err)
	err = storeDurableSnapshot(db, ret)
	return
}

func openDurableSnapshot(db stash.Stash, id uuid.UUID) (s durableSnapshot, o bool, e error) {
	db.View(func(tx *bolt.Tx) error {
		s, o, e = parseDurableSnapshot(tx.Bucket(snapshotsBucket).Get(stash.UUID(id)))
		return e
	})
	return
}

func storeDurableSnapshot(db stash.Stash, val durableSnapshot) error {
	return db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotsBucket).Put(val.id.Bytes(), val.Bytes())
	})
}

func storeDurableSnapshotEvents(db stash.Stash, id uuid.UUID, num int, ch <-chan Event) (err error) {
	prefix := stash.UUID(id)

	for i := 0; i < num; {
		chunkStart := i
		chunk := make([]Event, 0, 1024)

		for cur := 0; cur < 1024 && i < num; cur, i = cur+1, i+1 {
			e, ok := <-ch
			if !ok {
				return EndOfStreamError
			}

			chunk = append(chunk, e)
		}

		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(eventsBucket)
			for j, e := range chunk {
				if err := events.Put(prefix.ChildInt(chunkStart+j).Raw(), e); err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func deleteDurableSnapshot(db stash.Stash, id uuid.UUID) error {
	err := deleteDurableSnapshotEvents(db, id)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotsBucket).Delete(stash.UUID(id))
	})
}

func deleteDurableSnapshotEvents(db stash.Stash, id uuid.UUID) (err error) {
	prefix := stash.UUID(id)

	for contd := true; contd; {
		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(eventsBucket)
			cursor := events.Cursor()

			dead := make([][]byte, 0, 1024)
			k, _ := cursor.Seek(prefix.ChildInt(0))
			for i := 0; i < 1024; i++ {
				if k == nil || !prefix.ParentOf(k) {
					contd = false
					break
				}

				dead = append(dead, k)
				k, _ = cursor.Next()
			}

			for _, i := range dead {
				if err := events.Delete(i); err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}
	return
}

func readDurableSnapshot(r scribe.Reader) (interface{}, error) {
	var ret durableSnapshot
	var err error
	err = r.ReadUUID("id", &ret.id)
	err = r.ReadInt("size", &ret.size)
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
	w.WriteInt("size", d.size)
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

func (d durableSnapshot) Scan(db stash.Stash, start int, end int) (batch []Event, err error) {
	batch = make([]Event, 0, end-start+1)
	err = db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(eventsBucket).Cursor()

		var cur int
		end = common.Min(end, d.size-1)
		cur = start
		for k, v := cursor.Seek(d.Key().ChildInt(start).Raw()); v != nil; k, v = cursor.Next() {
			if cur > end {
				return nil
			}

			if !d.Key().ChildInt(cur).Equals(k) {
				return errors.Wrapf(DeletedError, "Snapshot deleted [%v]", d.id) // already deleted
			}

			batch = append(batch, Event(v))
			cur++
		}
		return nil
	})

	if len(batch) != end-start+1 {
		return nil, errors.Wrapf(DeletedError, "Snapshot deleted [%v]", d.id) // already deleted
	}
	return
}

func (d durableSnapshot) Delete(db stash.Stash) error {
	return deleteDurableSnapshot(db, d.id)
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
