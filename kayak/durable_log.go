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
	DeletedError     = errors.New("Kayak:DeletedError")
	EndOfStreamError = errors.New("Kayak:EndOfStream")
	OutOfBoundsError = errors.New("Kayak:OutOfBounds")
	InvariantError   = errors.New("Kayak:InvariantError")
)

// Db buckets.
var (
	segBucket            = []byte("kayak.seg")
	segItemsBucket       = []byte("kayak.seg.items")
	segHeadsBucket       = []byte("kayak.seg.heads")
	logCommitBucket      = []byte("kayak.log.commits")
	logActiveSegBucket   = []byte("kayak.log.actives")
	snapshotsBucket      = []byte("kayak.snapshots")
	snapshotEventsBucket = []byte("kayak.snapshots.events")
)

func initBuckets(tx *bolt.Tx) (err error) {
	var e error
	_, e = tx.CreateBucketIfNotExists(segBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(logActiveSegBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(segItemsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(segHeadsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(snapshotsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(snapshotEventsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(logCommitBucket)
	err = common.Or(err, e)
	return
}

// Invariants:
// * log.activeId will be set for a non deleted log.
// * log.active segment will exist for non deleted log.
// * log.commit will be set for a non deleted log.
//

type durableLog struct {
	id uuid.UUID
}

func setActiveSegmentId(tx *bolt.Tx, logId uuid.UUID, segmentId uuid.UUID) error {
	return tx.Bucket(logActiveSegBucket).Put(stash.UUID(logId), stash.UUID(segmentId))
}

func getActiveSegmentId(tx *bolt.Tx, logId uuid.UUID) (uuid.UUID, bool, error) {
	cur := tx.Bucket(logActiveSegBucket).Get(stash.UUID(logId))
	if cur == nil {
		return uuid.UUID{}, false, nil
	}

	id, err := uuid.FromBytes(cur)
	if err != nil {
		return uuid.UUID{}, false, err
	}

	return id, true, nil
}

func deleteActiveSegmentId(tx *bolt.Tx, logId uuid.UUID) error {
	return tx.Bucket(logActiveSegBucket).Delete(stash.UUID(logId))
}

func getCommit(tx *bolt.Tx, logId uuid.UUID) (int, bool, error) {
	val := tx.Bucket(logCommitBucket).Get(stash.UUID(logId))
	if val == nil {
		return -1, false, nil
	}

	i, err := stash.ParseInt(val)
	if err != nil {
		return -1, false, err
	}

	return i, true, nil
}

func setCommit(tx *bolt.Tx, logId uuid.UUID, pos int) (int, error) {
	cur, ok, err := getCommit(tx, logId)
	if err != nil {
		return 0, err
	}

	if ok && cur >= pos {
		return cur, nil
	}

	return pos, tx.Bucket(logCommitBucket).Put(stash.UUID(logId), stash.Int(pos))
}

func deleteCommit(tx *bolt.Tx, logId uuid.UUID) error {
	return tx.Bucket(logCommitBucket).Delete(stash.UUID(logId))
}

func openDurableLog(db stash.Stash, logId uuid.UUID) (log durableLog, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		err := initBuckets(tx)
		if err != nil {
			return err
		}

		_, found, err := getActiveSegmentId(tx, logId)
		if err != nil {
			return err
		}

		if found {
			log = durableLog{logId}
			return nil
		}

		seg, err := createEmptySegment(tx)
		if err != nil {
			return err
		}

		_, err = setCommit(tx, logId, -1)
		if err != nil {
			return errors.Wrapf(InvariantError, "Error setting commit [%v]: -1", logId)
		}
		defer common.RunIf(func() { deleteCommit(tx, logId) })(err)

		err = setActiveSegmentId(tx, logId, seg.id)
		if err != nil {
			return errors.Wrapf(InvariantError, "Error setting active segment [%v]: %v", logId, seg.id)
		}
		defer common.RunIf(func() { deleteActiveSegmentId(tx, logId) })(err)

		log = durableLog{logId}
		return nil
	})
	return
}

func deleteDurableLog(db stash.Stash, id uuid.UUID) error {
	return nil
}

func (d durableLog) setActiveSegmentId(tx *bolt.Tx, id uuid.UUID) error {
	return setActiveSegmentId(tx, d.id, id)
}

func (d durableLog) getActiveSegmentId(tx *bolt.Tx) (uuid.UUID, error) {
	s, ok, e := getActiveSegmentId(tx, d.id)
	if e != nil || !ok {
		return uuid.UUID{}, common.Or(e, errors.Wrapf(DeletedError, "Log [%v] dropped.", d.id))
	}

	return s, nil
}

func (d durableLog) setCommit(tx *bolt.Tx, pos int) (int, error) {
	return setCommit(tx, d.id, pos)
}

func (d durableLog) getCommit(tx *bolt.Tx) (int, error) {
	i, ok, e := getCommit(tx, d.id)
	if e != nil || !ok {
		return -1, common.Or(e, errors.Wrapf(DeletedError, "Log [%v] dropped.", d.id))
	}

	return i, e
}

func (d durableLog) active(tx *bolt.Tx) (durableSegment, error) {
	id, err := d.getActiveSegmentId(tx)
	if err != nil {
		return durableSegment{}, err
	}

	seg, found, err := openDurableSegment(tx, id)
	if err != nil || !found {
		return durableSegment{}, common.Or(err, errors.Wrapf(DeletedError, "Log [%v] deleted."))
	}
	return seg, nil
}

func (d durableLog) swapActive(tx *bolt.Tx, cur durableSegment, new durableSegment) (bool, error) {
	id, err := d.getActiveSegmentId(tx)
	if err != nil {
		return false, err
	}

	if id != cur.id {
		return false, nil
	}

	if new.prevIndex < cur.prevIndex {
		return false, errors.Wrapf(InvariantError, "New segment [%v, %v] is older than current. [%v]", new.id, new.prevIndex, cur.prevIndex)
	}

	// copy over items
	curHead, err := cur.head(tx)
	if err != nil {
		return false, err
	}

	newHead, err := new.head(tx)
	if err != nil {
		return false, err
	}

	if newHead >= curHead {
		return true, d.setActiveSegmentId(tx, new.id)
	}

	copies, err := cur.scan(tx, newHead+1, curHead)
	if err != nil {
		return false, err
	}

	_, err = new.insert(tx, copies)
	if err != nil {
		return false, err
	}

	err = d.setActiveSegmentId(tx, new.id)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (d durableLog) GetCommit(db stash.Stash) (pos int, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		pos, err = d.getCommit(tx)
		return err
	})
	return
}

func (d durableLog) SetCommit(db stash.Stash, pos int) (new int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		new, err = d.setCommit(tx, pos)
		return err
	})
	return
}

func (d durableLog) Active(db stash.Stash) (seg durableSegment, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		seg, err = d.active(tx)
		return err
	})
	return
}

func (d durableLog) SwapActive(db stash.Stash, cur durableSegment, next durableSegment) (ok bool, err error) {
	db.Update(func(tx *bolt.Tx) error {
		ok, err = d.swapActive(tx, cur, next)
		return err
	})
	return
}

func (d durableLog) Compact(db stash.Stash, until int, events <-chan Event, num int, config []byte) error {
	cur, err := d.Active(db)
	if err != nil {
		return err
	}

	new, err := cur.CopyAndCompact(db, until, events, num, config)
	if err != nil {
		return err
	}

	ok, err := d.SwapActive(db, cur, new)
	if err != nil || !ok {
		return common.Or(err, errors.Wrapf(InvariantError, "Concurrent compaction resulting in shortened log."))
	}

	defer cur.Delete(db)
	return nil // err on delete suppressed.
}

func (d durableLog) Delete(db stash.Stash) (bool, error) {
	return false, nil
	// cur, err := d.Active(db)
	// if err != nil {
	// return false, err
	// }
	//
	// new, err := cur.CompactAndCopy(db, until, events, num, config)
	// if err != nil {
	// return false, err
	// }
	//
	// return d.SwapActive(db, new)
}

type durableSegment struct {
	id uuid.UUID

	prevSnapshot uuid.UUID
	prevIndex    int
	prevTerm     int
}

func createEmptySegment(tx *bolt.Tx) (ret durableSegment, err error) {
	var s durableSnapshot

	s, err = createEmptySnapshot(tx)
	if err != nil {
		return
	}

	ret = durableSegment{uuid.NewV1(), s.id, -1, -1}
	err = tx.Bucket(segBucket).Put(ret.Key(), ret.Bytes())
	err = tx.Bucket(segHeadsBucket).Put(ret.Key(), stash.Int(-1))
	return
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
		head := prevIndex
		if prevIndex > -1 {
			head = prevIndex+1
		}

		if e := tx.Bucket(segHeadsBucket).Put(seg.Key(), stash.Int(head)); e != nil {
			return e
		}

		if e := tx.Bucket(segBucket).Put(seg.Key(), seg.Bytes()); e != nil {
			return e
		}

		return nil
	})
	return
}

func deleteDurableSegment(db stash.Stash, id uuid.UUID) error {
	err := deleteDurableSegmentItems(db, id)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		e := tx.Bucket(segHeadsBucket).Delete(stash.UUID(id))
		e = common.Or(e, tx.Bucket(segBucket).Delete(stash.UUID(id)))
		return e
	})
}

func deleteDurableSegmentItems(db stash.Stash, id uuid.UUID) (err error) {
	prefix := stash.UUID(id)

	for contd := true; contd; {
		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(segItemsBucket)
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

func openDurableSegment(tx *bolt.Tx, id uuid.UUID) (durableSegment, bool, error) {
	val := tx.Bucket(segBucket).Get(stash.UUID(id))
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

func (d durableSegment) assert(tx *bolt.Tx, index int, term int) (bool, error) {
	if index == d.prevIndex {
		return d.prevTerm == term, nil
	}

	item, ok, err := d.get(tx, index)
	if err != nil || ! ok {
		return false, err
	}

	return item.term == term, nil
}

func (d durableSegment) head(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(segHeadsBucket).Get(d.Key())
	if raw == nil {
		return -1, DeletedError
	}
	return stash.ParseInt(raw)
}

func (d durableSegment) get(tx *bolt.Tx, index int) (LogItem, bool, error) {
	val := tx.Bucket(segItemsBucket).Get(d.Key().ChildInt(index))
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
	if head-d.prevIndex == 0 {
		return []LogItem{}, nil
	}

	// prove batch will be empty
	end = common.Min(head, end)
	if end-beg < 0 {
		return []LogItem{}, nil
	}

	// init the batch
	cur, cursor, batch := beg, tx.Bucket(segItemsBucket).Cursor(), make([]LogItem, 0, end-beg+1)
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

func (d durableSegment) setHead(tx *bolt.Tx, pos int) error {
	cur, err := d.head(tx)
	if err != nil {
		return err
	}

	if cur >= pos {
		return nil
	}

	return tx.Bucket(segHeadsBucket).Put(d.Key(), stash.IntBytes(pos))
}

func (d durableSegment) append(tx *bolt.Tx, batch []Event, term int) (int, error) {
	index, err := d.head(tx)
	if err != nil {
		return 0, err
	}

	bucket := tx.Bucket(segItemsBucket)
	for _, e := range batch {
		index++

		item := newEventLogItem(index, term, e)
		if err := bucket.Put(d.Key().ChildInt(index).Raw(), item.Bytes()); err != nil {
			return index, err
		}
	}

	if err := d.setHead(tx, index); err != nil {
		return 0, err
	}

	return index, nil
}

func (d durableSegment) insert(tx *bolt.Tx, batch []LogItem) (int, error) {

	head, err := d.head(tx)
	if err != nil {
		return 0, err
	}

	if len(batch) == 0 {
		return head, nil
	}

	bucket := tx.Bucket(segItemsBucket)

	prev := d.prevIndex
	for _, i := range batch {
		if i.Index > head+1 {
			return 0, errors.Wrapf(OutOfBoundsError, "Illegal index [%v]. Item index is greater than head+1. Head [%v]", i.Index, head)
		}

		if prev > d.prevIndex && i.Index != prev+1 {
			return 0, errors.Wrapf(OutOfBoundsError, "Illegal index [%v]. Items must be inserted contiguously. Prev [%v]", i.Index, prev)
		}

		if err := bucket.Put(d.Key().ChildInt(i.Index).Raw(), i.Bytes()); err != nil {
			return 0, err
		}

		prev = i.Index
		if i.Index > head {
			head = i.Index
		}
	}

	if err := d.setHead(tx, head); err != nil {
		return 0, err
	}

	return head, nil
}

func (d durableSegment) Assert(db stash.Stash, i int, t int) (b bool, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		b, e = d.assert(tx,i,t)
		return e
	})
	return
}

func (d durableSegment) Head(db stash.Stash) (h int, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		h, e = d.head(tx)
		return e
	})
	return
}

func (d durableSegment) Get(db stash.Stash, index int) (i LogItem, o bool, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		i, o, e = d.get(tx, index)
		return e
	})
	return
}

// Scan inclusive of start and end
func (d durableSegment) Scan(db stash.Stash, start int, end int) (batch []LogItem, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		batch, err = d.scan(tx, start, end)
		return err
	})
	return
}

func (d durableSegment) Append(db stash.Stash, batch []Event, term int) (head int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		head, err = d.append(tx, batch, term)
		return err
	})
	return
}

func (d durableSegment) Insert(db stash.Stash, batch []LogItem) (head int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		head, err = d.insert(tx, batch)
		return err
	})
	return
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

func (d durableSegment) CopyAndCompact(db stash.Stash, until int, events <-chan Event, num int, config []byte) (seg durableSegment, err error) {
	head, err := d.Head(db)
	if err != nil {
		return durableSegment{}, err
	}

	if until < d.prevIndex || until > head {
		return durableSegment{}, errors.Wrapf(OutOfBoundsError, "Cannot compact. Mark [%v] out of bounds", until)
	}

	if head-d.prevIndex <= 0 {
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
	defer common.RunIf(func() { seg.Delete(db) })(err)

	// perform copy
	copied, err := d.Scan(db, until+1, head)
	if err != nil {
		return durableSegment{}, err
	}

	_, err = seg.Insert(db, copied)
	return seg, err
}

type durableSnapshot struct {
	id     uuid.UUID
	size   int
	config []byte
}

func createEmptySnapshot(tx *bolt.Tx) (ret durableSnapshot, err error) {
	ret = durableSnapshot{uuid.NewV1(), 0, []byte{}}
	err = tx.Bucket(snapshotsBucket).Put(ret.Key(), ret.Bytes())
	return
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
			events := tx.Bucket(snapshotEventsBucket)
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
			events := tx.Bucket(snapshotEventsBucket)
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
		cursor := tx.Bucket(snapshotEventsBucket).Cursor()

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
