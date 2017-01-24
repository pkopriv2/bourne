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

// Bolt implementation of kayak log store.
var (
	logBucket            = []byte("kayak.log")
	logCommitBucket      = []byte("kayak.log.commits")
	logActiveSegBucket   = []byte("kayak.log.actives")
	segBucket            = []byte("kayak.seg")
	segItemsBucket       = []byte("kayak.seg.items")
	segHeadsBucket       = []byte("kayak.seg.heads")
	snapshotsBucket      = []byte("kayak.snapshots")
	snapshotEventsBucket = []byte("kayak.snapshots.events")
)

func initBoltBuckets(tx *bolt.Tx) (err error) {
	var e error
	_, e = tx.CreateBucketIfNotExists(logBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(logActiveSegBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(logCommitBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(segBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(segItemsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(segHeadsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(snapshotsBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(snapshotEventsBucket)
	err = common.Or(err, e)
	return
}


// Store impl.
type boltStore struct {
	db *bolt.DB
}

func NewBoltStore(db *bolt.DB) LogStore {
	return &boltStore{db}
}

func (s *boltStore) Get(id uuid.UUID) (StoredLog, error) {
	return openBoltLog(s.db, id)
}

func (s *boltStore) New(id uuid.UUID, config []byte) (StoredLog, error) {
	return createBoltLog(s.db, id, config)
}

// Parent log abstraction
type boltLog struct {
	db  *bolt.DB
	raw logDat
}

func createBoltLog(db *bolt.DB, id uuid.UUID, config []byte) (*boltLog, error) {
	raw, err := createBoltLogDat(db, id, config)
	if err != nil {
		return nil, err
	}

	return &boltLog{db, raw}, nil
}

func openBoltLog(db *bolt.DB, id uuid.UUID) (*boltLog, error) {
	raw, ok, err := openBoltLogDat(db, id)
	if err != nil || !ok {
		return nil, err
	}

	return &boltLog{db, raw}, nil
}

func (b *boltLog) Id() uuid.UUID {
	return b.raw.id
}

func (b *boltLog) Active() (StoredSegment, error) {
	id, err := b.raw.Active(b.db)
	if err != nil {
		return nil, err
	}

	return openBoltSegment(b.db, id)
}

func (b *boltLog) GetCommit() (int, error) {
	return b.raw.GetCommit(b.db)
}

func (b *boltLog) SetCommit(pos int) (int, error) {
	return b.raw.SetCommit(b.db, pos)
}

func (b *boltLog) Swap(cur StoredSegment, new StoredSegment) (bool, error) {
	return b.raw.SwapActive(b.db, cur.(*boltSegment), new.(*boltSegment))
}

type logDat struct {
	id uuid.UUID
}

func (d logDat) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
}

func (d logDat) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func readLog(r scribe.Reader) (l logDat, e error) {
	e = r.ReadUUID("id", &l.id)
	return
}

func parseLog(bytes []byte) (l logDat, o bool, e error) {
	if bytes == nil {
		return
	}

	var msg scribe.Message
	msg, e = scribe.Parse(bytes)
	if e != nil {
		return
	}

	l, e = readLog(msg)
	return
}

func openBoltLogDat(db stash.Stash, logId uuid.UUID) (l logDat, o bool, e error) {
	e = db.Update(func(tx *bolt.Tx) error {
		if e := initBoltBuckets(tx); e != nil {
			return e
		}

		l, o, e = parseLog(tx.Bucket(logBucket).Get(stash.UUID(logId)))
		return e
	})
	return
}

func createBoltLogDat(db *bolt.DB, logId uuid.UUID, config []byte) (log logDat, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		if e := initBoltBuckets(tx); e != nil {
			return e
		}

		if _, ok, e := parseLog(tx.Bucket(logBucket).Get(stash.UUID(logId))); ok || e != nil {
			return common.Or(e, errors.Wrapf(AccessError, "Log already exists [%v]", logId))
		}
		return nil
	})
	if err != nil {
		return
	}

	snapshot, err := createBoltSnapshot(db, NewEventChannel([]Event{}), 0, config)
	if err != nil {
		return
	}
	defer common.RunIf(func() { snapshot.Delete() })(err)

	seg, err := createBoltSegment(db, snapshot.Id(), -1, -1)
	if err != nil {
		return
	}
	defer common.RunIf(func() { seg.Delete() })(err)

	log = logDat{logId}
	err = db.Update(func(tx *bolt.Tx) error {
		err = tx.Bucket(logBucket).Put(stash.UUID(logId), log.Bytes())
		if err != nil {
			return err
		}

		err = setActiveSegmentId(tx, logId, seg.Id())
		if err != nil {
			return err
		}

		_, err = setCommit(tx, logId, -1)
		return err
	})
	return
}

func deleteLog(db stash.Stash, id uuid.UUID) error {
	return nil
}

func (d logDat) GetCommit(db stash.Stash) (pos int, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		pos, err = d.getCommit(tx)
		return err
	})
	return
}

func (d logDat) SetCommit(db stash.Stash, pos int) (new int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		new, err = d.setCommit(tx, pos)
		return err
	})
	return
}

func (d logDat) Active(db stash.Stash) (id uuid.UUID, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		id, err = d.getActiveSegmentId(tx)
		return err
	})
	return
}

func (d logDat) SwapActive(db stash.Stash, cur *boltSegment, new *boltSegment) (ok bool, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		ok, err = d.swapActive(tx, cur, new)
		return err
	})
	return
}

func (d logDat) Delete(db stash.Stash) (bool, error) {
	return false, nil
}

func (d logDat) setActiveSegmentId(tx *bolt.Tx, id uuid.UUID) error {
	return setActiveSegmentId(tx, d.id, id)
}

func (d logDat) getActiveSegmentId(tx *bolt.Tx) (uuid.UUID, error) {
	s, ok, e := getActiveSegmentId(tx, d.id)
	if e != nil || !ok {
		return uuid.UUID{}, common.Or(e, errors.Wrapf(AccessError, "Log [%v] dropped.", d.id))
	}

	return s, nil
}

func (d logDat) setCommit(tx *bolt.Tx, pos int) (int, error) {
	return setCommit(tx, d.id, pos)
}

func (d logDat) getCommit(tx *bolt.Tx) (int, error) {
	i, ok, e := getCommit(tx, d.id)
	if e != nil || !ok {
		return -1, common.Or(e, errors.Wrapf(AccessError, "Log [%v] dropped.", d.id))
	}

	return i, e
}

func (d logDat) swapActive(tx *bolt.Tx, cur *boltSegment, new *boltSegment) (bool, error) {
	id, err := d.getActiveSegmentId(tx)
	if err != nil {
		return false, err
	}

	if id != cur.Id() {
		return false, nil
	}

	if new.PrevIndex() < cur.PrevIndex() {
		return false, errors.Wrapf(SwapError, "New segment [%v, %v] is older than current. [%v]", new.Id(), new.PrevIndex(), cur.PrevTerm())
	}

	// copy over items
	curHead, err := cur.raw.head(tx)
	if err != nil {
		return false, errors.Wrap(err, "Error retrieving current head position")
	}

	newHead, err := new.raw.head(tx)
	if err != nil {
		return false, errors.Wrap(err, "Error retrieving new head position")
	}

	if newHead >= curHead {
		return true, d.setActiveSegmentId(tx, new.Id())
	}

	copies, err := cur.raw.scan(tx, newHead+1, curHead)
	if err != nil {
		return false, errors.Wrapf(err, "Error scanning items [%v,%v]", newHead+1, curHead)
	}

	_, err = new.raw.insert(tx, copies)
	if err != nil {
		return false, errors.Wrapf(err, "Error copying items [%v]", len(copies))
	}

	err = d.setActiveSegmentId(tx, new.Id())
	return err == nil, err
}

//
// func (d durableSnapshot) Delete(db stash.Stash) error {
// return deleteDurableSnapshot(db, d.id)
// }
//
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

type boltSegment struct {
	db  *bolt.DB
	raw segmentDat
}

func createEmptyBoltSegment(db *bolt.DB, config []byte) (*boltSegment, error) {
	snapshot, err := createEmptyBoltSnapshot(db, config)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating empty snapshot")
	}

	return createBoltSegment(db, snapshot.Id(), -1, -1)
}

func createBoltSegment(db *bolt.DB, snapshotId uuid.UUID, prevIndex int, prevTerm int) (*boltSegment, error) {
	raw, err := createSegment(db, snapshotId, prevIndex, prevTerm)
	if err != nil {
		return nil, err
	}

	return &boltSegment{db, raw}, nil
}

func openBoltSegment(db *bolt.DB, id uuid.UUID) (*boltSegment, error) {
	raw, ok, err := openSegment(db, id)
	if err != nil || !ok {
		return nil, err
	}

	return &boltSegment{db, raw}, nil
}

func (b *boltSegment) Id() uuid.UUID {
	return b.raw.id
}

func (b *boltSegment) PrevIndex() int {
	return b.raw.prevIndex
}

func (b *boltSegment) PrevTerm() int {
	return b.raw.prevTerm
}

func (b *boltSegment) Snapshot() (StoredSnapshot, error) {
	return openBoltSnapshot(b.db, b.raw.prevSnapshot)
}

func (b *boltSegment) Head() (int, error) {
	return b.raw.Head(b.db)
}

func (b *boltSegment) Get(index int) (LogItem, bool, error) {
	return b.raw.Get(b.db, index)
}

func (b *boltSegment) Scan(beg int, end int) ([]LogItem, error) {
	return b.raw.Scan(b.db, beg, end)
}

func (b *boltSegment) Append(e Event, t int, s uuid.UUID, seq int, kind int) (int, error) {
	return b.raw.Append(b.db, e, t, s, seq, kind)
}

func (b *boltSegment) Insert(batch []LogItem) (int, error) {
	return b.raw.Insert(b.db, batch)
}

func (b *boltSegment) Compact(until int, ch <-chan Event, size int, config []byte) (StoredSegment, error) {
	head, err := b.Head()
	if err != nil {
		return nil, err
	}

	if until < b.raw.prevIndex || until > head {
		return nil, errors.Wrapf(OutOfBoundsError, "Cannot compact. Until mark [%v] out of bounds", until)
	}

	snapshot, err := createBoltSnapshot(b.db, ch, size, config)
	defer common.RunIf(func() { snapshot.Delete() })(err)

	if head-b.raw.prevIndex <= 0 {
		return createBoltSegment(b.db, snapshot.Id(), b.raw.prevIndex, b.raw.prevTerm)
	}

	prevItem, found, err := b.Get(until)
	if err != nil || !found {
		return nil, common.Or(err, errors.Wrapf(AccessError, "Segment [%v] deleted.  Cannot compact", b.raw.id))
	}

	seg, err := createBoltSegment(b.db, snapshot.Id(), prevItem.Index, prevItem.Term)
	if err != nil {
		return nil, err
	}

	// perform copy
	copied, err := b.Scan(until+1, head)
	if err != nil {
		return nil, err
	}

	_, err = seg.Insert(copied)
	return seg, err
}

func (b *boltSegment) Delete() error {
	return b.raw.Delete(b.db)
}

type segmentDat struct {
	id uuid.UUID

	prevSnapshot uuid.UUID
	prevIndex    int
	prevTerm     int
}

func createSegment(db *bolt.DB, snapshotId uuid.UUID, prevIndex int, prevTerm int) (seg segmentDat, err error) {
	seg = segmentDat{uuid.NewV1(), snapshotId, prevIndex, prevTerm}
	err = db.Update(func(tx *bolt.Tx) error {
		if e := tx.Bucket(segHeadsBucket).Put(seg.Key(), stash.Int(prevIndex)); e != nil {
			return e
		}

		if e := tx.Bucket(segBucket).Put(seg.Key(), seg.Bytes()); e != nil {
			return e
		}

		return nil
	})
	return
}

func deleteSegment(db stash.Stash, id uuid.UUID) error {

	err := deleteSegmentItems(db, id)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) (e error) {
		e = tx.Bucket(segHeadsBucket).Delete(stash.UUID(id))
		e = common.Or(e, tx.Bucket(segBucket).Delete(stash.UUID(id)))
		return
	})
}

func openSegment(db stash.Stash, id uuid.UUID) (s segmentDat, o bool, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		s, o, e = parseSegment(tx.Bucket(segBucket).Get(stash.UUID(id)))
		return e
	})
	return
}

func parseSegment(bytes []byte) (segmentDat, bool, error) {
	if bytes == nil {
		return segmentDat{}, false, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return segmentDat{}, false, errors.Wrapf(err, "Error parsing scribe message")
	}

	raw, err := readSegment(msg)
	if err != nil {
		return segmentDat{}, false, errors.Wrapf(err, "Error reading durable segment")
	}

	return raw.(segmentDat), true, nil
}

func readSegment(r scribe.Reader) (interface{}, error) {
	seg := segmentDat{}
	err := r.ReadUUID("id", &seg.id)
	err = common.Or(err, r.ReadUUID("prevSnapshot", &seg.prevSnapshot))
	err = common.Or(err, r.ReadInt("prevIndex", &seg.prevIndex))
	err = common.Or(err, r.ReadInt("prevTerm", &seg.prevTerm))
	return seg, err
}

func (d segmentDat) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteUUID("prevSnapshot", d.prevSnapshot)
	w.WriteInt("prevIndex", d.prevIndex)
	w.WriteInt("prevTerm", d.prevTerm)
}

func (d segmentDat) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d segmentDat) Key() stash.Key {
	return stash.UUID(d.id)
}

// generates a new segment but does NOT edit the existing in any way

// this is NOT commutative, but rather last move wins.

func (d segmentDat) Head(db stash.Stash) (h int, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		h, e = d.head(tx)
		return e
	})
	return
}

func (d segmentDat) Get(db stash.Stash, index int) (i LogItem, o bool, e error) {
	e = db.View(func(tx *bolt.Tx) error {
		i, o, e = d.get(tx, index)
		return e
	})
	return
}

// Scan inclusive of start and end
func (d segmentDat) Scan(db stash.Stash, start int, end int) (batch []LogItem, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		batch, err = d.scan(tx, start, end)
		return err
	})
	return
}

func (d segmentDat) Append(db stash.Stash, e Event, term int, source uuid.UUID, seq int, kind int) (head int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		head, err = d.append(tx, e, term, source, seq, kind)
		return err
	})
	return
}

func (d segmentDat) Insert(db stash.Stash, batch []LogItem) (head int, err error) {
	err = db.Update(func(tx *bolt.Tx) error {
		head, err = d.insert(tx, batch)
		return err
	})
	return
}

func (d segmentDat) Delete(db stash.Stash) error {
	return deleteSegment(db, d.id)
}

func (d segmentDat) CopyAndCompact(db stash.Stash, until int, events <-chan Event, num int, config []byte) (seg segmentDat, err error) {
	return segmentDat{}, nil
}

func (d segmentDat) head(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(segHeadsBucket).Get(d.Key())
	if raw == nil {
		return -1, AccessError
	}
	return stash.ParseInt(raw)
}

func (d segmentDat) get(tx *bolt.Tx, index int) (LogItem, bool, error) {
	val := tx.Bucket(segItemsBucket).Get(d.Key().ChildInt(index))
	if val == nil {
		return LogItem{}, false, nil
	}

	item, err := ParseItem(val)
	if err != nil {
		return LogItem{}, false, err
	}

	return item, true, nil
}

func (d segmentDat) scan(tx *bolt.Tx, beg int, end int) (batch []LogItem, err error) {
	if beg <= d.prevIndex {
		return nil, errors.Wrapf(OutOfBoundsError, "Start of range [%v] less than minimal recoverable index [%v]", beg, d.prevIndex)
	}

	// prove log isn't empty
	head, err := d.head(tx)
	if err != nil {
		return nil, err
	}

	if head-d.prevIndex == 0 {
		return []LogItem{}, nil
	}

	// prove batch isn't empty
	end = common.Min(head, end)
	if end-beg < 0 {
		return []LogItem{}, nil
	}

	// init the batch
	cur, cursor, batch := beg, tx.Bucket(segItemsBucket).Cursor(), make([]LogItem, 0, end-beg+1)
	for k, v := cursor.Seek(d.Key().ChildInt(cur).Raw()); v != nil && cur <= end; k, v = cursor.Next() {
		if !d.Key().ChildInt(cur).Equals(k) {
			return nil, errors.Wrapf(AccessError, "Segment deleted [%v]", d.id) // this shouldn't be possible.
		}

		i, e := ParseItem(v)
		if e != nil {
			return nil, e
		}

		batch = append(batch, i)
		cur++
	}

	// should NOT be possible to get a batch smaller than end-beg+1)
	return batch, nil
}

func (d segmentDat) setHead(tx *bolt.Tx, pos int) error {
	cur, err := d.head(tx)
	if err != nil {
		return err
	}

	if cur >= pos {
		return nil
	}

	return tx.Bucket(segHeadsBucket).Put(d.Key(), stash.IntBytes(pos))
}

func (d segmentDat) append(tx *bolt.Tx, e Event, term int, source uuid.UUID, seq int, kind int) (int, error) {
	head, err := d.head(tx)
	if err != nil {
		return 0, err
	}

	index := head + 1
	item := NewLogItem(index, e, term, source, seq, kind)
	bucket := tx.Bucket(segItemsBucket)
	if err := bucket.Put(d.Key().ChildInt(index).Raw(), item.Bytes()); err != nil {
		return index, err
	}

	if err := d.setHead(tx, index); err != nil {
		return 0, err
	}

	return index, nil
}

func (d segmentDat) insert(tx *bolt.Tx, batch []LogItem) (int, error) {

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

func deleteSegmentItems(db stash.Stash, id uuid.UUID) (err error) {
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
			return
		}
	}
	return
}

type boltSnapshot struct {
	db  *bolt.DB
	raw snapshotDat
}

func createEmptyBoltSnapshot(db *bolt.DB, config []byte) (*boltSnapshot, error) {
	return createBoltSnapshot(db, NewEventChannel([]Event{}), 0, config)
}

func createBoltSnapshot(db *bolt.DB, ch <-chan Event, size int, config []byte) (*boltSnapshot, error) {
	raw := snapshotDat{uuid.NewV1(), size, config}
	err := storeSnapshotEvents(db, raw.id, size, ch)
	if err != nil {
		return nil, err
	}
	defer common.RunIf(func() { raw.Delete(db) })(err)

	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotsBucket).Put(raw.Key(), raw.Bytes())
	})
	if err != nil {
		return nil, err
	}

	return &boltSnapshot{db, raw}, nil
}

func openBoltSnapshot(db *bolt.DB, id uuid.UUID) (*boltSnapshot, error) {
	var raw snapshotDat
	var err error
	var ok bool

	err = db.View(func(tx *bolt.Tx) error {
		raw, ok, err = parseSnapshot(tx.Bucket(snapshotsBucket).Get(stash.UUID(id)))
		return err
	})
	if !ok || err != nil {
		return nil, err
	}

	return &boltSnapshot{db, raw}, nil
}

func (b *boltSnapshot) Id() uuid.UUID {
	return b.raw.id
}

func (b *boltSnapshot) Size() int {
	return b.raw.size
}

func (b *boltSnapshot) Config() []byte {
	return b.raw.config
}

func (b *boltSnapshot) Scan(beg int, end int) ([]Event, error) {
	return b.raw.Scan(b.db, beg, end)
}

func (b *boltSnapshot) Delete() error {
	return b.raw.Delete(b.db)
}

// pure data impl.
type snapshotDat struct {
	id     uuid.UUID
	size   int
	config []byte
}

func readSnapshot(r scribe.Reader) (interface{}, error) {
	var ret snapshotDat
	var err error
	err = r.ReadUUID("id", &ret.id)
	err = r.ReadInt("size", &ret.size)
	err = r.ReadBytes("config", &ret.config)
	return ret, err
}

func parseSnapshot(bytes []byte) (snapshotDat, bool, error) {
	if bytes == nil {
		return snapshotDat{}, false, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return snapshotDat{}, false, err
	}

	raw, err := readSnapshot(msg)
	if err != nil {
		return snapshotDat{}, false, err
	}

	return raw.(snapshotDat), true, nil
}

func (d snapshotDat) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteInt("size", d.size)
	w.WriteBytes("config", d.config)
}

func (d snapshotDat) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d snapshotDat) String() string {
	return fmt.Sprintf("Snapshot(%v)", d.id.String()[0:8])
}

func (d snapshotDat) Config() []byte {
	return d.config
}

func (d snapshotDat) Key() stash.Key {
	return stash.UUID(d.id)
}

func (d snapshotDat) Delete(db *bolt.DB) error {
	// delete the ref first.
	err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotsBucket).Delete(stash.UUID(d.id))
	})

	return common.Or(err, deleteSnapshotEvents(db, d.id))
}

func (d snapshotDat) Scan(db *bolt.DB, start int, end int) (batch []Event, err error) {
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
				return errors.Wrapf(AccessError, "Snapshot deleted [%v]", d.id) // already deleted
			}

			batch = append(batch, Event(v))
			cur++
		}
		return nil
	})

	if len(batch) != end-start+1 {
		return nil, errors.Wrapf(AccessError, "Snapshot deleted [%v]", d.id) // already deleted
	}
	return
}

func storeSnapshotEvents(db *bolt.DB, id uuid.UUID, num int, ch <-chan Event) (err error) {
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

		err := storeSnapshotSegment(db, id, chunkStart, chunk)
		if err != nil {
			return err
		}
	}
	return nil
}

func storeSnapshotSegment(db *bolt.DB, id uuid.UUID, offset int, events []Event) (err error) {
	prefix := stash.UUID(id)

	num := len(events)
	for i := 0; i < num; {
		chunkStart := offset + i
		chunk := make([]Event, 0, 1024)

		for cur := 0; cur < 1024 && i < num; cur, i = cur+1, i+1 {
			chunk = append(chunk, events[i])
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

func deleteSnapshotEvents(db *bolt.DB, id uuid.UUID) (err error) {
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
