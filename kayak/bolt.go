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
	logItemBucket        = []byte("kayak.log.item")
	logMinBucket         = []byte("kayak.log.min")
	logMaxBucket         = []byte("kayak.log.max")
	logSnapshotBucket    = []byte("kayak.log.snapshot")
	snapshotsBucket      = []byte("kayak.snapshots")
	snapshotEventsBucket = []byte("kayak.snapshots.events")
)

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(logBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(logItemBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(logMinBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(logMaxBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(logSnapshotBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(snapshotsBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(snapshotEventsBucket)
		return common.Or(err, e)
	})
}

// Store impl.
type boltStore struct {
	db *bolt.DB
}

func NewBoltStore(db *bolt.DB) (LogStore, error) {
	if err := initBoltBuckets(db); err != nil {
		return nil, err
	}

	return &boltStore{db}, nil
}

func (s *boltStore) Get(id uuid.UUID) (StoredLog, error) {
	log, err := openBoltLog(s.db, id)
	if err != nil || log == nil {
		return nil, err
	}

	return log, nil
}

func (s *boltStore) New(id uuid.UUID, config []byte) (StoredLog, error) {
	return createBoltLog(s.db, id, config)
}

func (s *boltStore) NewSnapshot(lastIndex int, lastTerm int, ch <-chan Event, size int, config []byte) (StoredSnapshot, error) {
	return createBoltSnapshot(s.db, lastIndex, lastTerm, ch, size, config)
}

// Parent log abstraction
type boltLog struct {
	db *bolt.DB
	id uuid.UUID
}

func createBoltLog(db *bolt.DB, id uuid.UUID, config []byte) (log *boltLog, err error) {
	s, err := createEmptyBoltSnapshot(db, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.Delete()
		}
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		if checkBoltLog(tx, id) {
			return errors.Wrapf(InvariantError, "Log [%v] already exists", id)
		}

		return initBoltLog(tx, id, s.Id())
	})
	if err != nil {
		return nil, err
	}

	return &boltLog{db, id}, nil
}

func openBoltLog(db *bolt.DB, id uuid.UUID) (log *boltLog, err error) {
	err = db.View(func(tx *bolt.Tx) error {
		if checkBoltLog(tx, id) {
			log = &boltLog{db, id}
		}
		return nil
	})
	return
}

func checkBoltLog(tx *bolt.Tx, id uuid.UUID) bool {
	raw := tx.Bucket(logBucket).Get(stash.UUID(id))
	return raw != nil
}

func initBoltLog(tx *bolt.Tx, id uuid.UUID, snapshotId uuid.UUID) error {
	e := tx.Bucket(logBucket).Put(stash.UUID(id), []byte{})
	e = common.Or(e, tx.Bucket(logMinBucket).Put(stash.UUID(id), stash.IntBytes(-1)))
	e = common.Or(e, tx.Bucket(logMaxBucket).Put(stash.UUID(id), stash.IntBytes(-1)))
	e = common.Or(e, tx.Bucket(logSnapshotBucket).Put(stash.UUID(id), stash.UUID(snapshotId)))
	return e
}

func (b *boltLog) Id() uuid.UUID {
	return b.id
}

func (b *boltLog) Store() (LogStore, error) {
	return &boltStore{b.db}, nil
}

func (b *boltLog) Min() (m int, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		m, e = b.minIndex(tx)
		return e
	})
	return
}

func (b *boltLog) Max() (m int, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		m, e = b.maxIndex(tx)
		return e
	})
	return
}

func (b *boltLog) Last() (i int, t int, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, t, e = b.last(tx)
		return e
	})
	return
}

func (b *boltLog) Truncate(from int) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, e := b.truncate(tx, from)
		return e
	})
}

// prune must allow concurrent inserts, appends
func (b *boltLog) Prune(until int) error {
	min, err := b.Min()
	if err != nil {
		return err
	}

	for min != -1 && min <= until {
		min = common.Min(min+256, until)

		err := b.db.Update(func(tx *bolt.Tx) (e error) {
			min, e = b.prune(tx, min)
			return e
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *boltLog) Scan(beg int, end int) (i []Entry, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, e = b.scan(tx, beg, end)
		return e
	})
	return
}

func (b *boltLog) Append(evt Event, t int, k Kind) (i Entry, e error) {
	e = b.db.Update(func(tx *bolt.Tx) error {
		i, e = b.append(tx, evt, t, k)
		return e
	})
	return
}

func (b *boltLog) Get(index int) (i Entry, o bool, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, o, e = b.get(tx, index)
		return e
	})
	return
}

func (b *boltLog) Insert(batch []Entry) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return b.insert(tx, batch)
	})
}

func (b *boltLog) SnapshotId() (i uuid.UUID, e error) {
	e = b.db.View(func(tx *bolt.Tx) error {
		i, e = b.snapshotId(tx)
		return e
	})
	return
}

func (b *boltLog) Snapshot() (s StoredSnapshot, e error) {
	var dat snapshotDat
	e = b.db.View(func(tx *bolt.Tx) error {
		dat, e = b.rawSnapshot(tx)
		if e != nil {
			return e
		}

		s = &boltSnapshot{b.db, dat}
		return nil
	})
	return
}

func (b *boltLog) Install(s StoredSnapshot) error {
	cur, err := b.Snapshot()
	if err != nil {
		return err
	}
	// swap it.
	err = b.db.Update(func(tx *bolt.Tx) error {
		return b.swapSnapshot(tx, cur.(*boltSnapshot).raw, s.(*boltSnapshot).raw)
	})
	if err != nil {
		return err
	}

	// finally, truncate (safe to do concurrently)
	err = b.Prune(s.LastIndex())
	if err != nil {
		return err
	}

	// cur is useless, regardless of whether the delete succeeds
	defer cur.Delete()
	return nil
}

func (b *boltLog) maxIndex(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(logMaxBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return 0, errors.Wrapf(InvariantError, "No max index for log [%v]", b.id)
	}

	return stash.ParseInt(raw)
}

func (b *boltLog) setMaxIndex(tx *bolt.Tx, pos int) error {
	return tx.Bucket(logMaxBucket).Put(stash.UUID(b.id), stash.IntBytes(pos))
}

func (b *boltLog) minIndex(tx *bolt.Tx) (int, error) {
	raw := tx.Bucket(logMinBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return 0, errors.Wrapf(InvariantError, "No min index for log [%v]", b.id)
	}

	return stash.ParseInt(raw)
}

func (b *boltLog) setMinIndex(tx *bolt.Tx, pos int) error {
	return tx.Bucket(logMinBucket).Put(stash.UUID(b.id), stash.IntBytes(pos))
}

func (b *boltLog) snapshotId(tx *bolt.Tx) (uuid.UUID, error) {
	raw := tx.Bucket(logSnapshotBucket).Get(stash.UUID(b.id))
	if raw == nil {
		return uuid.UUID{}, errors.Wrapf(InvariantError, "Snapshot doesn't exist for log [%v]", b.id)
	}

	return uuid.FromBytes(raw)
}

func (b *boltLog) setSnapshotId(tx *bolt.Tx, id uuid.UUID) error {
	return tx.Bucket(logSnapshotBucket).Put(stash.UUID(b.id), stash.UUID(id))
}

func (b *boltLog) rawSnapshot(tx *bolt.Tx) (snapshotDat, error) {
	id, err := b.snapshotId(tx)
	if err != nil {
		return snapshotDat{}, err
	}

	raw, ok, err := openRawBoltSnapshot(tx, id)
	if err != nil || !ok {
		return raw, common.Or(err, errors.Wrapf(InvariantError, "Snapshot doesn't exist [%v]", id))
	}

	return raw, nil
}

func (b *boltLog) swapSnapshot(tx *bolt.Tx, cur snapshotDat, new snapshotDat) error {
	curId, e := b.snapshotId(tx)
	if e != nil {
		return e
	}

	if curId != cur.id {
		return errors.Wrapf(CompactionError, "Cannot swap snapshot [%v] with current [%v].  Current is no longer active.", new, cur)
	}

	if cur.maxIndex > new.maxIndex && cur.maxTerm >= new.maxTerm {
		return errors.Wrapf(CompactionError, "Cannot swap snapshot [%v] with current [%v].  It is older", new, cur)
	}

	return b.setSnapshotId(tx, new.id)
}

func (b *boltLog) get(tx *bolt.Tx, index int) (Entry, bool, error) {
	raw := tx.Bucket(logItemBucket).Get(stash.UUID(b.id).ChildInt(index))
	if raw == nil {
		return Entry{}, false, nil
	}

	item, err := parseEntryBytes(raw)
	if err != nil {
		return Entry{}, false, errors.Wrapf(InvariantError, "Error parsing item [%v]", index)
	}

	return item, true, nil
}

func (b *boltLog) last(tx *bolt.Tx) (int, int, error) {
	max, err := b.maxIndex(tx)
	if err != nil {
		return 0, 0, err
	}

	if max > -1 {
		item, ok, err := b.get(tx, max)
		if err != nil || !ok {
			return 0, 0, common.Or(err, errors.Wrapf(InvariantError, "Item does not exist [%v]", max))
		}

		return item.Index, item.Term, nil
	}

	raw, err := b.rawSnapshot(tx)
	if err != nil {
		return 0, 0, err
	}

	return raw.maxIndex, raw.maxTerm, nil
}

func (b *boltLog) append(tx *bolt.Tx, e Event, term int, k Kind) (Entry, error) {
	max, _, err := b.last(tx)
	if err != nil {
		return Entry{}, err
	}

	item := Entry{max + 1, e, term, k}
	return item, b.insert(tx, []Entry{item})
}

func (b *boltLog) insert(tx *bolt.Tx, batch []Entry) error {
	if len(batch) == 0 {
		return nil
	}

	max, err := b.maxIndex(tx)
	if err != nil {
		return err
	}

	// Empty case
	if max == -1 {
		max, _, err = b.last(tx)
		if err != nil {
			return err
		}

		if err := b.setMinIndex(tx, max+1); err != nil {
			return err
		}
	}

	bucket := tx.Bucket(logItemBucket)
	for _, i := range batch {
		if i.Index != max+1 {
			return errors.Wrapf(OutOfBoundsError, "Illegal index [%v]. Item index is greater than max+1. Head [%v]", i.Index, max)
		}

		if err := bucket.Put(stash.UUID(b.id).ChildInt(i.Index), i.Bytes()); err != nil {
			return errors.Wrapf(err, "Error inserting item [%v]", i)
		}

		max = i.Index
	}

	if err := b.setMaxIndex(tx, max); err != nil {
		return err
	}

	return nil
}

func (b *boltLog) prune(tx *bolt.Tx, until int) (int, error) {
	min, err := b.minIndex(tx)
	if err != nil {
		return 0, err
	}

	max, err := b.maxIndex(tx)
	if err != nil {
		return 0, err
	}

	if min == -1 || max == -1 {
		return min, nil
	}

	if until < min {
		return min, nil
	}

	until = common.Min(until, max)
	batch, err := b.scan(tx, min, until)
	if err != nil {
		return 0, err
	}

	bucket := tx.Bucket(logItemBucket)
	for _, i := range batch {
		if err := bucket.Delete(stash.UUID(b.id).ChildInt(i.Index)); err != nil {
			return 0, errors.Wrapf(err, "Error deleting item [%v]", i.Index)
		}
	}

	newMin := until + 1
	newMax := common.Max(until+1, max)
	if until >= max {
		newMin = -1
		newMax = -1
	}

	if err := b.setMinIndex(tx, newMin); err != nil {
		return 0, errors.Wrapf(err, "Error setting min index [%v]", newMin)
	}

	if err := b.setMaxIndex(tx, newMax); err != nil {
		return 0, errors.Wrapf(err, "Error setting min index [%v]", newMax)
	}

	return newMin, nil
}

func (b *boltLog) truncate(tx *bolt.Tx, from int) (int, error) {
	min, err := b.minIndex(tx)
	if err != nil {
		return 0, err
	}

	max, err := b.maxIndex(tx)
	if err != nil {
		return 0, err
	}

	if min == -1 || max == -1 {
		return 0, errors.Wrapf(OutOfBoundsError, "Cannot truncate empty log")
	}

	if from > max {
		return 0, errors.Wrapf(OutOfBoundsError, "Cannot truncate from [%v].  Max is currently [%v]", from, max)
	}

	if from < min {
		return 0, errors.Wrapf(OutOfBoundsError, "Cannot truncate from [%v].  Min is currently [%v]", from, min)
	}

	batch, err := b.scan(tx, from, max)
	if err != nil {
		return 0, err
	}

	bucket := tx.Bucket(logItemBucket)
	for _, i := range batch {
		if err := bucket.Delete(stash.UUID(b.id).ChildInt(i.Index)); err != nil {
			return 0, errors.Wrapf(err, "Error deleting item [%v]", i.Index)
		}
	}

	newMax := from - 1
	newMin := min
	if from <= min {
		newMin = -1
		newMax = -1
	}

	if err := b.setMinIndex(tx, newMin); err != nil {
		return 0, errors.Wrapf(err, "Error setting min index [%v]", newMin)
	}

	if err := b.setMaxIndex(tx, newMax); err != nil {
		return 0, errors.Wrapf(err, "Error setting max index [%v]", newMax)
	}

	return newMax, nil
}

func (b *boltLog) scan(tx *bolt.Tx, beg int, end int) ([]Entry, error) {
	min, err := b.minIndex(tx)
	if err != nil {
		return nil, err
	}

	max, err := b.maxIndex(tx)
	if err != nil {
		return nil, err
	}

	if beg < min || beg > max {
		return nil, errors.Wrapf(OutOfBoundsError, "Index out of bounds [%v] [%v,%v]", beg, min, max)
	}

	if min == -1 || max == -1 {
		return []Entry{}, nil
	}

	end = common.Min(end, max)

	cur, cursor, batch := beg, tx.Bucket(logItemBucket).Cursor(), make([]Entry, 0, end-beg+1)
	for _, v := cursor.Seek(stash.UUID(b.id).ChildInt(cur).Raw()); v != nil && cur <= end; _, v = cursor.Next() {
		i, e := parseEntryBytes(v)
		if e != nil {
			return nil, errors.Wrapf(InvariantError, "Error parsing item [%v]", cur)
		}

		batch = append(batch, i)
		cur++
	}

	return batch, nil
}

func deleteLog(db *bolt.DB, id uuid.UUID) error {
	return nil
}

func deleteLogItems(db *bolt.DB, id uuid.UUID) (err error) {
	prefix := stash.UUID(id)

	for contd := true; contd; {
		err = db.Update(func(tx *bolt.Tx) error {
			events := tx.Bucket(logItemBucket)
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
	return createBoltSnapshot(db, -1, -1, NewEventChannel([]Event{}), 0, config)
}

// FIXME: Cleanup erroneous snapshot installations
func createBoltSnapshot(db *bolt.DB, lastIndex int, lastTerm int, ch <-chan Event, size int, config []byte) (*boltSnapshot, error) {
	raw := snapshotDat{uuid.NewV1(), lastIndex, lastTerm, size, config}
	err := storeSnapshotEvents(db, raw.id, size, ch)
	if err != nil {
		return nil, err
	}

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
		raw, ok, err = parseSnapshotDat(tx.Bucket(snapshotsBucket).Get(stash.UUID(id)))
		return err
	})
	if !ok || err != nil {
		return nil, err
	}

	return &boltSnapshot{db, raw}, nil
}

func openRawBoltSnapshot(tx *bolt.Tx, id uuid.UUID) (snapshotDat, bool, error) {
	return parseSnapshotDat(tx.Bucket(snapshotsBucket).Get(stash.UUID(id)))
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

func (b *boltSnapshot) LastIndex() int {
	return b.raw.maxIndex
}

func (b *boltSnapshot) LastTerm() int {
	return b.raw.maxTerm
}

func (b *boltSnapshot) Key() stash.Key {
	return stash.UUID(b.raw.id)
}

func (b *boltSnapshot) Delete() error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(snapshotsBucket).Delete(b.Key())
	})

	return common.Or(err, deleteSnapshotEvents(b.db, b.Id()))
}

func (b *boltSnapshot) Scan(start int, end int) (batch []Event, err error) {
	batch = make([]Event, 0, end-start+1)
	err = b.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(snapshotEventsBucket).Cursor()

		var cur int
		end = common.Min(end, b.Size()-1)
		cur = start
		for k, v := cursor.Seek(b.Key().ChildInt(start)); v != nil; k, v = cursor.Next() {
			if cur > end {
				return nil
			}

			if !b.Key().ChildInt(cur).Equals(k) {
				return errors.Wrapf(InvariantError, "Snapshot deleted [%v]", b.Id()) // already deleted
			}

			batch = append(batch, Event(v))
			cur++
		}
		return nil
	})

	if len(batch) != end-start+1 {
		return nil, errors.Wrapf(InvariantError, "Snapshot deleted [%v]", b.Id()) // already deleted
	}
	return
}

// pure data impl.
type snapshotDat struct {
	id       uuid.UUID
	maxIndex int
	maxTerm  int
	size     int
	config   []byte // not-nil
}

func readSnapshotDat(r scribe.Reader) (interface{}, error) {
	var ret snapshotDat
	var err error
	err = r.ReadUUID("id", &ret.id)
	err = r.ReadInt("maxIndex", &ret.maxIndex)
	err = r.ReadInt("maxTerm", &ret.maxTerm)
	err = r.ReadInt("size", &ret.size)
	err = r.ReadBytes("config", &ret.config)
	return ret, err
}

func parseSnapshotDat(bytes []byte) (snapshotDat, bool, error) {
	if bytes == nil {
		return snapshotDat{}, false, nil
	}

	msg, err := scribe.Parse(bytes)
	if err != nil {
		return snapshotDat{}, false, err
	}

	raw, err := readSnapshotDat(msg)
	if err != nil {
		return snapshotDat{}, false, err
	}

	return raw.(snapshotDat), true, nil
}

func (d snapshotDat) Write(w scribe.Writer) {
	w.WriteUUID("id", d.id)
	w.WriteInt("maxIndex", d.maxIndex)
	w.WriteInt("maxTerm", d.maxTerm)
	w.WriteInt("size", d.size)
	w.WriteBytes("config", d.config)
}

func (d snapshotDat) Bytes() []byte {
	return scribe.Write(d).Bytes()
}

func (d snapshotDat) String() string {
	return fmt.Sprintf("Snapshot(%v)", d.id.String()[0:8])
}

func (d snapshotDat) Key() stash.Key {
	return stash.UUID(d.id)
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

		if err := storeSnapshotSegment(db, id, chunkStart, chunk); err != nil {
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
