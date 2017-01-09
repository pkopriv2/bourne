package convoy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	logBucket = []byte("convoy/changelog")
	idBucket  = []byte("convoy/changelog/id")
	idKey     = []byte("id")
)

// Converts a stream of changes to events.
func changeStreamToEventStream(m member, ch <-chan change) <-chan event {
	ret := make(chan event)
	go func() {
		for chg := range ch {
			ret <- changeToEvent(m, chg)
		}

		close(ret)
	}()
	return ret
}

func changesToEvents(m member, chgs []change) []event {
	ret := make([]event, 0, len(chgs))
	for _, c := range chgs {
		ret = append(ret, changeToEvent(m, c))
	}
	return ret
}

// Fundamental unit of change of the changelog
type change struct {
	Seq int
	Key string
	Val string
	Ver int
	Del bool
}

func readChange(r scribe.Reader) (c change, err error) {
	err = r.ReadInt("seq", &c.Seq)
	err = common.Or(err, r.ReadString("key", &c.Key))
	err = common.Or(err, r.ReadString("val", &c.Val))
	err = common.Or(err, r.ReadInt("ver", &c.Ver))
	err = common.Or(err, r.ReadBool("del", &c.Del))
	return
}

func (c change) Write(w scribe.Writer) {
	w.WriteInt("seq", c.Seq)
	w.WriteString("key", c.Key)
	w.WriteString("val", c.Val)
	w.WriteInt("ver", c.Ver)
	w.WriteBool("del", c.Del)
}

type changeLogListener struct {
	cl     *changeLog
	ch     chan change
	closed chan struct{}
	closer chan struct{}
}

func newChangeLogListener(cl *changeLog) *changeLogListener {
	return &changeLogListener{cl, make(chan change, 1024), make(chan struct{}), make(chan struct{}, 1)}
}

func (l *changeLogListener) Ch() <-chan change {
	return l.ch
}

func (l *changeLogListener) Close() error {
	select {
	case <-l.cl.closed:
		return ClosedError
	case <-l.closed:
		return ClosedError
	case l.closer <- struct{}{}:
	}

	l.cl.subs.Remove(l)

	close(l.ch)
	l.ch = nil
	return nil
}

// The change log implementation.  The change log is
// built on a bolt DB instance, so it is guaranteed
// both durable and thread-safe.
type changeLog struct {
	stash  stash.Stash
	subs   concurrent.Map
	closed chan struct{}
	closer chan struct{}
}

// Opens the change log.  This uses the shared store
func openChangeLog(db stash.Stash) *changeLog {
	return &changeLog{
		stash:  db,
		subs:   concurrent.NewMap(),
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}
}

func (c *changeLog) Close() error {
	select {
	case <-c.closed:
		return ClosedError
	case c.closer <- struct{}{}:
	}

	for _, l := range c.listeners() {
		l.Close()
	}

	close(c.closed)
	return nil
}

func (c *changeLog) ensureOpen() error {
	select {
	case <-c.closed:
		return ClosedError
	default:
		return nil
	}
}

func (c *changeLog) listeners() (ret []*changeLogListener) {
	all := c.subs.All()
	ret = make([]*changeLogListener, 0, len(all))
	for k, _ := range all {
		ret = append(ret, k.(*changeLogListener))
	}
	return
}

func (c *changeLog) Listen() (*changeLogListener, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}

	ret := newChangeLogListener(c)
	c.subs.Put(ret, struct{}{})
	return ret, nil
}

func (c *changeLog) Seq() (seq int, err error) {
	if err := c.ensureOpen(); err != nil {
		return 0, err
	}

	err = c.stash.View(func(tx *bolt.Tx) error {
		seq = changeLogGetSeq(tx)
		return nil
	})
	return
}

func (c *changeLog) Inc() (seq int, err error) {
	if err := c.ensureOpen(); err != nil {
		return 0, err
	}

	err = c.stash.Update(func(tx *bolt.Tx) error {
		seq, err = changeLogIncSeq(tx)
		return err
	})
	return
}

func (c *changeLog) Id() (id uuid.UUID, err error) {
	if err := c.ensureOpen(); err != nil {
		return uuid.UUID{}, err
	}

	err = c.stash.Update(func(tx *bolt.Tx) error {
		id, err = changeLogGetId(tx)
		return err
	})
	return
}

func (c *changeLog) Append(key string, val string, del bool) (chg change, err error) {
	if err := c.ensureOpen(); err != nil {
		return change{}, err
	}

	err = c.stash.Update(func(tx *bolt.Tx) error {
		chg, err = changeLogAppend(tx, key, val, del)
		return err
	})

	if err != nil {
		return
	}

	for _, l := range c.listeners() {
		// do not allow listener to be closed
		select {
		case <-c.closed:
			return change{}, ClosedError
		case <-l.closed:
			continue
		case l.closer <- struct{}{}:
		}

		select {
		case l.ch <- chg:
		default:
			// drop
		}

		<-l.closer
	}
	return
}

func (c *changeLog) All() (chgs []change, err error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}

	err = c.stash.View(func(tx *bolt.Tx) error {
		chgs, err = changeLogReadAll(tx)
		return err
	})
	return
}

// Helper functions
func changeLogKeyBytes(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func changeLogParseKey(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}

func changeLogValBytes(c change) []byte {
	buf := new(bytes.Buffer)
	scribe.Encode(gob.NewEncoder(buf), c)
	return buf.Bytes()
}

func changeLogParseVal(val []byte) (change, error) {
	msg, err := scribe.Decode(gob.NewDecoder(bytes.NewBuffer(val)))
	if err != nil {
		var chg change
		return chg, err
	}

	return readChange(msg)
}

func changeLogGetSeq(tx *bolt.Tx) int {
	bucket := tx.Bucket(logBucket)
	if bucket == nil {
		return 0
	}

	return int(bucket.Sequence())
}

func changeLogIncSeq(tx *bolt.Tx) (int, error) {
	bucket, err := tx.CreateBucketIfNotExists(idBucket)
	if err != nil {
		return 0, err
	}

	seq, err := bucket.NextSequence()
	if err != nil {
		return 0, err
	}

	return int(seq), err
}

func changeLogGetId(tx *bolt.Tx) (uuid.UUID, error) {
	var id uuid.UUID

	bucket, err := tx.CreateBucketIfNotExists(idBucket)
	if err != nil {
		return id, err
	}

	idBytes := bucket.Get(idKey)
	if idBytes != nil {
		id, err = uuid.FromBytes(idBytes)
		return id, err
	}

	id = uuid.NewV4()
	return id, bucket.Put(idKey, id.Bytes())
}

func changeLogAppend(tx *bolt.Tx, key string, val string, del bool) (change, error) {
	var chg change

	bucket, err := tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		return chg, err
	}

	id, err := bucket.NextSequence()
	if err != nil {
		return chg, err
	}

	chg = change{int(id), key, val, int(id), del}
	return chg, bucket.Put(changeLogKeyBytes(id), changeLogValBytes(chg))
}

func changeLogReadAll(tx *bolt.Tx) ([]change, error) {
	bucket := tx.Bucket(logBucket)
	if bucket == nil {
		return nil, nil
	}

	chgs := make([]change, 0, 1024)
	return chgs, bucket.ForEach(func(_ []byte, val []byte) error {
		chg, err := changeLogParseVal(val)
		if err != nil {
			return err
		}

		chgs = append(chgs, chg)
		return nil
	})
}

// Converts a change to a standard data event.
//
// NOTE: See Storage for notes on reconciliation
func changeToEvent(m member, c change) event {
	return item{m.id, m.version, c.Key, c.Val, c.Ver, c.Del, time.Now()}
}
