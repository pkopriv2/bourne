package convoy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	logBucket = []byte("convoy/changelog")
	idBucket  = []byte("convoy/changelog/id")
	idKey     = []byte("id")
)

// Adds a listener to the change log and returns a buffered channel of changes
// and a control channel used to close
// The change channel is closed when either log is closed or when the control channel
// is closed.
func changeLogListen(cl ChangeLog) (<-chan Change, chan<- struct{}) {
	ret, done := make(chan Change, 1024), make(chan struct{})

	// // TODO: this is a slight memory leak on the listener....
	// cl.Listen(func(chg Change, ok bool) {
	// })
	return ret, done
}

// Converts a stream of changes to events.
func changeStreamToEventStream(m member, ch <-chan Change) <-chan event {
	ret := make(chan event)
	go func() {
		for chg := range ch {
			ret <- changeToEvent(m, chg)
		}

		close(ret)
	}()
	return ret
}

func changesToEvents(m member, chgs []Change) []event {
	ret := make([]event, 0, len(chgs))
	for _, c := range chgs {
		ret = append(ret, changeToEvent(m, c))
	}
	return ret
}

// A change log is nothing but an ordered list of changes.
type ChangeLog interface {
	io.Closer

	// Every change log must be globally unique.  The standard
	// database
	Id() (uuid.UUID, error)

	// Returns the current sequence of the log
	Seq() (int, error)

	// Increments and returns the sequence of the log.  This is
	// guaranteed to be transactionally safe.
	Inc() (int, error)

	// Appends a change to the log and notifies any listeners.
	Append(key string, val string, del bool) (Change, error)

	// Returns all the changes in the lifetime of the change log.
	All() ([]Change, error)

	// Returns the next change in the changelog, blocking until a change
	// is available.  When connecting the changelog to the external system
	// this should be triggered before backfilling any data
	// Subscribe() Stream
}

// Stream
type Stream interface {
	io.Closer

	Next() (Change, error)
}

// Fundamental unit of change within the published database
type Change struct {
	Seq int
	Key string
	Val string
	Ver int
	Del bool
}

func ReadChange(r scribe.Reader) (Change, error) {
	c := &Change{}
	if err := r.Read("seq", &c.Seq); err != nil {
		return *c, err
	}
	if err := r.Read("key", &c.Key); err != nil {
		return *c, err
	}
	if err := r.Read("val", &c.Val); err != nil {
		return *c, err
	}
	if err := r.Read("ver", &c.Ver); err != nil {
		return *c, err
	}
	if err := r.Read("del", &c.Del); err != nil {
		return *c, err
	}
	return *c, nil
}

func (c Change) Write(w scribe.Writer) {
	w.Write("seq", c.Seq)
	w.Write("key", c.Key)
	w.Write("val", c.Val)
	w.Write("ver", c.Ver)
	w.Write("del", c.Del)
}

// The change log implementation.  The change log is
// built on a bolt DB instance, so it is guaranteed
// both durable and thread-safe.
type changeLog struct {
	stash    stash.Stash
	handlers []func(Change, bool)
	lock     sync.RWMutex
}

// Opens the change log.  This uses the shared store
func openChangeLog(db stash.Stash) *changeLog {
	return &changeLog{stash: db, handlers: make([]func(Change, bool), 0, 4)}
}

func (c *changeLog) Close() error {
	var zero Change
	c.broadcast(zero, false)
	return nil
}

func (c *changeLog) Listen(fn func(Change, bool)) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.handlers = append(c.handlers, fn)
}

func (c *changeLog) Seq() (seq int, err error) {
	err = c.stash.View(func(tx *bolt.Tx) error {
		seq = changeLogGetSeq(tx)
		return err
	})
	return
}

func (c *changeLog) Inc() (seq int, err error) {
	err = c.stash.Update(func(tx *bolt.Tx) error {
		seq, err = changeLogIncSeq(tx)
		return err
	})
	return
}

func (c *changeLog) Id() (id uuid.UUID, err error) {
	err = c.stash.Update(func(tx *bolt.Tx) error {
		id, err = changeLogGetId(tx)
		return err
	})
	return
}

func (c *changeLog) Listeners() []func(Change, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	ret := make([]func(Change, bool), 0, len(c.handlers))
	for _, fn := range c.handlers {
		ret = append(ret, fn)
	}
	return ret
}

func (c *changeLog) Append(key string, val string, del bool) (chg Change, err error) {
	err = c.stash.Update(func(tx *bolt.Tx) error {
		chg, err = changeLogAppend(tx, key, val, del)
		return err
	})

	if err != nil {
		return
	}

	c.broadcast(chg, true)
	return
}

func (c *changeLog) All() (chgs []Change, err error) {
	err = c.stash.View(func(tx *bolt.Tx) error {
		chgs, err = changeLogReadAll(tx)
		return err
	})
	return
}

func (c *changeLog) broadcast(chg Change, ok bool) {
	for _, fn := range c.Listeners() {
		fn(chg, ok)
	}
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

func changeLogValBytes(c Change) []byte {
	buf := new(bytes.Buffer)
	scribe.Encode(gob.NewEncoder(buf), c)
	return buf.Bytes()
}

func changeLogParseVal(val []byte) (Change, error) {
	msg, err := scribe.Decode(gob.NewDecoder(bytes.NewBuffer(val)))
	if err != nil {
		var chg Change
		return chg, err
	}

	return ReadChange(msg)
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

func changeLogAppend(tx *bolt.Tx, key string, val string, del bool) (Change, error) {
	var chg Change

	bucket, err := tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		return chg, err
	}

	id, err := bucket.NextSequence()
	if err != nil {
		return chg, err
	}

	chg = Change{int(id), key, val, int(id), del}
	return chg, bucket.Put(changeLogKeyBytes(id), changeLogValBytes(chg))
}

func changeLogReadAll(tx *bolt.Tx) ([]Change, error) {
	bucket := tx.Bucket(logBucket)
	if bucket == nil {
		return nil, nil
	}

	chgs := make([]Change, 0, 1024)
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
func changeToEvent(m member, c Change) event {
	return item{m.Id, m.Version, c.Key, c.Val, c.Ver, c.Del, time.Now()}
}
