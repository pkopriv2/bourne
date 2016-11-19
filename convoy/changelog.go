package convoy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

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

// Adds a listener to the change log and returns a buffered channel of changes.
func changeLogListen(cl ChangeLog) <-chan Change {
	ret := make(chan Change, 1024)
	cl.Listen(func(chg Change) {
		ret <- chg
	})
	return ret
}

// The change log implementation.  The change log is
// built on a bolt DB instance, so it is guaranteed
// both durable and thread-safe.
type changeLog struct {
	// The underlying bolt db instance.
	db stash.Stash

	// Change handlers
	fns []func(Change)

	// Lock around handlers
	fnsLock sync.RWMutex
}

// Opens the change log.  This uses the shared store
func openChangeLog(db stash.Stash) ChangeLog {
	return &changeLog{db: db, fns: make([]func(Change), 0, 4)}
}

func (c *changeLog) Close() error {
	return nil // the stash is managed by the context.
}

func (c *changeLog) Listen(fn func(Change)) {
	c.fnsLock.Lock()
	defer c.fnsLock.Unlock()
	c.fns = append(c.fns, fn)
}

func (c *changeLog) Id() (id uuid.UUID, err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		id, err = getOrCreateId(tx)
		return err
	})
	return
}

func (c *changeLog) Listeners() []func(Change) {
	c.fnsLock.RLock()
	defer c.fnsLock.RUnlock()
	ret := make([]func(Change), 0, len(c.fns))
	for _, fn := range c.fns {
		ret = append(ret, fn)
	}
	return ret
}

func (c *changeLog) Append(key string, val string, ver int, del bool) (chg Change, err error) {
	err = c.db.Update(func(tx *bolt.Tx) error {
		chg, err = appendChange(tx, key, val, ver, del)
		return err
	})

	if err != nil {
		return
	}

	c.broadcast(chg)
	return
}

func (c *changeLog) All() (chgs []Change, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		chgs, err = readChanges(tx)
		return err
	})
	return
}


func (c *changeLog) broadcast(chg Change) {
	for _, fn := range c.Listeners() {
		fn(chg)
	}
}

// Helper functions
func changeKeyBytes(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func parseChangeKey(val []byte) uint64 {
	return binary.BigEndian.Uint64(val)
}

func changeValBytes(c Change) []byte {
	buf := new(bytes.Buffer)
	scribe.Encode(gob.NewEncoder(buf), c)
	return buf.Bytes()
}

func parseChangeVal(val []byte) (Change, error) {
	msg, err := scribe.Decode(gob.NewDecoder(bytes.NewBuffer(val)))
	if err != nil {
		var chg Change
		return chg, err
	}

	return ReadChange(msg)
}

func getOrCreateId(tx *bolt.Tx) (uuid.UUID, error) {
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

func appendChange(tx *bolt.Tx, key string, val string, ver int, del bool) (Change, error) {
	var chg Change

	bucket, err := tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		return chg, err
	}

	id, err := bucket.NextSequence()
	if err != nil {
		return chg, err
	}

	chg = Change{int(id), key, val, ver, del}
	return chg, bucket.Put(changeKeyBytes(id), changeValBytes(chg))
}

func readChanges(tx *bolt.Tx) ([]Change, error) {
	bucket := tx.Bucket(logBucket)
	if bucket == nil {
		return nil, nil
	}

	chgs := make([]Change, 0, 1024)
	return chgs, bucket.ForEach(func(_ []byte, val []byte) error {
		chg, err := parseChangeVal(val)
		if err != nil {
			return err
		}

		chgs = append(chgs, chg)
		return nil
	})
}
