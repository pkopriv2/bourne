package convoy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/enc"
	uuid "github.com/satori/go.uuid"
)

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func stob(v string) []byte {
	return []byte(v)
}

func logBucket(id uuid.UUID) []byte {
	return []byte(fmt.Sprintf("CONVOY:DB:LOG:%v", id))
}

func idxBucket(id uuid.UUID) []byte {
	return []byte(fmt.Sprintf("CONVOY:DB:IDX:%v", id))
}


// This is just a test implementation.  Will need to add durability to make this resilient to host failures.
type database struct {
	id uuid.UUID
	db bolt.DB

	lock sync.RWMutex
}

func NewDatabase(id uuid.UUID) Database {
	return nil
	// return &database{
	// id:        id,
	// db:
	// log:       make([]Change, 1024),
	// idx:       make(map[string]string),
	// listeners: make([]chan Change, 0, 10),
	// closed:    make(chan struct{})}
}

func (d *database) Close() error {
	return d.db.Close()
}

func (d *database) Get(key string) (string, error) {
	var val string
	var err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(logBucket(d.id))
		if bucket == nil {
			panic("Bolt db not initialized")
		}

		raw := bucket.Get([]byte(key))
		if raw == nil {
			return nil
		}

		val = string(raw)
		return nil
	})

	return val, err
}

func (d *database) Put(key string, val string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(idxBucket(d.id))
		if idx == nil {
			panic("Bolt db not initialized")
		}

		log := tx.Bucket(logBucket(d.id))
		if log == nil {
			panic("Bolt db not initialized")
		}

		version, _ := log.NextSequence()

		msg := enc.Write(newChange(int(version), false, key, val))
		buf := new(bytes.Buffer)
		msg.Stream(gob.NewEncoder(buf))

		if err := log.Put(itob(version), buf.Bytes()); err != nil {
			return err
		}

		if err := idx.Put([]byte(key), []byte(val)); err != nil {
			return err
		}

		return nil
	})
}

func (d *database) Del(key string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(idxBucket(d.id))
		if idx == nil {
			panic("Bolt db not initialized")
		}

		log := tx.Bucket(logBucket(d.id))
		if log == nil {
			panic("Bolt db not initialized")
		}

		// version, _ := log.NextSequence()

		// msg := enc.Write(newChange(int(version), false, true, val))
		// buf := new(bytes.Buffer)
		// msg.Stream(gob.NewEncoder(buf))
//
		// if err := log.Put(itob(version), buf.Bytes()); err != nil {
			// return err
		// }
//
		// if err := idx.Put([]byte(key), []byte(val)); err != nil {
			// return err
		// }

		return nil
	})
}

func (l *database) Id() uuid.UUID {
	return l.id
}
