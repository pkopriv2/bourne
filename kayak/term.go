package kayak

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	termBucket   = []byte("kayak.term")
	termIdBucket = []byte("kayak.term.id")
)

func initBoltTermBucket(tx *bolt.Tx) (err error) {
	var e error
	_, e = tx.CreateBucketIfNotExists(termBucket)
	err = common.Or(err, e)
	_, e = tx.CreateBucketIfNotExists(termIdBucket)
	err = common.Or(err, e)
	return
}

type termStore struct {
	db *bolt.DB
}

func openTermStorage(db *bolt.DB) (*termStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		return initBoltTermBucket(tx)
	})
	if err != nil {
		return nil, err
	}

	return &termStore{db}, nil
}

func (t *termStore) GetId(addr string) (id uuid.UUID, ok bool, err error) {
	err = t.db.View(func(tx *bolt.Tx) error {
		bytes := tx.Bucket(termIdBucket).Get([]byte(addr))
		if bytes == nil {
			return nil
		} else {
			ok = true
		}

		id, err = uuid.FromBytes(tx.Bucket(termIdBucket).Get([]byte(addr)))
		return err
	})
	return
}

func (t *termStore) SetId(addr string, id uuid.UUID) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(termIdBucket).Put([]byte(addr), id.Bytes())
	})
}


func (t *termStore) Get(id uuid.UUID) (term term, err error) {
	err = t.db.View(func(tx *bolt.Tx) error {
		term, _, err = parseTerm(tx.Bucket(termBucket).Get(stash.UUID(id)))
		return err
	})
	return
}

func (t *termStore) Save(id uuid.UUID, tm term) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(termBucket).Put(stash.UUID(id), tm.Bytes())
	})
}

// A term represents a particular member state in the Raft epochal time model.
type term struct {

	// the current term number (increases monotonically across the cluster)
	Num int

	// the current leader (as seen by this member)
	Leader *uuid.UUID

	// who was voted for this term (guaranteed not nil when leader != nil)
	VotedFor *uuid.UUID
}

func readTerm(r scribe.Reader) (t term, e error) {
	e = r.ReadInt("num", &t.Num)
	if e != nil {
		return
	}

	var votedFor uuid.UUID
	if err := r.ReadUUID("votedFor", &votedFor); err != nil {
		if _, ok := err.(*scribe.MissingFieldError); !ok {
			return t, err
		}
	} else {
		t.VotedFor = &votedFor
	}

	var leader uuid.UUID
	if err := r.ReadUUID("leader", &leader); err != nil {
		if _, ok := err.(*scribe.MissingFieldError); !ok {
			return t, err
		}
	} else {
		t.Leader = &leader
	}

	return
}

func parseTerm(bytes []byte) (t term, o bool, e error) {
	if bytes == nil {
		return
	}

	var msg scribe.Message
	msg, e = scribe.Parse(bytes)
	if e != nil {
		return
	}

	t, e = readTerm(msg)
	return
}

func (t term) Write(w scribe.Writer) {
	w.WriteInt("num", t.Num)
	if t.Leader != nil {
		w.WriteUUID("leader", *t.Leader)
	}

	if t.VotedFor != nil {
		w.WriteUUID("votedFor", *t.VotedFor)
	}
}

func (t term) Bytes() []byte {
	return scribe.Write(t).Bytes()
}

func (t term) String() string {
	var leaderStr string
	if t.Leader == nil {
		leaderStr = "nil"
	} else {
		leaderStr = t.Leader.String()[:8]
	}

	var votedForStr string
	if t.VotedFor == nil {
		votedForStr = "nil"
	} else {
		votedForStr = t.VotedFor.String()[:8]
	}

	return fmt.Sprintf("(num=%v,l=%v,v=%v)", t.Num, leaderStr, votedForStr)
}
