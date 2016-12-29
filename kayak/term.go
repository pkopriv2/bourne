package kayak

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

var (
	kayakBucket = []byte("kayak")
)

// A term represents a particular member state in the Raft epochal time model.
type term struct {

	// the current term number (increases monotonically across the cluster)
	num int

	// the current leader (as seen by this member)
	leader *uuid.UUID

	// who was voted for this term (guaranteed not nil when leader != nil)
	votedFor *uuid.UUID
}

func readTerm(r scribe.Reader) (t term, e error) {
	e = r.ReadInt("num", &t.num)
	if e != nil {
		return
	}

	var votedFor uuid.UUID
	if err := r.ReadUUID("votedFor", &votedFor); err != nil {
		if _, ok := err.(*scribe.MissingFieldError); !ok {
			return t, err
		}
	} else {
		t.votedFor = &votedFor
	}

	var leader uuid.UUID
	if err := r.ReadUUID("leader", &leader); err != nil {
		if _, ok := err.(*scribe.MissingFieldError); !ok {
			return t, err
		}
	} else {
		t.leader = &leader
	}

	return
}

func (t term) Write(w scribe.Writer) {
	w.WriteInt("num", t.num)
	if t.leader != nil {
		w.WriteUUID("leader", *t.leader)
	}

	if t.votedFor != nil {
		w.WriteUUID("votedFor", *t.votedFor)
	}
}

func (t term) String() string {
	var leaderStr string
	if t.leader == nil {
		leaderStr = "nil"
	} else {
		leaderStr = t.leader.String()[:8]
	}

	var votedForStr string
	if t.votedFor == nil {
		votedForStr = "nil"
	} else {
		votedForStr = t.votedFor.String()[:8]
	}

	return fmt.Sprintf("(%v,%v,%v)", t.num, leaderStr, votedForStr)
}

type termStash struct {
	stash stash.Stash
}

func openTermStash(s stash.Stash) *termStash {
	return &termStash{s}
}

func (t *termStash) Get(id uuid.UUID) (term term, err error) {
	err = t.stash.View(func(tx *bolt.Tx) error {
		tmp, e := termStashGet(tx, id)
		if e != nil {
			err = e
			return e
		}

		term = tmp
		return nil
	})
	return
}

func (t *termStash) Put(id uuid.UUID, tm term) error {
	return t.stash.Update(func(tx *bolt.Tx) error {
		return termStashPut(tx, id, tm)
	})
}

func termStashKeyBytes(id uuid.UUID) []byte {
	return []byte(fmt.Sprintf("%v.kayak.term", id.String()))
}

func termStashValBytes(t term) []byte {
	buf := new(bytes.Buffer)
	scribe.Encode(gob.NewEncoder(buf), t)
	return buf.Bytes()
}

func termStashParseVal(val []byte) (term, error) {
	msg, err := scribe.Decode(gob.NewDecoder(bytes.NewBuffer(val)))
	if err != nil {
		return term{}, err
	}

	return readTerm(msg)
}

func termStashGet(tx *bolt.Tx, id uuid.UUID) (term, error) {
	bucket := tx.Bucket(kayakBucket)
	if bucket == nil {
		return term{}, nil
	}

	val := bucket.Get(termStashKeyBytes(id))
	if val == nil {
		return term{}, nil
	}

	return termStashParseVal(val)
}

func termStashPut(tx *bolt.Tx, id uuid.UUID, t term) error {
	bucket, err := tx.CreateBucketIfNotExists(kayakBucket)
	if err != nil {
		return err
	}
	return bucket.Put(termStashKeyBytes(id), termStashValBytes(t))
}
