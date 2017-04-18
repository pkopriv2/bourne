package warden

import (
	"bytes"
	"encoding/gob"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// Basic errors
var (
	StorageInvariantError = errors.New("Warden:StorageInvariateError")
)

type DomainStorage interface {
	RegisterDomain(d Domain) error
}

type OracleStorage interface {
	RegisterOracle(id uuid.UUID, o oracle) error
	RegisterOracleKey(id uuid.UUID, o oracleKey) error
	LoadOracle(id uuid.UUID) (oracle, bool, error)
	LoadOracleKey(id uuid.UUID) (oracleKey, bool, error)
}

// Bolt buckets
var (
	oracleBucket    = []byte("warden.oracle")
	oracleKeyBucket = []byte("warden.oracle.key")
	keyBucket       = []byte("warden.keypair")
)

func init() {
	gob.Register(oracle{})
	gob.Register(oracleKey{})
	gob.Register(KeyPair{})
}

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(oracleBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(oracleKeyBucket)
		return common.Or(err, e)
	})
}

func registerKeyPair(tx *bolt.Tx, k KeyPair) error {
	raw, err := gobBytes(k)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(tx.Bucket(oracleBucket).Put(stash.String(k.Pub.Id()), raw))
}

func loadKeyPair(tx *bolt.Tx, id string) (k KeyPair, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(keyBucket).Get(stash.String(id)), &k)
	return
}

type boltOracleStorage struct {
	db *bolt.DB
}

func (b *boltOracleStorage) RegisterOracle(id uuid.UUID, o oracle) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return registerBoltOracle(tx, id, o)
	})
}

func (b *boltOracleStorage) RegisterOracleKey(id uuid.UUID, o oracleKey) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return registerBoltOracleKey(tx, id, o)
	})
}

func (b *boltOracleStorage) LoadOracle(id uuid.UUID) (o oracle, f bool, e error) {
	e = b.db.Update(func(tx *bolt.Tx) error {
		o, f, e = loadBoltOracle(tx, id)
		return e
	})
	return
}

func (b *boltOracleStorage) LoadOracleKey(id uuid.UUID) (o oracleKey, f bool, e error) {
	e = b.db.Update(func(tx *bolt.Tx) error {
		o, f, e = loadBoltOracleKey(tx, id)
		return e
	})
	return
}

func registerBoltOracle(tx *bolt.Tx, id uuid.UUID, o oracle) error {
	raw, err := gobBytes(o)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(tx.Bucket(oracleBucket).Put(stash.UUID(id), raw))
}

func registerBoltOracleKey(tx *bolt.Tx, id uuid.UUID, o oracleKey) error {
	raw, err := gobBytes(o)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(tx.Bucket(oracleKeyBucket).Put(stash.UUID(id), raw))
}

func loadBoltOracle(tx *bolt.Tx, id uuid.UUID) (o oracle, f bool, e error) {
	f, e = parseGobBytes(tx.Bucket(oracleBucket).Get(stash.UUID(id)), &o)
	return
}

func loadBoltOracleKey(tx *bolt.Tx, id uuid.UUID) (o oracleKey, f bool, e error) {
	f, e = parseGobBytes(tx.Bucket(oracleKeyBucket).Get(stash.UUID(id)), &o)
	return
}

func gobBytes(v interface{}) (ret []byte, err error) {
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(v)
	ret = buf.Bytes()
	return
}

func parseGobBytes(raw []byte, v interface{}) (bool, error) {
	if raw == nil {
		return false, nil
	}
	return true, gob.NewDecoder(bytes.NewBuffer(raw)).Decode(v)
}
