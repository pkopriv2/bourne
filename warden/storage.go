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
	StorageError          = errors.New("Warden:StorageError")
	StorageInvariantError = errors.Wrap(StorageError, "Warden:StorageError")
)

const (
	MaxPageSize = 1024
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
	certBucket             = []byte("warden.certs")
	certLatestBucket       = []byte("warden.certs.latest")
	inviteBucket           = []byte("warden.invites")
	subscriberBucket       = []byte("warden.subscribers")
	subscriberCertBucket   = []byte("warden.subscribers.certs")
	subscriberInviteBucket = []byte("warden.subscribers.invites")
	domainBucket           = []byte("warden.domains")
	domainAuthBucket       = []byte("warden.domains.auth")
	domainCertBucket       = []byte("warden.domains.certs")
	domainInviteBucket     = []byte("warden.domains.invites")
)

func init() {
	gob.Register(oracle{})
	gob.Register(oracleKey{})
	gob.Register(&rsaPublicKey{})
	gob.Register(KeyPair{})
	gob.Register(Certificate{})
	gob.Register(Invitation{})
}

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(certBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(certLatestBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(inviteBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberCertBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberInviteBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(domainBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(domainAuthBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(domainCertBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(domainInviteBucket)
		return common.Or(err, e)
	})
}

type domainDat struct {
	Desc   string
	Master KeyPair
	Oracle oracle
}

type subscriberDat struct {
	Master KeyPair
	Oracle oracle
}

func boltRegisterCert(tx *bolt.Tx, c Certificate) error {
	raw, err := gobBytes(c)
	if err != nil {
		return errors.WithStack(err)
	}

	// put on main index
	if err := tx.Bucket(certBucket).Put(stash.UUID(c.Id), raw); err != nil {
		return errors.WithStack(err)
	}

	if err := boltUpdateIndex(
		tx.Bucket(subscriberCertBucket), stash.String(c.Trustee), c.Id); err != nil {
		return errors.WithStack(err)
	}

	if err := boltUpdateIndex(
		tx.Bucket(domainCertBucket), stash.String(c.Domain), c.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltRegisterInvite(tx *bolt.Tx, i Invitation) error {
	raw, err := gobBytes(i)
	if err != nil {
		return errors.WithStack(err)
	}

	// put on main index
	if err := tx.Bucket(inviteBucket).Put(stash.UUID(i.Id), raw); err != nil {
		return errors.WithStack(err)
	}

	if err := boltUpdateIndex(
		tx.Bucket(subscriberInviteBucket), stash.String(i.Cert.Trustee), i.Id); err != nil {
		return errors.WithStack(err)
	}

	if err := boltUpdateIndex(
		tx.Bucket(domainInviteBucket), stash.String(i.Cert.Domain), i.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltLoadCertById(tx *bolt.Tx, id uuid.UUID) (i Certificate, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(certBucket).Get(stash.UUID(id)), &i)
	return
}

func boltLoadInviteById(tx *bolt.Tx, id uuid.UUID) (i Invitation, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(inviteBucket).Get(stash.UUID(id)), &i)
	return
}

func boltLoadCertsByIds(tx *bolt.Tx, ids []uuid.UUID) ([]Certificate, error) {
	certs := make([]Certificate, 0, len(ids))
	for _, id := range ids {
		c, ok, err := boltLoadCertById(tx, id)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.Wrapf(StorageInvariantError, "Cert [%v] expected to exist.", id)
		}

		certs = append(certs, c)
	}
	return certs, nil
}

func boltLoadInvitesByIds(tx *bolt.Tx, ids []uuid.UUID) ([]Invitation, error) {
	invites := make([]Invitation, 0, len(ids))
	for _, id := range ids {
		c, ok, err := boltLoadInviteById(tx, id)
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.Wrapf(StorageInvariantError, "Invite [%v] expected to exist.", id)
		}

		invites = append(invites, c)
	}
	return invites, nil
}

func boltLoadCertIdsByDomain(tx *bolt.Tx, dom string, beg, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(domainCertBucket), stash.String(dom), beg, end)
}

func boltLoadCertIdsBySubscriber(tx *bolt.Tx, subscriber string, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(subscriberCertBucket), stash.String(subscriber), beg, end)
}

func loadBoltInviteIdsByDomain(tx *bolt.Tx, dom string, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(domainInviteBucket), stash.String(dom), beg, end)
}

func boltLoadInviteIdsBySubscriber(tx *bolt.Tx, subscriber string, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(subscriberInviteBucket), stash.String(subscriber), beg, end)
}

func boltLoadCertIdBySubscriberAndDomain(tx *bolt.Tx, sub, dom string) (i uuid.UUID, o bool, e error) {
	raw := tx.Bucket(certLatestBucket).Get(stash.String(sub).ChildString(dom))
	if raw == nil {
		return
	}

	i, e = stash.ParseUUID(raw)
	return i, e == nil, nil
}

func boltLoadCertBySubscriberAndDomain(tx *bolt.Tx, sub, dom string) (Certificate, bool, error) {
	id, ok, err := boltLoadCertIdBySubscriberAndDomain(tx, sub, dom)
	if err != nil || !ok {
		return Certificate{}, false, err
	}
	return boltLoadCertById(tx, id)
}

func boltLoadCertsByDomain(tx *bolt.Tx, dom string, beg, end int) ([]Certificate, error) {
	ids, err := boltLoadCertIdsByDomain(tx, dom, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadCertsByIds(tx, ids)
}

func boltLoadInvitesByDomain(tx *bolt.Tx, dom string, beg, end int) ([]Invitation, error) {
	ids, err := boltLoadCertIdsByDomain(tx, dom, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadInvitesByIds(tx, ids)
}

func boltLoadCertsBySubscriber(tx *bolt.Tx, dom string, beg, end int) ([]Certificate, error) {
	ids, err := boltLoadCertIdsBySubscriber(tx, dom, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadCertsByIds(tx, ids)
}

func boltLoadInvitesBySubscriber(tx *bolt.Tx, dom string, beg, end int) ([]Invitation, error) {
	ids, err := boltLoadCertIdsBySubscriber(tx, dom, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadInvitesByIds(tx, ids)
}

func boltUpdateIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Put(stash.Key(root).ChildUUID(id), stash.UUID(id)))
}

func boltScanIndex(bucket *bolt.Bucket, root []byte, beg int, end int) ([]uuid.UUID, error) {
	if end-beg > MaxPageSize || beg > end {
		return nil, errors.Wrapf(StorageError, "Bad scan range [%v,%v]", beg, end)
	}

	ret := make([]uuid.UUID, 0, end-beg)

	iter := bucket.Cursor()
	k, v := boltSeekToPage(iter, root, beg)
	for ; v != nil; k, v = iter.Next() {
		if !stash.Key(root).ParentOf(k) {
			return ret, nil
		}

		id, err := stash.ParseUUID(v)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		ret = append(ret, id)
	}
	return ret, nil
}

func boltSeekToPage(iter *bolt.Cursor, root []byte, offset int) (k []byte, v []byte) {
	k, v = iter.Seek(root)
	for i := 0; i < offset; i++ {
		k, v = iter.Next()
	}
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
