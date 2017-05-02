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

// FIXME: Gob encoding is likely NOT deterministic.  Must move to deterministic encoding for signing

// The maximum number of items returned as part of any "list" style operation
const MaxPageSize = 1024

// The default authentication method.
const DefaultAuthMethod = "DEFAULT"

// Register all the gob types.
func init() {
	gob.Register(&rsaPublicKey{})
}

// Bolt info
var (
	keyPubBucket           = []byte("warden.keys.public")
	keyPairBucket          = []byte("warden.keys.backup")
	keyOwnerBucket         = []byte("warden.keys.owner")
	certBucket             = []byte("warden.certs")
	certLatestBucket       = []byte("warden.certs.latest")
	inviteBucket           = []byte("warden.invites")
	subscriberBucket       = []byte("warden.subscribers")
	subscriberAuthBucket   = []byte("warden.subscriber.auth")
	subscriberAuxBucket    = []byte("warden.subscriber.aux")
	subscriberCertBucket   = []byte("warden.subscribers.certs")
	subscriberInviteBucket = []byte("warden.subscribers.invites")
	domainBucket           = []byte("warden.domains")
	domainAuthBucket       = []byte("warden.domains.auth")
	domainCertBucket       = []byte("warden.domains.certs")
	domainInviteBucket     = []byte("warden.domains.invites")
)

func initBoltBuckets(db *bolt.DB) (err error) {
	return db.Update(func(tx *bolt.Tx) error {
		var e error
		_, e = tx.CreateBucketIfNotExists(keyPubBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(certBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(certLatestBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(inviteBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberAuthBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(subscriberAuxBucket)
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

type boltStorage bolt.DB

func newBoltStorage(db *bolt.DB) (*boltStorage, error) {
	if err := initBoltBuckets(db); err != nil {
		return nil, errors.WithStack(err)
	}
	return (*boltStorage)(db), nil
}

func (b *boltStorage) Bolt() *bolt.DB {
	return (*bolt.DB)(b)
}

func (b *boltStorage) SaveSubscriber(sub Subscriber, auth SignedOracleKey) error {
	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreSubscriber(tx, sub); err != nil {
			return err
		}
		return boltStoreSubscriberAuth(tx, sub.Id, DefaultAuthMethod, auth)
	})
}

func (b *boltStorage) LoadSubscriber(id string) (s StoredSubscriber, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		s, o, e = boltLoadSubscriber(tx, id)
		return e
	})
	return
}

// Stores the subscriber's auxillary keys.  (Usually encrypted with the user's oracle)
func (b *boltStorage) SaveSubscriberAuxKey(id, name string, pair SignedKeyPair) error {
	return b.Bolt().Update(func(tx *bolt.Tx) error {
		return nil
	})
}

// Stores the subscriber's auxillary keys.  (Usually encrypted with the user's oracle)
func (b *boltStorage) LoadSubscriberAuxKey(id, name string, pair SignedKeyPair) error {
	return b.Bolt().Update(func(tx *bolt.Tx) error {
		return nil
	})
}

// func (b *boltStorage) SaveSubscriberAuth(id, method string, auth SignedOracleKey) error {
// sub, err := EnsureSubscriber(b, id)
// if err != nil {
// return errors.WithStack(err)
// }
//
// if err := auth.Verify(sub.Identity); err != nil {
// return errors.Wrapf(err, "Cannot add auth [%v] to subscriber [%v]. Invalid signature.", id, method)
// }
//
// return b.Bolt().Update(func(tx *bolt.Tx) error {
// return boltStoreSubscriberAuth(tx, id, method, auth)
// })
// }
//
// func (b *boltStorage) LoadSubscriberAuth(id string, method string) (s StoredAuthenticator, o bool, e error) {
// e = b.Bolt().View(func(tx *bolt.Tx) error {
// s, o, e = boltLoadSubscriberAuth(tx, id, method)
// return e
// })
// return
// }

func (b *boltStorage) LoadDomain(id string) (d StoredDomain, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		d, o, e = boltLoadDomain(tx, id)
		return e
	})
	return
}

func (b *boltStorage) SaveDomain(dom Domain) error {
	var issuer StoredSubscriber
	// issuer, err := EnsureSubscriber(b, dom.cert.Issuer)
	// if err != nil {
	// return errors.WithStack(err)
	// }

	if err := dom.cert.Verify(dom.ident.Pub, issuer.Sign.Pub, issuer.Sign.Pub); err != nil {
		return errors.Wrapf(err, "Unable to create domain [%v].  Invalid certificate.", dom.Id)
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreDomain(tx, dom.ident, dom.oracle); err != nil {
			return err
		}

		if err := boltStoreCert(tx, dom.cert); err != nil {
			return err
		}

		if err := boltStoreDomainAuth(tx, dom.Id, dom.cert.Trustee, dom.oracleKey); err != nil {
			return err
		}

		return nil
	})
}

func (b *boltStorage) SaveCert(dom, subscriber string) (d SignedCertificate, k SignedOracleKey, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		// d, o, e = boltLoadDomainAuth(tx, dom, subscriber)
		// return e
		return nil
	})
	return
}

func (b *boltStorage) LoadCertByDomainAndSubscriber(dom, subscriber string) (d SignedCertificate, k SignedOracleKey, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		// d, o, e = boltLoadDomainAuth(tx, dom, subscriber)
		// return e
		return nil
	})
	return
}

func boltStoreSubscriber(tx *bolt.Tx, sub Subscriber) error {
	key := stash.UUID(sub.Id)
	if err := boltEnsureEmpty(tx.Bucket(subscriberBucket), key); err != nil {
		return errors.Wrapf(err, "Subscriber already exists [%v]", sub.Id)
	}

	raw, err := gobBytes(StoredSubscriber{sub})
	if err != nil {
		return errors.Wrapf(err, "Error encoding subscriber [%v]", sub.Id)
	}

	// put on main index
	if err := tx.Bucket(subscriberBucket).Put(key, raw); err != nil {
		return errors.Wrapf(err, "Error writing subscriber [%v]", sub.Id)
	}

	return nil
}

func boltLoadSubscriber(tx *bolt.Tx, id string) (s StoredSubscriber, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(subscriberBucket).Get(stash.String(id)), &s)
	return
}

func boltStoreSubscriberAuth(tx *bolt.Tx, id uuid.UUID, alias string, k SignedOracleKey) error {
	raw, err := gobBytes(StoredAuthenticator{k})
	if err != nil {
		return errors.Wrapf(err, "Error encoding oracle key [%v]", k)
	}

	if err := tx.Bucket(subscriberAuthBucket).Put(stash.UUID(id).ChildString(alias), raw); err != nil {
		return errors.Wrapf(err, "Error writing subscriber auth [%v]", id)
	}

	return nil
}

func boltLoadSubscriberAuth(tx *bolt.Tx, sub, method string) (k StoredAuthenticator, o bool, e error) {
	o, e = parseGobBytes(
		tx.Bucket(subscriberAuthBucket).Get(stash.String(sub).ChildString(method)), &k)
	return
}

func boltStoreDomain(tx *bolt.Tx, identity KeyPair, oracle SignedOracle) error {
	id := identity.Pub.Id()

	if err := boltEnsureEmpty(tx.Bucket(domainBucket), stash.String(id)); err != nil {
		return errors.Wrapf(err, "Domain already exists [%v]", id)
	}

	raw, err := gobBytes(StoredDomain{identity, oracle})
	if err != nil {
		return errors.Wrapf(err, "Error encoding domain [%v]", id)
	}

	// put on main index
	if err := tx.Bucket(domainBucket).Put(stash.String(id), raw); err != nil {
		return errors.Wrapf(err, "Error writing domain [%v]", id)
	}

	return nil
}

func boltLoadDomain(tx *bolt.Tx, id string) (d StoredDomain, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(domainBucket).Get(stash.String(id)), &d)
	return
}

func boltStoreDomainAuth(tx *bolt.Tx, dom, sub uuid.UUID, o SignedOracleKey) error {
	rawKey, err := gobBytes(o)
	if err != nil {
		return errors.Wrapf(err, "Error encoding oracle key [%v]", o)
	}

	if err := tx.Bucket(domainAuthBucket).Put(stash.UUID(dom).ChildUUID(sub), rawKey); err != nil {
		return errors.Wrapf(err, "Error writing domain auth [%v,%v]", dom, sub)
	}

	return nil
}

func boltLoadDomainAuth(tx *bolt.Tx, dom, sub string) (k StoredAuthenticator, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(domainAuthBucket).Get(stash.String(dom).ChildString(sub)), &k)
	return
}

func boltStoreCert(tx *bolt.Tx, c SignedCertificate) error {
	if err := boltEnsureEmpty(tx.Bucket(certBucket), stash.UUID(c.Id)); err != nil {
		return errors.Wrapf(err, "Cert already exists [%v]", c.Id)
	}

	rawCert, err := gobBytes(c)
	if err != nil {
		return errors.Wrapf(err, "Error encoding cert [%v]", c)
	}

	// store main cert
	if err := tx.Bucket(certBucket).Put(stash.UUID(c.Id), rawCert); err != nil {
		return errors.Wrapf(err, "Error writing cert [%v]", c.Id)
	}

	// update subcriber index
	if err := boltUpdateIndex(
		tx.Bucket(subscriberCertBucket), stash.UUID(c.Trustee), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update domain index
	if err := boltUpdateIndex(
		tx.Bucket(domainCertBucket), stash.UUID(c.Domain), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update latest index
	if err := boltUpdateIndex(
		tx.Bucket(certLatestBucket), stash.UUID(c.Domain).ChildUUID(c.Trustee), c.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltStoreInvite(tx *bolt.Tx, i Invitation) error {
	if err := boltEnsureEmpty(tx.Bucket(inviteBucket), stash.UUID(i.Id)); err != nil {
		return errors.Wrapf(err, "Invite already exists [%v]", i.Id)
	}

	raw, err := gobBytes(i)
	if err != nil {
		return errors.WithStack(err)
	}

	// put on main index
	if err := tx.Bucket(inviteBucket).Put(stash.UUID(i.Id), raw); err != nil {
		return errors.WithStack(err)
	}

	// update subscriber index: sub:/id:
	if err := boltUpdateIndex(
		tx.Bucket(subscriberInviteBucket), stash.UUID(i.Cert.Trustee), i.Id); err != nil {
		return errors.WithStack(err)
	}

	// update domain index: :dom:/id:
	if err := boltUpdateIndex(
		tx.Bucket(domainInviteBucket), stash.UUID(i.Cert.Domain), i.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltLoadCertById(tx *bolt.Tx, id uuid.UUID) (i SignedCertificate, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(certBucket).Get(stash.UUID(id)), &i)
	return
}

func boltLoadInviteById(tx *bolt.Tx, id uuid.UUID) (i Invitation, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(inviteBucket).Get(stash.UUID(id)), &i)
	return
}

func boltLoadCertsByIds(tx *bolt.Tx, ids []uuid.UUID) ([]SignedCertificate, error) {
	certs := make([]SignedCertificate, 0, len(ids))
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

func boltLoadCertIdBySubscriberAndDomain(tx *bolt.Tx, dom, sub string) (i uuid.UUID, o bool, e error) {
	raw := tx.Bucket(certLatestBucket).Get(stash.String(dom).ChildString(sub))
	if raw == nil {
		return
	}

	i, e = stash.ParseUUID(raw)
	return i, e == nil, nil
}

func boltUpdateActiveIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Put(root, stash.UUID(id)))
}

func boltLoadCertBySubscriberAndDomain(tx *bolt.Tx, sub, dom string) (SignedCertificate, bool, error) {
	id, ok, err := boltLoadCertIdBySubscriberAndDomain(tx, sub, dom)
	if err != nil || !ok {
		return SignedCertificate{}, false, err
	}
	return boltLoadCertById(tx, id)
}

func boltLoadCertsByDomain(tx *bolt.Tx, dom string, beg, end int) ([]SignedCertificate, error) {
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

func boltLoadCertsBySubscriber(tx *bolt.Tx, dom string, beg, end int) ([]SignedCertificate, error) {
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

func boltDeleteIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Delete(stash.Key(root).ChildUUID(id)))
}

func boltEnsureEmpty(bucket *bolt.Bucket, key []byte) error {
	if val := bucket.Get(key); val != nil {
		return errors.Wrapf(StorageInvariantError, "Key not empty [%v]", key)
	}
	return nil
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
