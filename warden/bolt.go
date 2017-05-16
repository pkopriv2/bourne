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
	gob.Register(&shamirShard{})
	gob.Register(SignatureShard{})
}

// Bolt info
var (
	keyPubBucket       = []byte("warden.keys.public")
	keyPairBucket      = []byte("warden.keys.pair")
	keyOwnerBucket     = []byte("warden.keys.owner")
	certBucket         = []byte("warden.certs")
	certLatestBucket   = []byte("warden.certs.latest")
	inviteBucket       = []byte("warden.invites")
	memberBucket       = []byte("warden.members")
	memberAuthBucket   = []byte("warden.member.auth")
	memberAuxBucket    = []byte("warden.member.aux")
	memberCertBucket   = []byte("warden.members.certs")
	memberInviteBucket = []byte("warden.members.invites")
	trustBucket        = []byte("warden.trusts")
	trustAuthBucket    = []byte("warden.trusts.auth")
	trustCertBucket    = []byte("warden.trusts.certs")
	trustInviteBucket  = []byte("warden.trusts.invites")
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
		_, e = tx.CreateBucketIfNotExists(memberBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberAuthBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberAuxBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberCertBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberInviteBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustAuthBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustCertBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustInviteBucket)
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

func (b *boltStorage) SaveMember(sub Membership, auth AccessShard) (mem Member, code AccessCode, err error) {
	mem = Member{sub, uuid.NewV1()}
	code = AccessCode{auth, mem.Id}

	err = b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreMember(tx, mem); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(boltStoreAccessCode(tx, code))
	})
	return
}

func (b *boltStorage) LoadMember(id uuid.UUID) (m Member, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		m, o, e = boltLoadMember(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadAccessCode(lookup []byte) (s AccessCode, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		s, o, e = boltLoadAccessCode(tx, lookup)
		return e
	})
	return
}

// func (b *boltStorage) SaveMemberAuth(id uuid.UUID, auth AccessShard) error {
// sub, err := EnsureMember(b, id)
// if err != nil {
// return errors.WithStack(err)
// }
//
// if err := auth.Verify(sub.Identity); err != nil {
// return errors.Wrapf(err, "Cannot add auth [%v] to member [%v]. Invalid signature.", id, method)
// }
//
// return b.Bolt().Update(func(tx *bolt.Tx) error {
// return boltStoreMemberAuth(tx, id, method, auth)
// })
// }
//

// func (b *boltStorage) LoadTrust(id string) (d storedTrust, o bool, e error) {
// e = b.Bolt().View(func(tx *bolt.Tx) error {
// d, o, e = boltLoadTrust(tx, id)
// return e
// })
// return
// }

func (b *boltStorage) SaveTrust(dom Trust) error {
	return nil
	// var issuer storedMember
	// // issuer, err := EnsureMember(b, dom.cert.Issuer)
	// // if err != nil {
	// // return errors.WithStack(err)
	// // }
	//
	// if err := dom.Cert.Verify(dom.signingKey.Pub, issuer.Sign.Pub, issuer.Sign.Pub); err != nil {
	// return errors.Wrapf(err, "Unable to create trust [%v].  Invalid certificate.", dom.Id)
	// }
	//
	// return b.Bolt().Update(func(tx *bolt.Tx) error {
	// if err := boltStoreTrust(tx, dom.signingKey, dom.oracle); err != nil {
	// return err
	// }
	//
	// if err := boltStoreCert(tx, dom.Cert); err != nil {
	// return err
	// }
	//
	// if err := boltStoreTrustAuth(tx, dom.Id, dom.Cert.Trustee, dom.oracleKey); err != nil {
	// return err
	// }
	//
	// return nil
	// })
}

func (b *boltStorage) SaveCert(dom, member string) (d SignedCertificate, k SignedEncryptedShard, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		// d, o, e = boltLoadTrustAuth(tx, dom, member)
		// return e
		return nil
	})
	return
}

func (b *boltStorage) LoadCertByTrustAndMember(dom, member string) (d SignedCertificate, k SignedEncryptedShard, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		// d, o, e = boltLoadTrustAuth(tx, dom, member)
		// return e
		return nil
	})
	return
}

func boltStoreMember(tx *bolt.Tx, m Member) error {
	key := stash.UUID(m.Id)
	if err := boltEnsureEmpty(tx.Bucket(memberBucket), key); err != nil {
		return errors.Wrapf(err, "Member already exists [%v]", m.Id)
	}

	raw, err := gobBytes(m)
	if err != nil {
		return errors.Wrapf(err, "Error encoding member [%v]", m.Id)
	}

	// put on main index
	if err := tx.Bucket(memberBucket).Put(key, raw); err != nil {
		return errors.Wrapf(err, "Error writing member [%v]", m.Id)
	}

	return nil
}

func boltLoadMember(tx *bolt.Tx, id uuid.UUID) (s Member, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(memberBucket).Get(stash.UUID(id)), &s)
	return
}

func boltStoreAccessCode(tx *bolt.Tx, a AccessCode) error {
	raw, err := gobBytes(a)
	if err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	if err := tx.Bucket(memberAuthBucket).Put(stash.Key(a.Lookup()), raw); err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	return nil
}

func boltLoadAccessCode(tx *bolt.Tx, lookup []byte) (k AccessCode, o bool, e error) {
	o, e = parseGobBytes(
		tx.Bucket(memberAuthBucket).Get(lookup), &k)
	return
}

//
// func boltStoreTrust(tx *bolt.Tx, identity KeyPair, oracle SignedShard) error {
// id := identity.Pub.Id()
//
// if err := boltEnsureEmpty(tx.Bucket(trustBucket), stash.String(id)); err != nil {
// return errors.Wrapf(err, "Trust already exists [%v]", id)
// }
//
// raw, err := gobBytes(storedTrust{identity, oracle})
// if err != nil {
// return errors.Wrapf(err, "Error encoding trust [%v]", id)
// }
//
// // put on main index
// if err := tx.Bucket(trustBucket).Put(stash.String(id), raw); err != nil {
// return errors.Wrapf(err, "Error writing trust [%v]", id)
// }
//
// return nil
// }
//
// func boltLoadTrust(tx *bolt.Tx, id string) (d storedTrust, o bool, e error) {
// o, e = parseGobBytes(tx.Bucket(trustBucket).Get(stash.String(id)), &d)
// return
// }
//
// func boltStoreTrustAuth(tx *bolt.Tx, dom, sub uuid.UUID, o SignedEncryptedShard) error {
// rawKey, err := gobBytes(o)
// if err != nil {
// return errors.Wrapf(err, "Error encoding oracle key [%v]", o)
// }
//
// if err := tx.Bucket(trustAuthBucket).Put(stash.UUID(dom).ChildUUID(sub), rawKey); err != nil {
// return errors.Wrapf(err, "Error writing trust auth [%v,%v]", dom, sub)
// }
//
// return nil
// }
//
// func boltLoadTrustAuth(tx *bolt.Tx, dom, sub string) (k storedAuthenticator, o bool, e error) {
// o, e = parseGobBytes(tx.Bucket(trustAuthBucket).Get(stash.String(dom).ChildString(sub)), &k)
// return
// }
//
// func boltStoreCert(tx *bolt.Tx, c SignedCertificate) error {
// if err := boltEnsureEmpty(tx.Bucket(certBucket), stash.UUID(c.Id)); err != nil {
// return errors.Wrapf(err, "Cert already exists [%v]", c.Id)
// }
//
// rawCert, err := gobBytes(c)
// if err != nil {
// return errors.Wrapf(err, "Error encoding cert [%v]", c)
// }
//
// // store main cert
// if err := tx.Bucket(certBucket).Put(stash.UUID(c.Id), rawCert); err != nil {
// return errors.Wrapf(err, "Error writing cert [%v]", c.Id)
// }
//
// // update subcriber index
// if err := boltUpdateIndex(
// tx.Bucket(memberCertBucket), stash.UUID(c.Trustee), c.Id); err != nil {
// return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
// }
//
// // update trust index
// if err := boltUpdateIndex(
// tx.Bucket(trustCertBucket), stash.UUID(c.Trust), c.Id); err != nil {
// return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
// }
//
// // update latest index
// if err := boltUpdateIndex(
// tx.Bucket(certLatestBucket), stash.UUID(c.Trust).ChildUUID(c.Trustee), c.Id); err != nil {
// return errors.WithStack(err)
// }
//
// return nil
// }
//
// func boltStoreInvite(tx *bolt.Tx, i Invitation) error {
// if err := boltEnsureEmpty(tx.Bucket(inviteBucket), stash.UUID(i.Id)); err != nil {
// return errors.Wrapf(err, "Invite already exists [%v]", i.Id)
// }
//
// raw, err := gobBytes(i)
// if err != nil {
// return errors.WithStack(err)
// }
//
// // put on main index
// if err := tx.Bucket(inviteBucket).Put(stash.UUID(i.Id), raw); err != nil {
// return errors.WithStack(err)
// }
//
// // update member index: sub:/id:
// if err := boltUpdateIndex(
// tx.Bucket(memberInviteBucket), stash.UUID(i.Cert.Trustee), i.Id); err != nil {
// return errors.WithStack(err)
// }
//
// // update trust index: :dom:/id:
// if err := boltUpdateIndex(
// tx.Bucket(trustInviteBucket), stash.UUID(i.Cert.Trust), i.Id); err != nil {
// return errors.WithStack(err)
// }
//
// return nil
// }
//
// func boltLoadCertById(tx *bolt.Tx, id uuid.UUID) (i SignedCertificate, o bool, e error) {
// o, e = parseGobBytes(tx.Bucket(certBucket).Get(stash.UUID(id)), &i)
// return
// }
//
// func boltLoadInviteById(tx *bolt.Tx, id uuid.UUID) (i Invitation, o bool, e error) {
// o, e = parseGobBytes(tx.Bucket(inviteBucket).Get(stash.UUID(id)), &i)
// return
// }
//
// func boltLoadCertsByIds(tx *bolt.Tx, ids []uuid.UUID) ([]SignedCertificate, error) {
// certs := make([]SignedCertificate, 0, len(ids))
// for _, id := range ids {
// c, ok, err := boltLoadCertById(tx, id)
// if err != nil {
// return nil, err
// }
//
// if !ok {
// return nil, errors.Wrapf(StorageInvariantError, "Cert [%v] expected to exist.", id)
// }
//
// certs = append(certs, c)
// }
// return certs, nil
// }
//
// func boltLoadInvitesByIds(tx *bolt.Tx, ids []uuid.UUID) ([]Invitation, error) {
// invites := make([]Invitation, 0, len(ids))
// for _, id := range ids {
// c, ok, err := boltLoadInviteById(tx, id)
// if err != nil {
// return nil, err
// }
//
// if !ok {
// return nil, errors.Wrapf(StorageInvariantError, "Invite [%v] expected to exist.", id)
// }
//
// invites = append(invites, c)
// }
// return invites, nil
// }
//
// func boltLoadCertIdsByTrust(tx *bolt.Tx, dom string, beg, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(trustCertBucket), stash.String(dom), beg, end)
// }
//
// func boltLoadCertIdsByMember(tx *bolt.Tx, member string, beg int, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(memberCertBucket), stash.String(member), beg, end)
// }
//
// func loadBoltInviteIdsByTrust(tx *bolt.Tx, dom string, beg int, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(trustInviteBucket), stash.String(dom), beg, end)
// }
//
// func boltLoadInviteIdsByMember(tx *bolt.Tx, member string, beg int, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(memberInviteBucket), stash.String(member), beg, end)
// }
//
// func boltLoadCertIdByMemberAndTrust(tx *bolt.Tx, dom, sub string) (i uuid.UUID, o bool, e error) {
// raw := tx.Bucket(certLatestBucket).Get(stash.String(dom).ChildString(sub))
// if raw == nil {
// return
// }
//
// i, e = stash.ParseUUID(raw)
// return i, e == nil, nil
// }
//
// func boltUpdateActiveIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
// return errors.WithStack(bucket.Put(root, stash.UUID(id)))
// }
//
// func boltLoadCertByMemberAndTrust(tx *bolt.Tx, sub, dom string) (SignedCertificate, bool, error) {
// id, ok, err := boltLoadCertIdByMemberAndTrust(tx, sub, dom)
// if err != nil || !ok {
// return SignedCertificate{}, false, err
// }
// return boltLoadCertById(tx, id)
// }
//
// func boltLoadCertsByTrust(tx *bolt.Tx, dom string, beg, end int) ([]SignedCertificate, error) {
// ids, err := boltLoadCertIdsByTrust(tx, dom, beg, end)
// if err != nil {
// return nil, err
// }
// return boltLoadCertsByIds(tx, ids)
// }
//
// func boltLoadInvitesByTrust(tx *bolt.Tx, dom string, beg, end int) ([]Invitation, error) {
// ids, err := boltLoadCertIdsByTrust(tx, dom, beg, end)
// if err != nil {
// return nil, err
// }
// return boltLoadInvitesByIds(tx, ids)
// }
//
// func boltLoadCertsByMember(tx *bolt.Tx, dom string, beg, end int) ([]SignedCertificate, error) {
// ids, err := boltLoadCertIdsByMember(tx, dom, beg, end)
// if err != nil {
// return nil, err
// }
// return boltLoadCertsByIds(tx, ids)
// }
//
// func boltLoadInvitesByMember(tx *bolt.Tx, dom string, beg, end int) ([]Invitation, error) {
// ids, err := boltLoadCertIdsByMember(tx, dom, beg, end)
// if err != nil {
// return nil, err
// }
// return boltLoadInvitesByIds(tx, ids)
// }
//
// func boltUpdateIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
// return errors.WithStack(bucket.Put(stash.Key(root).ChildUUID(id), stash.UUID(id)))
// }
//
// func boltDeleteIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
// return errors.WithStack(bucket.Delete(stash.Key(root).ChildUUID(id)))
// }

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
