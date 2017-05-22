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

// TODO:
//   * Build more robust certificate checking.

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
	memberCodeBucket   = []byte("warden.member.auth")
	memberAuxBucket    = []byte("warden.member.aux")
	memberCertBucket   = []byte("warden.members.certs")
	memberInviteBucket = []byte("warden.members.invites")
	trustBucket        = []byte("warden.trusts")
	trustCodeBucket    = []byte("warden.trusts.auth")
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
		_, e = tx.CreateBucketIfNotExists(memberCodeBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberAuxBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberCertBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberInviteBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(trustCodeBucket)
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

func (b *boltStorage) SaveMember(mem Member, code MemberCode) error {
	if mem.Id != code.MemberId {
		return errors.Wrapf(StorageInvariantError, "Access code not for member.")
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreMember(tx, mem); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(boltStoreAccessCode(tx, code))
	})
}

func (b *boltStorage) LoadMemberById(id uuid.UUID) (m Member, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		m, o, e = boltLoadMember(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadMemberCode(lookup []byte) (s MemberCode, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		s, o, e = boltLoadAccessCode(tx, lookup)
		return e
	})
	return
}

func (b *boltStorage) LoadMemberByLookup(lookup []byte) (m Member, s MemberCode, o bool, e error) {
	s, o, e = b.LoadMemberCode(lookup)
	if e != nil || !o {
		return Member{}, MemberCode{}, false, errors.WithStack(e)
	}

	m, o, e = b.LoadMemberById(s.MemberId)
	if e != nil || !o {
		return Member{}, MemberCode{}, false, errors.WithStack(e)
	}
	return
}

func (b *boltStorage) LoadTrustCore(id uuid.UUID) (d TrustCore, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		d, o, e = boltLoadTrust(tx, id)
		return e
	})
	return
}

func (b *boltStorage) LoadCertificate(id uuid.UUID) (c SignedCertificate, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, o, e = boltLoadCertById(tx, id)
		return e
	})
	return
}

func (b *boltStorage) LoadCertificateByMemberAndTrust(memberId, trustId uuid.UUID) (c SignedCertificate, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, o, e = boltLoadCertByMemberAndTrust(tx, memberId, trustId)
		return e
	})
	return
}

func (b *boltStorage) LoadInvitationById(id uuid.UUID) (i Invitation, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		i, o, e = boltLoadInviteById(tx, id)
		return e
	})
	return
}

func (b *boltStorage) LoadTrustCode(trustId, memberId uuid.UUID) (d TrustCode, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		d, o, e = boltLoadTrustCode(tx, trustId, memberId)
		return e
	})
	return
}

func (b *boltStorage) SaveTrust(core TrustCore, code TrustCode, cert SignedCertificate) error {
	// FIXME: This isn't transactionally safe.  A member may be deleted after 'ensure'
	// but before storing the trust.  However, if member's can't be deleted, it's okay.
	issuer, err := EnsureMember(b, cert.Issuer)
	if err != nil {
		return errors.WithStack(err)
	}

	if core.Id != code.TrustId {
		return errors.Wrapf(StorageInvariantError, "Code and trust must match!")
	}

	if core.Id != cert.Trust || code.MemberId != cert.Trustee {
		return errors.Wrapf(StorageInvariantError, "Cert not for trust and member!")
	}

	if err := cert.Verify(core.SigningKey.Pub, issuer.SigningKey.Pub, issuer.SigningKey.Pub); err != nil {
		return errors.Wrapf(err, "Invalid certificate: %v", cert)
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreTrust(tx, core); err != nil {
			return errors.WithStack(err)
		}
		if err := boltStoreTrustCode(tx, code); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(boltStoreCert(tx, cert))
	})
}

// func (b *boltStorage) LoadInvitationById(id uuid.UUID) (Invitaton, bool, error) {
//
// }

func (b *boltStorage) SaveInvitation(inv Invitation) error {
	// FIXME: This isn't transactionally safe.  A member may be deleted after 'ensure'
	// but before storing the invitation.
	_, err := EnsureMember(b, inv.Cert.Trustee)
	if err != nil {
		return errors.WithStack(err)
	}

	issuer, err := EnsureMember(b, inv.Cert.Issuer)
	if err != nil {
		return errors.WithStack(err)
	}

	trust, err := EnsureTrust(b, inv.Cert.Trust)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := inv.Cert.Verify(issuer.SigningKey.Pub, inv.IssuerSig); err != nil {
		return errors.Wrapf(err, "Invalid issuer signature: %v", inv)
	}

	if err := inv.Cert.Verify(trust.SigningKey.Pub, inv.TrustSig); err != nil {
		return errors.Wrapf(err, "Invalid trust signature: %v", inv)
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		return errors.WithStack(boltStoreInvite(tx, inv))
	})
}

func (b *boltStorage) SaveCertificate(cert SignedCertificate) error {
	// FIXME: This isn't transactionally safe.  A member may be deleted after 'ensure'
	// but before storing the invitation.
	trustee, err := EnsureMember(b, cert.Trustee)
	if err != nil {
		return errors.WithStack(err)
	}

	issuer, err := EnsureMember(b, cert.Issuer)
	if err != nil {
		return errors.WithStack(err)
	}

	trust, err := EnsureTrust(b, cert.Trust)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := cert.Verify(trust.SigningKey.Pub, issuer.SigningKey.Pub, trustee.SigningKey.Pub); err != nil {
		return errors.Wrapf(err, "Invalid certificate [%v]", cert)
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		return errors.WithStack(boltStoreCert(tx, cert))
	})
}

// func (b *boltStorage) LoadCertByTrustAndMember(trustId, memberId string) (d SignedCertificate, k SignedEncryptedShard, o bool, e error) {
// e = b.Bolt().View(func(tx *bolt.Tx) error {
// // d, o, e = boltLoadTrustAuth(tx, dom, member)
// // return e
// return nil
// })
// return
// }

func boltStoreMember(tx *bolt.Tx, m Member) error {
	if err := boltEnsureEmpty(tx.Bucket(memberBucket), stash.UUID(m.Id)); err != nil {
		return errors.Wrapf(err, "Member already exists [%v]", m.Id)
	}

	raw, err := gobBytes(m)
	if err != nil {
		return errors.Wrapf(err, "Error encoding member [%v]", m.Id)
	}

	// put on main index
	if err := tx.Bucket(memberBucket).Put(stash.UUID(m.Id), raw); err != nil {
		return errors.Wrapf(err, "Error writing member [%v]", m.Id)
	}

	return nil
}

func boltLoadMember(tx *bolt.Tx, id uuid.UUID) (s Member, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(memberBucket).Get(stash.UUID(id)), &s)
	return
}

func boltStoreAccessCode(tx *bolt.Tx, a MemberCode) error {
	if err := boltEnsureEmpty(tx.Bucket(memberCodeBucket), a.Lookup()); err != nil {
		return errors.Wrap(err, "Access code for lookup already exists.")
	}

	raw, err := gobBytes(a)
	if err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	if err := tx.Bucket(memberCodeBucket).Put(a.Lookup(), raw); err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	return nil
}

func boltLoadAccessCode(tx *bolt.Tx, lookup []byte) (k MemberCode, o bool, e error) {
	o, e = parseGobBytes(
		tx.Bucket(memberCodeBucket).Get(lookup), &k)
	return
}

func boltStoreTrust(tx *bolt.Tx, core TrustCore) error {
	if err := boltEnsureEmpty(tx.Bucket(trustBucket), stash.UUID(core.Id)); err != nil {
		return errors.Wrapf(err, "Trust already exists [%v]", core.Id)
	}

	raw, err := gobBytes(core)
	if err != nil {
		return errors.Wrapf(err, "Error encoding trust [%v]", core.Id)
	}

	// put on main index
	if err := tx.Bucket(trustBucket).Put(stash.UUID(core.Id), raw); err != nil {
		return errors.Wrapf(err, "Error writing trust [%v]", core.Id)
	}

	return nil
}

func boltLoadTrust(tx *bolt.Tx, id uuid.UUID) (d TrustCore, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(trustBucket).Get(stash.UUID(id)), &d)
	return
}

func boltStoreTrustCode(tx *bolt.Tx, code TrustCode) error {
	key := stash.UUID(code.TrustId).ChildUUID(code.MemberId)

	if err := boltEnsureEmpty(tx.Bucket(trustCodeBucket), key); err != nil {
		return errors.Wrapf(err, "Trust [%v] already has access code [%v]", code.TrustId, code.MemberId)
	}

	raw, err := gobBytes(code)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := tx.Bucket(trustCodeBucket).Put(key, raw); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltLoadTrustCode(tx *bolt.Tx, trustId uuid.UUID, memberId uuid.UUID) (c TrustCode, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(trustCodeBucket).Get(stash.UUID(trustId).ChildUUID(memberId)), &c)
	return
}

func boltLoadCertById(tx *bolt.Tx, id uuid.UUID) (c SignedCertificate, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(certBucket).Get(stash.UUID(id)), &c)
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

	// update member index
	if err := boltUpdateIndex(
		tx.Bucket(memberCertBucket), stash.UUID(c.Trustee), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update trust index
	if err := boltUpdateIndex(
		tx.Bucket(trustCertBucket), stash.UUID(c.Trust), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	if err := tx.Bucket(certLatestBucket).Put(stash.UUID(c.Trustee).ChildUUID(c.Trust), stash.UUID(c.Id)); err != nil {
		return errors.Wrapf(err, "Error writing cert active index [%v]", c.Id)
	}

	return nil
}

// func boltLoadCertIdsByTrust(tx *bolt.Tx, dom string, beg, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(trustCertBucket), stash.String(dom), beg, end)
// }

// func boltLoadCertIdsByMember(tx *bolt.Tx, member string, beg int, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(memberCertBucket), stash.String(member), beg, end)
// }
//
// func loadBoltInviteIdsByTrust(tx *bolt.Tx, trustId uuid.UUID, beg int, end int) ([]uuid.UUID, error) {
// return boltScanIndex(tx.Bucket(trustInviteBucket), stash.String(dom), beg, end)
// }

func boltLoadInviteIdsByMember(tx *bolt.Tx, member string, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(memberInviteBucket), stash.String(member), beg, end)
}

func boltLoadCertIdByMemberAndTrust(tx *bolt.Tx, memberId, trustId uuid.UUID) (i uuid.UUID, o bool, e error) {
	raw := tx.Bucket(certLatestBucket).Get(stash.UUID(memberId).ChildUUID(trustId))
	if raw == nil {
		return
	}

	i, e = stash.ParseUUID(raw)
	return i, e == nil, e
}

// func boltUpdateActiveIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
// return errors.WithStack(bucket.Put(root, stash.UUID(id)))
// }

func boltLoadCertByMemberAndTrust(tx *bolt.Tx, memberId, trustId uuid.UUID) (SignedCertificate, bool, error) {
	id, ok, err := boltLoadCertIdByMemberAndTrust(tx, memberId, trustId)
	if err != nil || !ok {
		return SignedCertificate{}, false, errors.WithStack(err)
	}
	return boltLoadCertById(tx, id)
}

// func boltLoadCertsByTrust(tx *bolt.Tx, dom string, beg, end int) ([]SignedCertificate, error) {
// ids, err := boltLoadCertIdsByTrust(tx, dom, beg, end)
// if err != nil {
// return nil, err
// }
// return boltLoadCertsByIds(tx, ids)
// }

//
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

	// update member index: (memberId -> )
	if err := boltUpdateIndex(
		tx.Bucket(memberInviteBucket), stash.UUID(i.Cert.Trustee), i.Id); err != nil {
		return errors.WithStack(err)
	}

	// update trust index: :dom:/id:
	if err := boltUpdateIndex(
		tx.Bucket(trustInviteBucket), stash.UUID(i.Cert.Trust), i.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func boltLoadInviteById(tx *bolt.Tx, id uuid.UUID) (i Invitation, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(inviteBucket).Get(stash.UUID(id)), &i)
	return
}

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
func boltUpdateIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Put(stash.Key(root).ChildUUID(id), stash.UUID(id)))
}

func boltDeleteIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Delete(stash.Key(root).ChildUUID(id)))
}

func boltEnsureEmpty(bucket *bolt.Bucket, key []byte) error {
	if val := bucket.Get(key); val != nil {
		return errors.Wrapf(StorageInvariantError, "Key not empty [%v]", cryptoBytes(key).Hex())
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
