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

// Bolt info
var (
	keyPubBucket       = []byte("warden.keys.public")
	keyPairBucket      = []byte("warden.keys.pair")
	keyOwnerBucket     = []byte("warden.keys.owner")
	certBucket         = []byte("warden.certs")
	certLatestBucket   = []byte("warden.certs.latest")
	certMemberBucket   = []byte("warden.certs.member")
	inviteBucket       = []byte("warden.invites")
	memberCoreBucket   = []byte("warden.member")
	memberLookupBucket = []byte("warden.member.lookup")
	memberAuthBucket   = []byte("warden.member.auth")
	memberCertBucket   = []byte("warden.member.certs")
	memberInviteBucket = []byte("warden.member.invites")
	trustBucket        = []byte("warden.trust")
	trustCodeBucket    = []byte("warden.trust.auth")
	trustCertBucket    = []byte("warden.trust.certs")
	trustInviteBucket  = []byte("warden.trust.invites")
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
		_, e = tx.CreateBucketIfNotExists(memberCoreBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberLookupBucket)
		err = common.Or(err, e)
		_, e = tx.CreateBucketIfNotExists(memberAuthBucket)
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

func (b *boltStorage) SaveMember(core memberCore, auth memberAuth, lookup []byte) error {
	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreMemberCore(tx, core); err != nil {
			return errors.WithStack(err)
		}

		if err := boltStoreMemberAuth(tx, auth); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(
			boltStoreMemberIdByLookup(tx, lookup, core.Id))
	})
}

func (b *boltStorage) LoadMemberById(id uuid.UUID) (m memberCore, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		m, o, e = boltLoadMemberCore(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadMemberIdByLookup(lookup []byte) (id uuid.UUID, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		id, o, e = boltLoadMemberIdByLookup(tx, lookup)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadMemberByLookup(lookup []byte) (m memberCore, o bool, e error) {
	var id uuid.UUID

	id, o, e = b.LoadMemberIdByLookup(lookup)
	if e != nil || !o {
		e = errors.WithStack(e)
		return
	}
	m, o, e = b.LoadMemberById(id)
	if e != nil {
		e = errors.WithStack(e)
		return
	}
	return
}

func (b *boltStorage) LoadMemberAuth(id uuid.UUID, authId []byte) (s memberAuth, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		s, o, e = boltLoadMemberAuth(tx, id, authId)
		return e
	})
	return
}

func (b *boltStorage) LoadTrustCore(id uuid.UUID) (d trustCore, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		d, o, e = boltLoadTrust(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadCertificateById(id uuid.UUID) (c SignedCertificate, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, o, e = boltLoadCertById(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadCertificateByMemberAndTrust(memberId, trustId uuid.UUID) (c SignedCertificate, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, o, e = boltLoadCertByMemberAndTrust(tx, memberId, trustId)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadCertificatesByMember(id uuid.UUID, opts PagingOptions) (c []SignedCertificate, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, e = boltLoadCertsByMember(tx, id, opts.Beg, opts.End)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadCertificatesByTrust(id uuid.UUID, opts PagingOptions) (c []SignedCertificate, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		c, e = boltLoadCertsByTrust(tx, id, opts.Beg, opts.End)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadInvitationById(id uuid.UUID) (i Invitation, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		i, o, e = boltLoadInviteById(tx, id)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadInvitationsByMember(id uuid.UUID, beg, end int) (i []Invitation, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		i, e = boltLoadInvitesByMember(tx, id, beg, end)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) RevokeCertificate(memberId, trustId uuid.UUID) (e error) {
	e = b.Bolt().Update(func(tx *bolt.Tx) error {
		e = boltRevokeCert(tx, memberId, trustId)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) LoadTrustCode(trustId, memberId uuid.UUID) (d trustCode, o bool, e error) {
	e = b.Bolt().View(func(tx *bolt.Tx) error {
		d, o, e = boltLoadTrustCode(tx, trustId, memberId)
		return errors.WithStack(e)
	})
	return
}

func (b *boltStorage) SaveTrust(core trustCore, code trustCode, cert SignedCertificate) error {
	// FIXME: Move to rpc_server.go (Assuming transactional problems don't arise)
	issuer, err := EnsureMember(b, cert.IssuerId)
	if err != nil {
		return errors.WithStack(err)
	}

	if core.Id != code.TrustId {
		return errors.Wrapf(StorageInvariantError, "Code and trust must match!")
	}

	if core.Id != cert.TrustId || code.MemberId != cert.TrusteeId || code.MemberId != cert.IssuerId {
		return errors.Wrapf(StorageInvariantError, "Cert not for trust and member!")
	}

	if err := cert.Verify(issuer.SigningKey.Pub, core.SigningKey.Pub, issuer.SigningKey.Pub); err != nil {
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

func (b *boltStorage) SaveInvitation(inv Invitation) error {
	// FIXME: Move to rpc_server.go (Assuming transactional problems don't arise)
	_, err := EnsureMember(b, inv.Cert.TrusteeId)
	if err != nil {
		return errors.WithStack(err)
	}

	issuer, err := EnsureMember(b, inv.Cert.IssuerId)
	if err != nil {
		return errors.WithStack(err)
	}

	trust, err := EnsureTrust(b, inv.Cert.TrustId)
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

func (b *boltStorage) SaveCertificate(cert SignedCertificate, code trustCode) error {
	// FIXME: Move to rpc_server.go (Assuming transactional problems don't arise)
	if code.MemberId != cert.TrusteeId || code.TrustId != cert.TrustId {
		return errors.Wrapf(StorageInvariantError, "Inconsistent data")
	}

	trustee, err := EnsureMember(b, cert.TrusteeId)
	if err != nil {
		return errors.WithStack(err)
	}

	issuer, err := EnsureMember(b, cert.IssuerId)
	if err != nil {
		return errors.WithStack(err)
	}

	trust, err := EnsureTrust(b, cert.TrustId)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := cert.Verify(trust.SigningKey.Pub, issuer.SigningKey.Pub, trustee.SigningKey.Pub); err != nil {
		return errors.Wrapf(err, "Invalid certificate [%v]", cert)
	}

	return b.Bolt().Update(func(tx *bolt.Tx) error {
		if err := boltStoreCert(tx, cert); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(boltStoreTrustCode(tx, code))
	})
}

func boltStoreMemberCore(tx *bolt.Tx, m memberCore) error {
	if err := boltEnsureEmpty(tx.Bucket(memberCoreBucket), stash.UUID(m.Id)); err != nil {
		return errors.Wrapf(err, "Member already exists [%v]", m.Id)
	}

	raw, err := gobBytes(m)
	if err != nil {
		return errors.Wrapf(err, "Error encoding member [%v]", m.Id)
	}

	// put on main index
	if err := tx.Bucket(memberCoreBucket).Put(stash.UUID(m.Id), raw); err != nil {
		return errors.Wrapf(err, "Error writing member [%v]", m.Id)
	}

	return nil
}

func boltLoadMemberCore(tx *bolt.Tx, id uuid.UUID) (s memberCore, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(memberCoreBucket).Get(stash.UUID(id)), &s)
	return
}

func boltLoadMemberIdByLookup(tx *bolt.Tx, lookup []byte) (s uuid.UUID, o bool, e error) {
	val := tx.Bucket(memberLookupBucket).Get(lookup)
	o = val != nil
	if !o {
		return
	}

	s, e = stash.ParseUUID(val)
	return
}

func boltStoreMemberIdByLookup(tx *bolt.Tx, lookup []byte, id uuid.UUID) error {
	e := tx.Bucket(memberLookupBucket).Put(lookup, stash.UUID(id))
	return errors.WithStack(e)
}

func boltStoreMemberAuth(tx *bolt.Tx, a memberAuth) error {
	if err := boltEnsureEmpty(
		tx.Bucket(memberAuthBucket), a.Id()); err != nil {
		return errors.Wrap(err, "Access code for lookup already exists.")
	}

	raw, err := gobBytes(a)
	if err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	if err := tx.Bucket(memberAuthBucket).Put(a.Id(), raw); err != nil {
		return errors.Wrapf(err, "Error encoding access code [%v]", a)
	}
	return nil
}

func boltLoadMemberAuth(tx *bolt.Tx, id uuid.UUID, auth []byte) (k memberAuth, o bool, e error) {
	o, e = parseGobBytes(
		tx.Bucket(memberAuthBucket).Get(stash.UUID(id).Child(auth)), &k)
	return
}

func boltStoreTrust(tx *bolt.Tx, core trustCore) error {
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

func boltLoadTrust(tx *bolt.Tx, id uuid.UUID) (d trustCore, o bool, e error) {
	o, e = parseGobBytes(tx.Bucket(trustBucket).Get(stash.UUID(id)), &d)
	return
}

func boltStoreTrustCode(tx *bolt.Tx, code trustCode) error {
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

func boltLoadTrustCode(tx *bolt.Tx, trustId uuid.UUID, memberId uuid.UUID) (c trustCode, o bool, e error) {
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
	if err := boltUpdateListIndex(tx.Bucket(memberCertBucket), stash.UUID(c.TrusteeId), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update trust index
	if err := boltUpdateListIndex(tx.Bucket(trustCertBucket), stash.UUID(c.TrustId), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	if err := tx.Bucket(certLatestBucket).Put(stash.UUID(c.TrusteeId).ChildUUID(c.TrustId), stash.UUID(c.Id)); err != nil {
		return errors.Wrapf(err, "Error writing cert active index [%v]", c.Id)
	}

	return nil
}

func boltRevokeCert(tx *bolt.Tx, memberId, trustId uuid.UUID) error {
	c, ok, err := boltLoadCertByMemberAndTrust(tx, memberId, trustId)
	if err != nil {
		return errors.WithStack(err)
	}

	if !ok {
		return errors.Wrapf(TrustError, "No certificate between trust [%v] and member [%v]", trustId, memberId)
	}

	// update by member index
	if err := boltDeleteListIndex(tx.Bucket(memberCertBucket), stash.UUID(c.TrusteeId), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update by trust index
	if err := boltDeleteListIndex(tx.Bucket(trustCertBucket), stash.UUID(c.TrustId), c.Id); err != nil {
		return errors.Wrapf(err, "Error writing cert index [%v]", c.Id)
	}

	// update by member and trust.
	if err := tx.Bucket(certLatestBucket).Delete(stash.UUID(c.TrusteeId).ChildUUID(c.TrustId)); err != nil {
		return errors.Wrapf(err, "Error writing cert active index [%v]", c.Id)
	}

	return nil
}

func boltLoadCertIdsByTrust(tx *bolt.Tx, id uuid.UUID, beg, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(trustCertBucket), stash.UUID(id), beg, end)
}

func boltLoadCertIdsByMember(tx *bolt.Tx, id uuid.UUID, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(memberCertBucket), stash.UUID(id), beg, end)
}

func boltLoadInviteIdsByTrust(tx *bolt.Tx, id uuid.UUID, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(trustInviteBucket), stash.UUID(id), beg, end)
}

func boltLoadInviteIdsByMember(tx *bolt.Tx, id uuid.UUID, beg int, end int) ([]uuid.UUID, error) {
	return boltScanIndex(tx.Bucket(memberInviteBucket), stash.UUID(id), beg, end)
}

func boltLoadCertIdByMemberAndTrust(tx *bolt.Tx, memberId, trustId uuid.UUID) (i uuid.UUID, o bool, e error) {
	raw := tx.Bucket(certLatestBucket).Get(stash.UUID(memberId).ChildUUID(trustId))
	if raw == nil {
		return
	}

	i, e = stash.ParseUUID(raw)
	return i, e == nil, e
}

func boltLoadCertByMemberAndTrust(tx *bolt.Tx, memberId, trustId uuid.UUID) (SignedCertificate, bool, error) {
	id, ok, err := boltLoadCertIdByMemberAndTrust(tx, memberId, trustId)
	if err != nil || !ok {
		return SignedCertificate{}, false, errors.WithStack(err)
	}
	return boltLoadCertById(tx, id)
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

	// update member index: (memberId -> )
	if err := boltUpdateListIndex(
		tx.Bucket(memberInviteBucket), stash.UUID(i.Cert.TrusteeId), i.Id); err != nil {
		return errors.WithStack(err)
	}

	// update trust index: :dom:/id:
	if err := boltUpdateListIndex(
		tx.Bucket(trustInviteBucket), stash.UUID(i.Cert.TrustId), i.Id); err != nil {
		return errors.WithStack(err)
	}

	return nil
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

func boltLoadInvitesByTrust(tx *bolt.Tx, trustId uuid.UUID, beg, end int) ([]Invitation, error) {
	ids, err := boltLoadInviteIdsByTrust(tx, trustId, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadInvitesByIds(tx, ids)
}

func boltLoadInvitesByMember(tx *bolt.Tx, memberId uuid.UUID, beg, end int) ([]Invitation, error) {
	ids, err := boltLoadInviteIdsByMember(tx, memberId, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadInvitesByIds(tx, ids)
}

func boltLoadCertsByTrust(tx *bolt.Tx, trustId uuid.UUID, beg, end int) ([]SignedCertificate, error) {
	ids, err := boltLoadCertIdsByTrust(tx, trustId, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadCertsByIds(tx, ids)
}

func boltLoadCertsByMember(tx *bolt.Tx, memberId uuid.UUID, beg, end int) ([]SignedCertificate, error) {
	ids, err := boltLoadCertIdsByMember(tx, memberId, beg, end)
	if err != nil {
		return nil, err
	}
	return boltLoadCertsByIds(tx, ids)
}

func boltLoadIndex(bucket *bolt.Bucket, lookup []byte) (uuid.UUID, bool, error) {
	val := bucket.Get(lookup)
	if val == nil {
		return uuid.UUID{}, false, nil
	}

	ret, err := stash.ParseUUID(val)
	return ret, true, errors.WithStack(err)
}

func boltUpdateListIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Put(stash.Key(root).ChildUUID(id), stash.UUID(id)))
}

func boltDeleteListIndex(bucket *bolt.Bucket, root []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Delete(stash.Key(root).ChildUUID(id)))
}

func boltUpdateUniqueIndex(bucket *bolt.Bucket, key []byte, id uuid.UUID) error {
	return errors.WithStack(bucket.Put(stash.Key(key), stash.UUID(id)))
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
