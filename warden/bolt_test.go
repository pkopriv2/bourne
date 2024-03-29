package warden

import (
	"crypto/rand"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	db, err := stash.OpenTransient(common.NewEmptyContext())
	if err != nil {
		t.FailNow()
		return
	}

	store, err := newBoltStorage(db)
	if err != nil {
		t.FailNow()
		return
	}

	if err := initBoltBuckets(db); err != nil {
		t.FailNow()
		return
	}

	t.Run("SaveMember", func(t *testing.T) {
		owner, e := GenRsaKey(rand.Reader, 1024)
		if !assert.Nil(t, e) {
			return
		}

		login := func() credential {
			return newSigningCred(lookupByKey(owner.Public()), owner)
		}

		creds, e := extractCreds(login)
		if !assert.Nil(t, e) {
			return
		}
		defer creds.Destroy()

		mem, shard, e := newMember(rand.Reader, uuid.NewV1(), uuid.NewV1(), creds)
		if !assert.Nil(t, e) {
			return
		}

		acct := newMemberAgreement(mem.Id, mem.SubscriptionId, Basic)

		args, e := creds.Auth(rand.Reader)
		if !assert.Nil(t, e) {
			return
		}

		auth, e := newMemberAuth(rand.Reader, mem.Id, shard, args)
		if !assert.Nil(t, e) {
			return
		}

		if !assert.Nil(t, store.SaveMember(acct, mem, auth, creds.MemberLookup())) {
			return
		}

		m, o, e := store.LoadMemberByLookup(creds.MemberLookup())
		if !assert.Nil(t, e) || !assert.True(t, o) {
			return
		}

		m, o, e = store.LoadMemberById(m.Id)
		if !assert.Nil(t, e) || !assert.True(t, o) {
			return
		}
		assert.Equal(t, mem, m)

		a, o, e := store.LoadMemberAuth(m.Id, creds.AuthId())
		if !assert.Nil(t, e) || !assert.True(t, o) {
			return
		}
		assert.Equal(t, mem.Id, a.MemberId)

		now, e := m.secret(a.Shard, login)
		assert.Nil(t, e)

		was, e := mem.secret(shard, login)
		assert.Nil(t, e)
		assert.Equal(t, was, now)

		owner2, err := GenRsaKey(rand.Reader, 1024)
		if !assert.Nil(t, err) {
			return
		}

		login2 := func() credential {
			return newSigningCred(lookupByKey(owner2.Public()), owner2)
		}

		_, e = mem.secret(shard, login2)
		assert.NotNil(t, e)
	})

	// t.Run("LoadMemberById_NoExist", func(t *testing.T) {
	// _, o, e := store.LoadMemberById(uuid.UUID{})
	// assert.Nil(t, e)
	// assert.False(t, o)
	// })
	//
	// t.Run("LoadMemberByLookup_NoExist", func(t *testing.T) {
	// _, _, o, e := store.LoadMemberByLookup([]byte{})
	// assert.Nil(t, e)
	// assert.False(t, o)
	// })
	//
	// t.Run("SaveMember_AlreadyExists", func(t *testing.T) {
	// owner, err := GenRsaKey(rand.Reader, 1024)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// login := func(pad KeyPad) error {
	// return pad.BySignature(owner)
	// }
	//
	// creds, e := enterCreds(login)
	//
	// mem, code, e := newMember(rand.Reader, creds)
	// assert.Nil(t, e)
	// assert.Nil(t, store.SaveMember(mem, code))
	// assert.NotNil(t, store.SaveMember(mem, code))
	// })

	// t.Run("SaveTrust", func(t *testing.T) {
	// memberKey, err := GenRsaKey(rand.Reader, 1024)
	// if err != nil {
	// t.FailNow()
	// return
	// }
	//
	// login := func(pad KeyPad) error {
	// return pad.BySignature(memberKey)
	// }
	//
	// creds, e := enterCreds(login)
	//
	// mem, code, e := newMember(rand.Reader, creds)
	// assert.Nil(t, e)
	// assert.Nil(t, store.SaveMember(mem, code))
	//
	// memSecret, e := mem.secret(code, login)
	// assert.Nil(t, e)
	//
	// memSigningKey, e := mem.signingKey(memSecret)
	// assert.Nil(t, e)
	//
	// trust, e := newTrust(rand.Reader, mem.Id, memSecret, memSigningKey, "test")
	// assert.Nil(t, e)
	// assert.NotNil(t, trust)
	// assert.Nil(t, store.SaveTrust(trust.core(), trust.trusteeCode(), trust.trusteeCert))
	//
	// actCore, o, e := store.LoadTrustCore(trust.Id)
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.NotNil(t, actCore)
	//
	// actCode, o, e := store.LoadTrustCode(trust.Id, mem.Id)
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.NotNil(t, actCode)
	//
	// actCert, o, e := store.LoadCertificateById(trust.trusteeCert.Id)
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.NotNil(t, actCert)
	//
	// actTrust := actCore.privateTrust(actCode, actCert)
	//
	// trustSecret, e := trust.deriveSecret(memSecret)
	// assert.Nil(t, e)
	//
	// actTrustSecret, e := actTrust.deriveSecret(memSecret)
	// assert.Nil(t, e)
	// assert.Equal(t, trustSecret, actTrustSecret)
	//
	// actCert2, o, e := store.LoadCertificateByMemberAndTrust(mem.Id, trust.Id)
	// assert.Nil(t, e)
	// assert.True(t, o)
	// assert.Equal(t, actCert, actCert2)
	// })
}
