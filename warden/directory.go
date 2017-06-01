package warden

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

func lookupByKey(key PublicKey) []byte {
	return stash.String("Signer://").ChildString(key.Id())
}

func lookupByEmail(email string) []byte {
	return stash.String("Email://").ChildString(email)
}

func lookupById(id uuid.UUID) []byte {
	return stash.String("Id://").ChildUUID(id)
}

type directory struct {
	ctx  common.Context
	opts SessionOptions
}

func (d directory) LookupByKey(key PublicKey) KeyPad {
	return keyPad{d.ctx, d.opts, lookupByKey(key), false, SignedToken{}}
}

func (d directory) LookupByEmail(email string) KeyPad {
	return keyPad{d.ctx, d.opts, lookupByEmail(email), false, SignedToken{}}
}

func (d directory) LookupById(id uuid.UUID) KeyPad {
	return keyPad{d.ctx, d.opts, lookupById(id), false, SignedToken{}}
}
