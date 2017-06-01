package warden

import "github.com/pkopriv2/bourne/common"

type registrar struct {
	ctx   common.Context
	opts  SessionOptions
	token SignedToken
}

func (r *registrar) RegisterByEmail(email string) KeyPad {
	return &keyPad{r.ctx, r.opts, lookupByEmail(email), true, r.token}
}

func (r *registrar) RegisterByKey(key PublicKey) KeyPad {
	return &keyPad{r.ctx, r.opts, lookupByKey(key), true, r.token}
}
