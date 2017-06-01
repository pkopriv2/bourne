package warden

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type keyPad struct {
	ctx   common.Context
	opts  SessionOptions
	acct  []byte
	reg   bool
	token SignedToken
}

func (a keyPad) signingCreds(signer Signer, strength ...Strength) func() credential {
	return func() credential {
		return newSigningCred(a.acct, signer, strength...)
	}
}

func (a keyPad) passphraseCreds(pass string) (func() credential, error) {
	// hash and immediately destroy the pass phrase
	hash, err := hashN([]byte(pass), SHA256, 1)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cryptoBytes([]byte(pass)).Destroy()

	return func() credential {
		return newPassCred(a.acct, hash)
	}, nil
}

func (a keyPad) AuthBySignature(signer Signer, strength ...Strength) (s Session, e error) {
	login := a.signingCreds(signer, strength...)

	if a.reg {
		s, e = a.register(a.token, login)
	} else {
		s, e = a.authenticate(login)
	}
	return s, errors.WithStack(e)
}

func (a keyPad) AuthByPassphrase(phrase string) (s Session, e error) {
	creds, e := a.passphraseCreds(phrase)
	if e != nil {
		return nil, errors.WithStack(e)
	}

	if a.reg {
		s, e = a.register(a.token, creds)
	} else {
		s, e = a.authenticate(creds)
	}
	return s, errors.WithStack(e)
}

func (a keyPad) register(token SignedToken, login func() credential) (Session, error) {
	creds := login()
	defer creds.Destroy()

	core, shard, err := newMember(a.opts.deps.Rand, token.MemberId, token.SubscriberId, creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	auth, err := creds.Auth(a.opts.deps.Rand)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	timer := a.ctx.Timer(a.opts.Timeout)
	defer timer.Closed()

	token, err = a.opts.deps.Net.Register(timer.Closed(), token, core, shard, creds.MemberLookup(), auth, a.opts.TokenTtl)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	session, err := newSession(a.ctx, core, shard, token, login, a.opts, *a.opts.deps)
	return session, errors.WithStack(err)
}

func (a keyPad) authenticate(login func() credential) (Session, error) {
	creds := login()
	defer creds.Destroy()

	auth, err := creds.Auth(a.opts.deps.Rand)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	timer := a.ctx.Timer(a.opts.Timeout)
	defer timer.Closed()

	token, err := a.opts.deps.Net.Authenticate(timer.Closed(), creds.MemberLookup(), creds.AuthId(), auth, a.opts.TokenTtl)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	core, code, found, err := a.opts.deps.Net.MemberByIdAndAuth(timer.Closed(), token, token.MemberId, creds.AuthId())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !found {
		return nil, errors.Wrapf(UnauthorizedError, "No such member")
	}

	session, err := newSession(a.ctx, core, code, token, login, a.opts, *a.opts.deps)
	return session, errors.WithStack(err)
}
