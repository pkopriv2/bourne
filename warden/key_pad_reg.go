package warden

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

type regKeyPad struct {
	ctx    common.Context
	deps   Dependencies
	opts   SessionOptions
	lookup []byte
	token  []byte
}

func (s *regKeyPad) BySignature(signer Signer, strength ...Strength) (Session, error) {
	session, err := s.register(func() credential {
		return newSigningCred(s.lookup, signer, strength...)
	})
	return session, errors.WithStack(err)
}

func (s *regKeyPad) ByPassword(pass string) (Session, error) {
	hash, err := SHA256.Hash([]byte(pass))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cryptoBytes([]byte(pass)).Destroy()

	session, err := s.register(func() credential {
		return newPassCred(s.lookup, hash)
	})
	return session, errors.WithStack(err)
}

func (s *regKeyPad) register(login func() credential) (Session, error) {
	panic("not implemented")
}
