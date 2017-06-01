package warden

import (
	"io"

	"github.com/pkg/errors"
)

// Password based credentials

// The server component
type passAuth struct {
	Ver  authProtocol
	Val  []byte
	Salt []byte
	Iter int
	Hash Hash
}

func newPassAuth(rand io.Reader, size, iter int, hash Hash, pass []byte) (passAuth, error) {
	salt, err := genRandomBytes(rand, size)
	if err != nil {
		return passAuth{}, errors.WithStack(err)
	}

	return passAuth{
		Ver:  PassV1,
		Val:  cryptoBytes(pass).Pbkdf2(salt, iter, size, hash.standard()),
		Salt: salt,
		Iter: iter,
		Hash: hash,
	}, nil
}

func (p *passAuth) Protocol() authProtocol {
	return p.Ver
}

func (p *passAuth) Authenticate(args []byte) error {
	derived := cryptoBytes(args).Pbkdf2(p.Salt, p.Iter, len(p.Val), p.Hash.standard())
	if !derived.Equals(p.Val) {
		return errors.Wrapf(UnauthorizedError, "Incorrect credentials")
	}
	return nil
}
