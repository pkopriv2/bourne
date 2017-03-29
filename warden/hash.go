package warden

import (
	"crypto"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/scribe"
)

// Common hash implementations.
//
// TODO: Fill out remaining
const (
	SHA1 Hash = iota
	SHA256
	SHA512
)

type Hash int

func (h Hash) Standard() hash.Hash {
	switch h {
	default:
		return nil
	case SHA1:
		return sha1.New()
	case SHA256:
		return sha256.New()
	case SHA512:
		return sha512.New()
	}
}

func (h Hash) Crypto() crypto.Hash {
	switch h {
	default:
		return 0
	case SHA1:
		return crypto.SHA1
	case SHA256:
		return crypto.SHA256
	case SHA512:
		return crypto.SHA512
	}
}

func (h Hash) String() string {
	switch h {
	default:
		return "Unknown"
	case SHA1:
		return "SHA1"
	case SHA256:
		return "SHA256"
	case SHA512:
		return "SHA512"
	}
}

func (h Hash) Hash(msg []byte) ([]byte, error) {
	impl := h.Standard()
	if _, err := impl.Write(msg); err != nil {
		return nil, errors.WithStack(err)
	}
	return impl.Sum(nil), nil
}

func (h Hash) Bytes() []byte {
	return scribe.Write(h).Bytes()
}

func (h Hash) Write(w scribe.Writer) {
	w.WriteInt("ord", int(h))
}

func ReadHash(r scribe.Reader) (h Hash, e error) {
	e = r.ReadInt("ord", (*int)(&h))
	return
}
