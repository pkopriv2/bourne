package warden

import (
	"io"

	"github.com/pkg/errors"
)

type passCred struct {
	lookup []byte
	pass   []byte // prehashed to avoid exposing consumer pass
}

func newPassCred(lookup []byte, pass []byte) *passCred {
	return &passCred{lookup, pass}
}

func (p *passCred) Lookup() []byte {
	return p.lookup
}

func (p *passCred) Destroy() {
	cryptoBytes(p.pass).Destroy()
}

func (p *passCred) Protocol() authProtocol {
	return PassV1
}

func (p *passCred) Auth(rand io.Reader) ([]byte, error) {
	hash, err := hashN(p.pass, SHA256, 2)
	return hash, errors.WithStack(err)
}

func (p *passCred) Seed([]byte) ([]byte, error) {
	hash, err := hashN(p.pass, SHA256, 1)
	return hash, errors.WithStack(err)
}

func (p *passCred) DecryptShard(shard memberShard) (Shard, error) {
	seed, err := p.Seed(shard.AuthArgs)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *passCred) EncryptShard(rand io.Reader, signer Signer, shard Shard) (memberShard, error) {
	key, err := p.Seed(p.pass)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	return memberShard{p.Lookup(), PassV1, enc, nil}, nil
}
