package warden

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
)

type passCred struct {
	acct []byte
	pass []byte // prehashed to avoid exposing consumer pass
}

func newPassCred(acct, pass []byte) *passCred {
	cop := make([]byte, len(pass))
	copy(cop, pass)
	return &passCred{acct, cop}
}

func (p *passCred) MemberLookup() []byte {
	return p.acct
}

func (p *passCred) AuthId() []byte {
	return stash.String("Passphrase://").ChildString("Default")
}

func (p *passCred) Destroy() {
	cryptoBytes(p.pass).Destroy()
}

func (p *passCred) Protocol() authProtocol {
	return PassV1
}

// In order to avoid breaking the zero knowledge guarantees,
// the authenticated value MUST not be able rederive the
// the encryption key of the member shard.
func (p *passCred) Auth(rand io.Reader) ([]byte, error) {
	hash, err := hashN(p.pass, SHA256, 2)
	return hash, errors.WithStack(err)
}

func (p *passCred) EncryptionKey() ([]byte, error) {
	hash, err := hashN(p.pass, SHA256, 1)
	return hash, errors.WithStack(err)
}

func (p *passCred) DecryptShard(shard memberShard) (Shard, error) {
	seed, err := p.EncryptionKey()
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *passCred) EncryptShard(rand io.Reader, signer Signer, shard Shard) (memberShard, error) {
	key, err := p.EncryptionKey()
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	return memberShard{p.AuthId(), PassV1, enc, nil}, nil
}
