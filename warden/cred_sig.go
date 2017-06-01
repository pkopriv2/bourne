package warden

import (
	"io"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
)

// internal only (never stored)
type signingCred struct {
	lookup []byte
	Signer Signer
	Hash   Hash
	Skew   time.Duration
}

func newSigningCred(lookup []byte, signer Signer, strength ...Strength) *signingCred {
	return &signingCred{lookup, signer, SHA256, 5 * time.Minute}
}

func (p *signingCred) MemberLookup() []byte {
	return p.lookup
}

func (p *signingCred) AuthId() []byte {
	return stash.String("Signer://").ChildString(p.Signer.Public().Id())
}

func (p *signingCred) Destroy() {
}

func (p *signingCred) Protocol() authProtocol {
	return SignV1
}

func (p *signingCred) Auth(rand io.Reader) ([]byte, error) {
	now := time.Now()

	fmt, err := gobBytes(now)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sig, err := p.Signer.Sign(rand, p.Hash, fmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret, err := gobBytes(signArgs{p.Signer.Public(), now, sig, p.Skew})
	return ret, errors.WithStack(err)
}

func (p *signingCred) Seed(nonce []byte) ([]byte, error) {
	sig, err := p.Signer.Sign(rand.New(rand.NewSource(0)), p.Hash, nonce)
	return sig.Data, errors.WithStack(err)
}

func (p *signingCred) DecryptShard(shard memberShard) (Shard, error) {
	seed, err := p.Seed(shard.AuthArgs)
	if err != nil {
		return SignedShard{}, errors.WithStack(err)
	}

	raw, err := shard.Raw.Decrypt(seed)
	return raw, errors.WithStack(err)
}

func (p *signingCred) EncryptShard(rand io.Reader, signer Signer, shard Shard) (memberShard, error) {
	nonce, err := genRandomBytes(rand, 32)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	key, err := p.Seed(nonce)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	enc, err := encryptAndSignShard(rand, signer, shard, key)
	if err != nil {
		return memberShard{}, errors.WithStack(err)
	}

	return memberShard{p.AuthId(), SignV1, enc, nonce}, nil
}
