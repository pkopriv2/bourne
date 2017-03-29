package warden

import (
	"crypto/rsa"
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

// Public key implementation.
type rsaPublicKey struct {
	raw *rsa.PublicKey
}

func (r *rsaPublicKey) Algorithm() KeyAlgorithm {
	return RSA
}

func (r *rsaPublicKey) Verify(hash Hash, msg []byte, sig []byte) error {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}
	if err := rsa.VerifyPSS(r.raw, hash.Crypto(), hashed, sig, nil); err != nil {
		return errors.Wrapf(err, "Unable to verify signature [%v] with key [%v]", Bytes(sig), r.raw)
	}
	return nil
}

func (r *rsaPublicKey) Write(w scribe.Writer) {
	w.WriteBigInt("n", r.raw.N)
	w.WriteInt("e", r.raw.E)
}

func (r *rsaPublicKey) Bytes() []byte {
	return scribe.Write(r).Bytes()
}

func parseRsaPublicKey(bytes []byte) (*rsaPublicKey, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return readRsaPublicKey(msg)
}

func readRsaPublicKey(r scribe.Reader) (k *rsaPublicKey, e error) {
	k = &rsaPublicKey{&rsa.PublicKey{}}
	e = r.ReadBigInt("n", &k.raw.N)
	e = common.Or(e, r.ReadInt("e", &k.raw.E))
	return
}

// Private key implementation
type rsaPrivateKey struct {
	raw *rsa.PrivateKey
}

func (r *rsaPrivateKey) Algorithm() KeyAlgorithm {
	return RSA
}

func (r *rsaPrivateKey) public() *rsaPublicKey {
	return &rsaPublicKey{&r.raw.PublicKey}
}

func (r *rsaPrivateKey) Public() PublicKey {
	return r.public()
}

func (r *rsaPrivateKey) Sign(rand io.Reader, hash Hash, msg []byte) ([]byte, error) {
	hashed, err := hash.Hash(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to hash message [%v] using alg [%v]", Bytes(msg), hash)
	}

	sig, err := rsa.SignPSS(rand, r.raw, hash.Crypto(), hashed, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to sign msg [%v]", Bytes(msg))
	}

	return sig, nil
}

func (r *rsaPrivateKey) Bytes() []byte {
	return scribe.Write(r).Bytes()
}

func (r *rsaPrivateKey) Write(w scribe.Writer) {
	w.WriteInt("e", r.raw.E)
	w.WriteBigInt("n", r.raw.N)
	w.WriteBigInt("d", r.raw.D)
	w.WriteBigInts("primes", r.raw.Primes)
}

func parseRsaPrivateKey(bytes []byte) (*rsaPrivateKey, error) {
	msg, err := scribe.Parse(bytes)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return readRsaPrivateKey(msg)
}

func readRsaPrivateKey(r scribe.Reader) (k *rsaPrivateKey, e error) {
	k = &rsaPrivateKey{&rsa.PrivateKey{}}
	e = r.ReadInt("e", &k.raw.E)
	e = common.Or(e, r.ReadBigInt("n", &k.raw.N))
	e = common.Or(e, r.ReadBigInt("d", &k.raw.D))
	e = common.Or(e, r.ReadBigInts("primes", &k.raw.Primes))
	return
}
