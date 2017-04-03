package warden

import (
	"io"

	"github.com/pkg/errors"
)

// Options for generating an oracle
type oracleOptions struct {
	Strength   int
	DerivHash  Hash
	DerivIter  int
	DeriveSize int
}

func buildOracleOptions(fns ...func(*oracleOptions)) oracleOptions {
	ret := oracleOptions{bits_256, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Options for generating an oracle key
type oracleKeyOptions struct {
	Cipher  SymmetricCipher
	KeyHash Hash
	KeyIter int
	KeySize int
}

func buildOracleKeyOptions(fns ...func(*oracleKeyOptions)) oracleKeyOptions {
	ret := oracleKeyOptions{AES_256_GCM, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func generateOracle(rand io.Reader, id string, alias string, fns ...func(*oracleOptions)) (oracle, line, error) {
	opts := buildOracleOptions(fns...)

	ret, err := generateLine(rand, opts.Strength)
	if err != nil {
		return oracle{}, line{}, errors.Wrapf(err, "Error generating curve of strength [%v]", opts.Strength)
	}

	salt, err := generateRandomBytes(rand, opts.Strength)
	if err != nil {
		return oracle{}, line{}, errors.Wrapf(err, "Error generating salt of strength [%v]", opts.Strength)
	}

	pub, err := generatePoint(rand, ret, opts.Strength)
	if err != nil {
		return oracle{}, line{}, errors.Wrapf(err, "Error generating point of strength [%v]", opts.Strength)
	}

	return oracle{id, pub, opts.Strength, opts.DerivHash, salt, opts.DerivIter}, ret, nil
}

// Generates a new key for the oracle whose secret was derived from line.  Only the given pass
// may unlock the oracle key.
func generateOracleKey(rand io.Reader, oracleId string, id string, line line, pass []byte, opts oracleKeyOptions) (oracleKey, error) {
	pt, err := generatePoint(rand, line, opts.KeySize)
	if err != nil {
		return oracleKey{}, errors.Wrapf(err, "Error generating point of strength [%v]", opts.KeySize)
	}

	defer pt.Destroy()

	salt, err := generateRandomBytes(rand, opts.KeySize)
	if err != nil {
		return oracleKey{}, errors.Wrapf(err, "Error generating salt of strength [%v]", opts.KeySize)
	}

	encPt, err := encryptPoint(rand, pt, opts.Cipher,
		crypoBytes(pass).Pbkdf2(salt, opts.KeyIter, opts.KeySize, opts.KeyHash.Standard()))
	if err != nil {
		return oracleKey{}, errors.WithMessage(err, "Error generating oracle key")
	}

	return oracleKey{oracleId, id, encPt, opts.KeyHash, salt, opts.KeyIter}, nil
}

// An oracle is used to protect a secret that may be shared amongst many actors.
//
// An oracle protects the secret by creating a curve (in this case a line) and
// publishing a point on the curve.  Only entities which can provide the other
// points required to rederive the curve can unlock an oracle.   To provide an
// an "unlocking" mechanism, there is also an oracle key which contains a
// point on the line that has been encrypted with a symmetric cipher key.  The
// key can be anything, from a password to another key or even another oracle's
// seed value.
//
// The purpose of the oracle is to act as an encryption seeding algorithm.
// The seed must be paired with other knowledge to be useful, meaning the risk
// of a leaked shared oracle is minimal.  Even though this is a critical
// component, it can't act alone.
//
type oracle struct {

	// The id of the oracle.
	Id string

	// The public point on the secret curve
	pt point

	// The seed key derivation options
	derivSize int
	derivHash Hash
	derivSalt []byte
	derivIter int
}

// Decrypts the oracle, returning a hash of the underlying secret.
func (p oracle) Unlock(key oracleKey, pass []byte) ([]byte, line, error) {
	ymxb, err := p.DeriveLine(key, pass)
	if err != nil {
		return nil, line{}, errors.WithStack(err)
	}
	return crypoBytes(ymxb.Bytes()).Pbkdf2(
		p.derivSalt, p.derivIter, p.derivSize, p.derivHash.Standard()), ymxb, nil
}

// Decrypts the oracle, returning a hash of the underlying secret.
func (p oracle) DeriveLine(key oracleKey, pass []byte) (line, error) {
	pt, err := key.access(pass)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	defer pt.Destroy()

	ymxb, err := pt.Derive(p.pt)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	return ymxb, nil
}

// An oracle key is required to unlock an oracle.  It contains a point on
// the corresponding oracle's secret line.
type oracleKey struct {
	OracleId string
	Id       string

	// the secure point.  (Must be paired with another point to be useful)
	pt securePoint

	// the secure point encryption key options
	keyHash Hash
	keySalt []byte
	keyIter int
}

func (p oracleKey) access(pass []byte) (point, error) {
	pt, err := p.pt.Decrypt(
		crypoBytes(pass).Pbkdf2(p.keySalt, p.keyIter, p.pt.Cipher.KeySize(), p.keyHash.Standard()))
	if err != nil {
		return point{}, errors.WithStack(err)
	}
	return pt, nil
}
