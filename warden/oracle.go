package warden

import (
	"io"

	"github.com/pkg/errors"
)

// Options for generating an oracle.  These options will determine the
// strength of the oracle as well as the strength of any accessors to the
// oracle.
type oracleOptions struct {
	// sharing options
	ShareCipher SymmetricCipher
	ShareHash   Hash
	ShareIter   int // used for key derivations only

	// signature options
	SigHash Hash
}

func buildOracleOptions(fns ...func(*oracleOptions)) oracleOptions {
	ret := oracleOptions{AES_256_GCM, SHA256, 1024, SHA256}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func generateOracle(rand io.Reader, id string, fns ...func(*oracleOptions)) (oracle, line, error) {
	opts := buildOracleOptions(fns...)

	size := opts.ShareCipher.KeySize()

	ret, err := generateLine(rand, size)
	if err != nil {
		return oracle{}, line{}, errors.Wrapf(err, "Error generating curve of strength [%v]", size)
	}

	pub, err := generatePoint(rand, ret, size)
	if err != nil {
		return oracle{}, line{}, errors.Wrapf(err, "Error generating point of strength [%v]", size)
	}

	return oracle{id, pub, opts}, ret, nil
}

// Generates a new key for the oracle whose secret was derived from line.  Only the given pass
// may unlock the oracle key.
func generateOracleKey(rand io.Reader, oid string, id string, line line, pass []byte, opts oracleOptions) (oracleKey, error) {
	size := opts.ShareCipher.KeySize()

	pt, err := generatePoint(rand, line, size)
	if err != nil {
		return oracleKey{}, errors.Wrapf(err, "Error generating point of strength [%v]", size)
	}

	defer pt.Destroy()

	salt, err := generateRandomBytes(rand, size)
	if err != nil {
		return oracleKey{}, errors.Wrapf(err, "Error generating salt of strength [%v]", size)
	}

	enc, err := encryptPoint(rand, pt, opts.ShareCipher,
		Bytes(pass).Pbkdf2(salt, opts.ShareIter, size, opts.ShareHash.Standard()))
	if err != nil {
		return oracleKey{}, errors.WithMessage(err, "Error generating oracle key")
	}

	return oracleKey{oid, id, enc, opts.ShareHash, salt, opts.ShareIter}, nil
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

	// key options.  In order to share in the oracle, must agree to these
	opts oracleOptions
}

// Decrypts the oracle, returning a hash of the underlying secret.
func (p oracle) DeriveLine(key oracleKey, pass []byte) (line, error) {
	if key.oid != p.Id {
		return line{}, errors.Wrapf(TrustError, "Cannot use that key [%v] to open oracle [%v]", key.Id, p.Id)
	}

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
	oid string
	Id  string

	// the secure point.  (Must be paired with another point to be useful)
	pt securePoint

	// the secure point encryption key options
	keyHash Hash
	keySalt []byte
	keyIter int
}

func (p oracleKey) access(pass []byte) (point, error) {
	pt, err := p.pt.Decrypt(
		Bytes(pass).Pbkdf2(p.keySalt, p.keyIter, p.pt.Cipher.KeySize(), p.keyHash.Standard()))
	if err != nil {
		return point{}, errors.WithStack(err)
	}
	return pt, nil
}
