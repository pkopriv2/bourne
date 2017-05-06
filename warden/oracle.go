package warden

import (
	"io"

	"github.com/pkg/errors"
)

// FIXME: Move to Blakley's secret sharing method (intersecting vectorspaces)

// Options for generating an oracle.  These options will determine the
// strength of the oracle as well as the strength of any accessors to the
// oracle.
type OracleOptions struct {

	// the strength of the curve is ~ size(slope) + size(y-intercept)
	CurveStrength int

	// sharing options
	ShareCipher SymmetricCipher
	ShareHash   Hash
	ShareIter   int // used for key derivations only

	// signature options
	SigHash Hash
}

func defaultOracleOptions() OracleOptions {
	return OracleOptions{1024, Aes256Gcm, SHA256, 1024, SHA256}
}

func buildOracleOptions(fns ...func(*OracleOptions)) OracleOptions {
	ret := defaultOracleOptions()
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A signed oracle.  (Used to prove legitimacy of an oracle)
type SignedOracle struct {
	Oracle
	Sig Signature
}

// Verifies the signed oracle
func (s SignedOracle) Verify(key PublicKey) error {
	fmt, err := s.Format()
	if err != nil {
		return err
	}

	return s.Sig.Verify(key, fmt)
}

// A signed oracle key.  (Used to prove legitimacy of raw key)
type SignedOracleKey struct {
	OracleKey
	Sig Signature
}

// Verifies the signed oracle key
func (s SignedOracleKey) Verify(key PublicKey) error {
	fmt, err := s.Format()
	if err != nil {
		return err
	}

	return s.Sig.Verify(key, fmt)
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
type Oracle struct {

	// The public point on the secret curve.
	Pt point

	// key options.  In order to share in the oracle, must agree to these
	Opts OracleOptions
}

// Generates a new random oracle + the curve that generated the oracle.  The returned curve
// may be used to generate oracle keys.
func genOracle(rand io.Reader, opts OracleOptions) (Oracle, line, error) {
	size := opts.ShareCipher.KeySize()

	ret, err := generateLine(rand, size)
	if err != nil {
		return Oracle{}, line{}, errors.Wrapf(
			err, "Error generating curve of strength [%v]", size)
	}

	pub, err := generatePoint(rand, ret, size)
	if err != nil {
		return Oracle{}, line{}, errors.Wrapf(
			err, "Error generating point of strength [%v]", size)
	}

	return Oracle{pub, opts}, ret, nil
}

// Decrypts the oracle, returning a hash of the underlying secret.
func (p Oracle) Unlock(key OracleKey, pass []byte) (line, error) {
	pt, err := key.Extract(pass)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	defer pt.Destroy()

	ymxb, err := pt.Derive(p.Pt)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	return ymxb, nil
}

func (p Oracle) Sign(rand io.Reader, priv Signer, hash Hash) (SignedOracle, error) {
	fmt, err := p.Format()
	if err != nil {
		return SignedOracle{}, err
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return SignedOracle{}, err
	}

	return SignedOracle{p, sig}, nil
}

func (p Oracle) Format() ([]byte, error) {
	return gobBytes(p)
}

// An oracle key is required to unlock an oracle.  It contains a point on
// the corresponding oracle's secret line.
type OracleKey struct {
	// the secure point.  (Must be paired with another point to be useful)
	Pt securePoint

	// encryption key derivation arguments point
	KeyHash Hash
	KeySalt []byte
	KeyIter int
}

// Generates a new key for the oracle whose secret was derived from line.  Only the given pass
// may unlock the oracle key.
func genOracleKey(rand io.Reader, line line, pass []byte, opts OracleOptions) (OracleKey, error) {
	size := opts.ShareCipher.KeySize()

	pt, err := generatePoint(rand, line, size)
	if err != nil {
		return OracleKey{}, errors.Wrapf(
			err, "Error generating point of strength [%v]", size)
	}

	defer pt.Destroy()

	salt, err := generateRandomBytes(rand, size)
	if err != nil {
		return OracleKey{}, errors.Wrapf(
			err, "Error generating salt of strength [%v]", size)
	}

	enc, err := encryptPoint(rand, pt, opts.ShareCipher,
		cryptoBytes(pass).Pbkdf2(salt, opts.ShareIter, size, opts.ShareHash.standard()))
	if err != nil {
		return OracleKey{}, errors.WithMessage(
			err, "Error generating oracle key")
	}

	return OracleKey{enc, opts.ShareHash, salt, opts.ShareIter}, nil
}

func (p OracleKey) Sign(rand io.Reader, priv Signer, hash Hash) (SignedOracleKey, error) {
	fmt, err := p.Format()
	if err != nil {
		return SignedOracleKey{}, err
	}

	sig, err := priv.Sign(rand, hash, fmt)
	if err != nil {
		return SignedOracleKey{}, err
	}

	return SignedOracleKey{p, sig}, nil
}

func (p OracleKey) Format() ([]byte, error) {
	return gobBytes(p)
}

func (p OracleKey) Extract(pass []byte) (point, error) {
	return p.Pt.Decrypt(
		cryptoBytes(pass).Pbkdf2(p.KeySalt, p.KeyIter, p.Pt.Cipher.KeySize(), p.KeyHash.standard()))
}
