package secret

import (
	"io"

	"github.com/pkg/errors"
)

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
type SecretAlgorithm int

const (
	ShamirAlpha SecretAlgorithm = iota
)

func (s SecretAlgorithm) Gen(rand io.Reader, opts SecretOptions) (Secret, error) {
	switch s {
	default:
		return nil, errors.Wrapf(ErrUnkownSecretAlgorithm, "Unknown sharding algorithm [%v]", s)
	case ShamirAlpha:
		return generateShamirSecret(rand, opts)
	}
}
