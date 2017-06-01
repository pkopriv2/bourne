package warden

import (
	"time"

	"github.com/pkg/errors"
)

// Signing authenticator arguments.  These are stored on the
type signArgs struct {
	Pub  PublicKey // only used for creating the auth initially.
	Now  time.Time
	Sig  Signature
	Skew time.Duration
}

func parseSignArgs(raw []byte) (s signArgs, e error) {
	_, e = parseGobBytes(raw, &s)
	return
}

type signAuth struct {
	Ver authProtocol
	Pub PublicKey
}

func newSignAuth(raw []byte) (signAuth, error) {
	args, err := parseSignArgs(raw)
	if err != nil {
		return signAuth{}, errors.WithStack(err)
	}

	return signAuth{SignV1, args.Pub}, nil
}

func (s *signAuth) Protocol() authProtocol {
	return s.Ver
}

// Expects input to be: signArgs
func (s *signAuth) Authenticate(raw []byte) error {
	args, err := parseSignArgs(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	if args.Sig.Key != s.Pub.Id() {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature does not match auth key")
	}

	if args.Skew > 30*time.Minute {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature tolerance too high.")
	}

	if time.Now().Sub(args.Now) > args.Skew {
		return errors.Wrapf(UnauthorizedError, "Unauthorized.  Signature too old.")
	}

	msg, err := gobBytes(args.Now)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(args.Sig.Verify(s.Pub, msg))
}
