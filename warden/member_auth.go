package warden

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
)

// MemberAuth is the server generated component of the authentication
// protovcol.  Every auth will have a
type memberAuth struct {
	MemberId uuid.UUID
	Shard    memberShard
	AuthImpl []byte // implementation of member auth.  (type specified by Shard.authProtocol)
}

func (a memberAuth) Id() []byte {
	return stash.UUID(a.MemberId).Child(a.Shard.Id)
}

func newMemberAuth(rand io.Reader, id uuid.UUID, shard memberShard, args []byte) (memberAuth, error) {
	var impl interface{}
	var err error
	switch shard.Proto {
	default:
		return memberAuth{}, errors.Wrapf(AuthError, "Unsupported protocol [%v]", shard.Proto)
	case PassV1:
		impl, err = newPassAuth(rand, 32, 1024, SHA256, args)
		if err != nil {
			return memberAuth{}, errors.WithStack(err)
		}
	case SignV1:
		impl, err = newSignAuth(args)
		if err != nil {
			return memberAuth{}, errors.WithStack(err)
		}
	}

	raw, err := gobBytes(impl)
	if err != nil {
		return memberAuth{}, errors.WithStack(err)
	}
	return memberAuth{id, shard, raw}, nil
}

func (m memberAuth) authenticate(args []byte) error {
	var impl authenticator
	switch m.Shard.Proto {
	default:
		return errors.Wrapf(AuthError, "Unsupported auth protocol [%v]", m.Shard.Proto)
	case PassV1:
		var auth passAuth
		if _, err := parseGobBytes(m.AuthImpl, &auth); err != nil {
			return errors.WithStack(err)
		}
		impl = &auth
	case SignV1:
		var auth signAuth
		if _, err := parseGobBytes(m.AuthImpl, &auth); err != nil {
			return errors.WithStack(err)
		}
		impl = &auth
		return nil
	}
	return errors.WithStack(impl.Authenticate(args))
}
