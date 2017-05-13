package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
)

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)

type KeyPad struct {
	ctx  common.Context
	rand io.Reader
	net  transport
}

func (k KeyPad) WithSignature(signer Signer, timeout time.Duration) (signedAuth, error) {
	ctrl := k.ctx.Timer(timeout)
	defer ctrl.Close()

	auth := newAuthChallenge()

	sig, err := sign(k.rand, auth, signer, SHA256)
	if err != nil {
		return signedAuth{}, errors.WithStack(err)
	}

	ret, err := k.net.Auth.BySignature(ctrl.Closed(), signer.Public().Id(), auth, sig)
	return ret, errors.WithStack(err)
}
