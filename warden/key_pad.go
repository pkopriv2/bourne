package warden

import (
	"time"

	"github.com/pkg/errors"
)

// A key pad gives access to the various authentication methods and will
// be used during the registration process.
//
// Future: Accept alternative login methods (e.g. pins, passwords, multi-factor, etc...)

type SignatureOptions struct {
	Hash Hash
	Skew time.Duration
}

func buildSignatureOptions(fns ...func(*SignatureOptions)) SignatureOptions {
	ret := SignatureOptions{SHA256, 5 * time.Minute}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

type KeyPad interface {
	BySignature(signer Signer, opts ... func(*SignatureOptions)) error
	ByPassword(user string, pass string) error
}

// A one time keypad.  Destroyed after first use.
type oneTimePad struct {
	Creds credential
}

func (c *oneTimePad) BySignature(s Signer, fns...func(*SignatureOptions)) error {
	opts := buildSignatureOptions(fns...)
	c.Creds = &signCred{s, opts.Hash, opts.Skew}
	return nil
}

func (c *oneTimePad) ByPassword(user string, pass string) error {
	preHash, err := cryptoBytes([]byte(pass)).Hash(SHA256)
	if err != nil {
		return errors.Wrapf(err, "Error pre-hashing user password [%v]", string(user))
	}

	c.Creds = &passCred{[]byte(user), preHash}
	return nil
}

func extractCreds(fn func(KeyPad) error) (credential, error) {
	pad := &oneTimePad{}
	if err := fn(pad); err != nil {
		return nil, errors.WithStack(err)
	}

	if pad.Creds == nil {
		return nil, errors.Wrap(AuthError, "No credentials entered.")
	}

	return pad.Creds, nil
}
