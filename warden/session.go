package warden

import (
	"io"

	"github.com/pkg/errors"
)

// A session represents an authenticated session with the trust ecosystem.  Sessions
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the session in memory, without
// fear of leaking any critical details.
type Session struct {

	// the encrypted subscription key
	sub SigningKey

	// internal only transport.
	net Transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the auth token
	auth token

	// the hased master key.  This isn't actually usable on its own.  It
	// must be paired with every piece of data that this user attempts to
	// decrypt.
	master []byte
}

// Returns the id of my account.  This may be shared through unsecure channels.
func (s *Session) WhoAmI() string {
	return s.sub.Pub.Id()
}

func (s *Session) VerifyKey() PublicKey {
	return s.sub.Pub
}

func (s *Session) signingKey() (PrivateKey, error) {
	return s.sub.Decrypt(s.master)
}

func (s *Session) domainMasterKey(cancel <-chan struct{}, domain string) ([]byte, error) {
	encrypted, err := s.net.LoadDomainMasterKey(cancel, s.auth, domain)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	key, err := encrypted.Decrypt(s.master)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

func (s *Session) domainSigningKey(cancel <-chan struct{}, domain string) (PrivateKey, error) {
	encrypted, err := s.net.LoadDomainSigningKey(cancel, s.auth, domain)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	key, err := encrypted.Decrypt(s.master)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

func (s *Session) domainVerifyKey(cancel <-chan struct{}, domain string) (PublicKey, error) {
	return s.net.LoadDomainVerifyKey(cancel, s.auth, domain)
}

// Returns the master key.
type MasterKey struct {
	PtPub  point
	PtPriv securePoint
	Key    CipherText

	// point args
	PtHash Hash
	PtSalt []byte
	PtIter int

	// key args
	KeyHash Hash
	KeySalt []byte
	KeyIter int
}

func (p MasterKey) deriveLine(creds []byte) (line, error) {
	point, err := p.PtPriv.Decrypt(p.PtSalt, p.PtIter, p.PtHash.Standard(), creds)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	defer point.Destroy()

	ymxb, err := point.Derive(p.PtPub)
	if err != nil {
		return line{}, errors.WithStack(err)
	}
	return ymxb, nil
}

func (p MasterKey) Decrypt(creds []byte) ([]byte, error) {
	line, err := p.deriveLine(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer line.Destroy()

	return p.Key.Decrypt(
		Bytes(line.Bytes()).Pbkdf2(
			p.KeySalt, p.KeyIter, p.Key.Cipher.KeySize(), p.KeyHash.Standard()))
}

// A signing key is an encrypted private key.  It may only be decrypted
// by someone who has been trusted to sign on its behalf.
type SigningKey struct {
	Pub  PublicKey
	Key  CipherText
	Hash Hash
	Salt []byte
	Iter int
}

func (p SigningKey) Decrypt(master []byte) (PrivateKey, error) {
	raw, err := p.Key.Decrypt(
		Bytes(master).Pbkdf2(
			p.Salt, p.Iter, p.Key.Cipher.KeySize(), p.Hash.Standard()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer raw.Destroy()

	priv, err := p.Pub.Algorithm().ParsePrivateKey(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return priv, nil
}
