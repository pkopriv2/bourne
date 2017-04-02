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
	sub signingKey

	// internal only transport.
	net Transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the auth token
	auth token

	// the hashed oracle key.  This isn't actually usable on its own.  It
	// must be paired with every piece of data that this user attempts to
	// decrypt.
	oracle []byte
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order
func (s *Session) MyId() string {
	return s.sub.Pub.Id()
}

// Returns the subscriber key of this session.
func (s *Session) MyKey() PublicKey {
	return s.sub.Pub
}

// Returns the signing key associated with this session.
func (s *Session) mySigningKey() (PrivateKey, error) {
	return s.sub.Decrypt(s.oracle)
}

// Returns the personal oracle of this subscription.  The personal oracle is actually
// safe to store in memory over an extended period of time.  This is because the
// oracle isn't actually useful on it's own.  It must be paired with elements of the
// data that is being accessed in order to be useful.
//
// In order for a attacker to penetrate further, he would need both the oracle and a valid
// token. The token would give him temporary access to your subscription, but
// once the token expired, he could not login again.  Higher security environments
// could tune down the ttl of a session token to limit exposure.
//
// It should be noted that even in the event of a compromised oracle + token,
// the system itself is never in danger because the risk has been spread over the
// community.  The leak extends as far as the trust extends.  No other users are at risk
// because of a leaked oracle or token.  And this is the worst case scenario.
func (s *Session) myOracle() ([]byte, error) {
	return s.oracle, nil
}

// Returns the domain oracle.
func (s *Session) domainOracle(cancel <-chan struct{}, domainId string) ([]byte, error) {
	encrypted, err := s.net.LoadDomainOracle(cancel, s.auth, domainId)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	key, err := encrypted.Decrypt(s.oracle)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

// Returns the domain signing key.
func (s *Session) domainSigningKey(cancel <-chan struct{}, domainId string) (PrivateKey, error) {
	encrypted, err := s.net.LoadDomainSigningKey(cancel, s.auth, domainId)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	key, err := encrypted.Decrypt(s.oracle)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return key, nil
}

// Returns the domain's public key
func (s *Session) domainVerifyKey(cancel <-chan struct{}, domainId string) (PublicKey, error) {
	return s.net.LoadDomainVerifyKey(cancel, s.auth, domainId)
}

// A oracle key/oracle is used to seed other encryption algorithms.  This implements the
// core sharing/derivation algorithm.  An oracle is an immutable object taht is personal
// to each resource that created it.  In the case of a user, it is their private account
// oracle. In the case of a domain, it is the domain oracle.  The domain oracle consists
// of the following components:
//
// 1.) A public point on a curve in 2-dimensional space. (all user's see this)
// 2.) A point that has been encrypted with a hashed form of the the user's key.
//     (credentials in the case of a subscription's oracle)
// 3.) A key that has been encrypted with the line that passes through the 1. and 2.
//
// While the trust service hosts all the critical data, the resource supplies the knowledge
// necessary to decrypt the oracle key.   A decrypted oracle key should NEVER be transmitted
// but may be stored in memory longer term as long as the data that it encrypts
// have separate access channels.
//
type oracle struct {
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

func (p oracle) DeriveLine(creds []byte) (line, error) {
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

func (p oracle) Decrypt(creds []byte) ([]byte, error) {
	line, err := p.DeriveLine(creds)
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
type signingKey struct {
	Pub  PublicKey
	Key  CipherText
	Hash Hash
	Salt []byte
	Iter int
}

func (p signingKey) Decrypt(oracle []byte) (PrivateKey, error) {
	raw, err := p.Key.Decrypt(
		Bytes(oracle).Pbkdf2(
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
