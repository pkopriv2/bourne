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

	// the encrypted subscription key.
	sub SigningKey

	// the transport mechanism. (expected to be secure).
	net Transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the subcriber's oracle/seed.
	oracle []byte
}

func (s *Session) auth(cancel <-chan struct{}) (token, error) {
	return token{}, nil
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order to share with other users.
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

// Returns the personal encryption seed of this subscription.  The seed is actually
// safe to store in memory over an extended period of time.  This is because the
// seed isn't actually useful on it's own.  It must be paired with elements of the
// data that is being accessed in order to be useful.
//
// In order for a attacker to penetrate further, he would need both the seed and a valid
// token. The token would give him temporary access to the subscription, but
// once the token expired, he could not login again.  Higher security environments
// could tune down the ttl of a session token to limit exposure.
//
// It should be noted that even in the event of a compromised seed + token,
// the system itself is never in danger because the risk has been spread over the
// community.  The leak extends as far as the trust extends.  No other users are at risk
// because of a leaked seed or token.
func (s *Session) myOracle() []byte {
	return s.oracle
}

// A signing key is an encrypted private key.  It may only be decrypted
// by someone who has been trusted to sign on its behalf.
type SigningKey struct {
	Pub    PublicKey
	Priv   CipherText
	fnHash Hash
	fnSalt []byte
	fnIter int
}

func (p SigningKey) Decrypt(seed []byte) (PrivateKey, error) {
	raw, err := p.Priv.Decrypt(
		Bytes(seed).Pbkdf2(
			p.fnSalt, p.fnIter, p.Priv.Cipher.KeySize(), p.fnHash.Standard()))
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
