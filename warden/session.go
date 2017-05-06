package warden

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

// A session represents an authenticated session with the trust ecosystem.  Sessions
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the session in memory, without
// fear of leaking any critical details.
type Session struct {

	// the subscriber
	sub Subscriber

	// the transport mechanism. (expected to be secure).
	net transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the subcriber's oracle.  safe to store in memory.
	oracle []byte
}

// Returns an authentication token.
func (s *Session) auth(cancel <-chan struct{}) (auth, error) {
	return auth{}, nil
}

// Sends
func (s *Session) rpc(cancel <-chan struct{}, fn func(auth) error) error {
	return nil
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

// Destroys the session's memory - zeroing out any sensitive info
func (s *Session) Destroy() {
	cryptoBytes(s.oracle).Destroy()
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order to share with other users.
func (s *Session) MyId() uuid.UUID {
	return s.sub.Id
}

// Returns the session owner's public key.  This key and its id may be shared freely.
func (s *Session) MyKey() PublicKey {
	return s.sub.Sign.Pub
}

// Returns the signing key associated with this session. Should be promptly destroyed.
func (s *Session) mySigningKey() (PrivateKey, error) {
	return s.sub.Sign.Decrypt(s.oracle)
}

// Returns the invitation key associated with this session. Should be promptly destroyed.
func (s *Session) myInviteKey() (PrivateKey, error) {
	return s.sub.Invite.Decrypt(s.oracle)
}

// Lists the session owner's currently pending invitations.
func (s *Session) MyInvitations(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Invitation, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.Invites.BySubscriber(cancel, auth, s.MyId(), opts.Beg, opts.End)
}

// Lists the session owner's currently active certificates.
func (s *Session) MyCertificates(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Certificate, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.Certs.ActiveBySubscriber(cancel, auth, s.MyId(), opts.Beg, opts.End)
}

// Lists the trusts that were created by the session owner.
func (s *Session) MyTrusts(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Trust, error) {
	panic("Not yet implemented")
}

// Creates a new trust with the given alias.  An alias is a non-unique alternative lookup name.
func (s *Session) NewTrust(cancel <-chan struct{}, alias string, fns ...func(s *TrustOptions)) (Trust, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	dom, err := generateTrust(s, alias, fns...)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	if err := s.net.Trusts.Register(cancel, auth, dom); err != nil {
		return Trust{}, errors.WithStack(err)
	}

	return dom, nil
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) LoadTrustById(cancel <-chan struct{}, id uuid.UUID) (Trust, bool, error) {
	auth, err := s.auth(cancel)
	if err != nil {
		return Trust{}, false, errors.WithStack(err)
	}

	return s.net.Trusts.ById(cancel, auth, id)
}

// Loads the trust with the given signing key.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) LoadTrustByKey(cancel <-chan struct{}, key string) (Trust, bool, error) {
	return Trust{}, false, nil
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *Session) Accept(cancel <-chan struct{}, id uuid.UUID) error {
	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	inv, ok, err := s.net.Invites.ById(cancel, auth, id)
	if err != nil || !ok {
		return errors.WithStack(common.Or(err, InvariantError))
	}

	dom, ok, err := s.net.Trusts.ById(cancel, auth, inv.Cert.Trust)
	if err != nil || !ok {
		return errors.WithStack(err)
	}

	priv, err := s.mySigningKey()
	if err != nil {
		return errors.WithStack(err)
	}

	key, err := inv.accept(s.rand, dom.oracle.Oracle, priv, s.myOracle(), dom.oracle.Opts)
	if err != nil {
		return errors.WithStack(err)
	}

	sig, err := inv.Cert.Sign(s.rand, priv, dom.oracle.Opts.SigHash)
	if err != nil {
		return errors.WithStack(err)
	}

	return s.net.Certs.Register(cancel, auth, inv.Cert, key, inv.TrustSig, inv.IssuerSig, sig)
}

// // Determines the authenticity of the provided invitation.
// func (s *Session) VerifyInvitation(cancel <-chan struct{}, i Invitation) error {
// auth, err := s.auth(cancel)
// if err != nil {
// return errors.WithStack(err)
// }
//
// trustKey, err := s.net.Keys.ByTrust(cancel, auth, i.Cert.Trust)
// if err != nil {
// return errors.WithStack(err)
// }
//
// issuerKey, err := s.net.Keys.BySubscriber(cancel, auth, i.Cert.Issuer)
// if err != nil {
// return errors.WithStack(err)
// }
//
// return i.verify(trustKey, issuerKey)
// }

// Revokes trust from the given subscriber.
func (s *Session) Revoke(cancel <-chan struct{}, t Trust, sub uuid.UUID) error {
	return nil
}

// Renews the certificate associated with the given trust, returning the
// updated trust.
func (s *Session) Renew(cancel <-chan struct{}, t Trust) (Trust, error) {
	return Trust{}, nil
}
