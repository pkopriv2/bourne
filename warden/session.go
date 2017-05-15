package warden

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

var (
	UnsupportedLoginError = errors.New("Warden:UnsupportedLogin")
)

// A session represents an authenticated session with the trust ecosystem.  Sessions
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the session in memory, without
// fear of leaking any critical details.
type Session struct {
	//
	ctx common.Context

	// the login credentials
	login func(KeyPad) error

	// the session's subscriber info
	sub Subscriber

	// the session's encrypted shard.
	loginShard signedEncryptedShard

	// the transport mechanism. (expected to be secure).
	net transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the token pool.
	tokens chan Token
}

// Returns an authentication token.
func (s *Session) Close() error {
	return nil
}

// Returns an authentication token.
func (s *Session) auth(cancel <-chan struct{}) (Token, error) {
	return Token{}, nil
}

// Returns the secret by signature method.
func (s *Session) mySecretBySignature(signer Signer) (Secret, error) {
	return nil, nil
	// key, err := signer.Sign(s.rand, s.sub.SigningKey.Opts.Hash, s.sub.Nonce)
	// if err != nil {
		// return nil, errors.WithStack(err)
	// }
	// defer destroyBytes(key.Data)
//
	// shard, err := s.loginShard.Decrypt(key.Data)
	// if err != nil {
		// return nil, errors.WithStack(err)
	// }
	// defer shard.Destroy()
//
	// secret, err := shard.Derive(s.sub.Pub)
	// return secret, errors.WithStack(err)
}

// Returns the session owner's secret.  This should be destroyed promptly after use.
func (s *Session) mySecret() (Secret, error) {
	return nil, nil
	// creds, err := enterCreds(s.login)
	// if err != nil {
		// return nil, errors.WithStack(err)
	// }
//
	// if creds.Signer != nil {
		// return s.mySecretBySignature(creds.Signer)
	// }
//
	// return nil, errors.Wrapf(UnsupportedLoginError, "Must use one of [WithSignature]")
}

// Returns the personal encryption seed of this subscription.
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
func (s *Session) myEncryptionSeed(secret Secret) ([]byte, error) {
	ret, err := secret.Hash(SHA256)
	return ret, errors.WithStack(err)
}

// Returns the signing key associated with this session. Should be promptly destroyed.
func (s *Session) mySigningKey(secret Secret) (PrivateKey, error) {
	seed, err := s.myEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	key, err := s.sub.SigningKey.Decrypt(seed)
	return key, errors.WithStack(err)
}

// Returns the invitation key associated with this session. Should be promptly destroyed.
func (s *Session) myInviteKey(secret Secret) (PrivateKey, error) {
	seed, err := s.myEncryptionSeed(secret)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	key, err := s.sub.InviteKey.Decrypt(seed)
	return key, errors.WithStack(err)
}

// Destroys the session's memory - zeroing out any sensitive info
func (s *Session) Destroy() {
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order to share with other users.
func (s *Session) MyId() uuid.UUID {
	return s.sub.Id
}

// Returns the session owner's public signing key.  This key and its id may be shared freely.
func (s *Session) MyKey() PublicKey {
	return s.sub.SigningKey.Pub
}

// Lists the session owner's currently pending invitations.
func (s *Session) MyInvitations(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Invitation, error) {
	invites, err := s.net.Invites.InvitationsBySubscriber(cancel, s.auth, s.MyId(), buildPagingOptions(fns...))
	return invites, errors.WithStack(err)
}

// Lists the session owner's currently active certificates.
func (s *Session) MyCertificates(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Certificate, error) {
	certs, err := s.net.Certs.ActiveBySubscriber(cancel, s.auth, s.MyId(), buildPagingOptions(fns...))
	return certs, errors.WithStack(err)
}

// Keys that have been in some way trusted by the owner of the session.
func (s *Session) MyTrusts(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Trust, error) {
	opts := buildPagingOptions(fns...)
	return s.net.Trusts.BySubscriber(cancel, s.auth, s.MyId(), opts.Beg, opts.End)
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) TrustById(cancel <-chan struct{}, id uuid.UUID) (Trust, bool, error) {
	trust, ok, err := s.net.Trusts.ById(cancel, s.auth, id)
	return trust, ok, errors.WithStack(err)
}

// Loads the trust with the given signing key.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) TrustByKey(cancel <-chan struct{}, key string) (Trust, bool, error) {
	return Trust{}, false, nil
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) InviteById(cancel <-chan struct{}, id uuid.UUID) (Invitation, bool, error) {
	trust, ok, err := s.net.Invites.InvitationById(cancel, s.auth, id)
	return trust, ok, errors.WithStack(err)
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *Session) AcceptTrust(cancel <-chan struct{}, i Invitation) error {
	err := i.acceptInvitation(cancel, s)
	return errors.WithStack(err)
}

// Revokes trust from the given subscriber for the given trust.
func (s *Session) RevokeTrust(cancel <-chan struct{}, t Trust, sub uuid.UUID) error {
	err := t.revokeCertificate(cancel, s, sub)
	return errors.WithStack(err)
}

// Renew's the session owner's certificate with the trust.
func (s *Session) RenewTrust(cancel <-chan struct{}, t Trust) (Trust, error) {
	trust, err := t.renewCertificate(cancel, s)
	return trust, errors.WithStack(err)
}
