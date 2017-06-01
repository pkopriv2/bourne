package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

type SessionOptions struct {
	Timeout   time.Duration
	TokenTtl  time.Duration
	TokenHash Hash
	deps      *Dependencies
}

func buildSessionOptions(fns ...func(*SessionOptions)) SessionOptions {
	ret := SessionOptions{30 * time.Second, 30 * time.Second, SHA256, nil}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A session represents an authenticated session with the trust ecosystem.  Sessions
// contain a signed message from the trust service - plus a hashed form of the
// authentication credentials.  The hash is NOT enough to rederive any secrets
// on its own - therefore it is safe to maintain the session in memory, without
// fear of leaking any critical details.
type session struct {

	// the core context
	ctx common.Context

	// the core logger
	logger common.Logger

	// the login credentials closure.  Destroyed immediately after use.
	login func() credential

	// the session's owners membership info=
	core memberCore

	// the access shard used for this session
	shard memberShard

	// the transport mechanism. (expected to be secure).
	net Transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the token pool.
	tokens chan SignedToken

	// session options
	opts SessionOptions
}

func newSession(ctx common.Context, m memberCore, a memberShard, t SignedToken, l func() credential, s SessionOptions, d Dependencies) (*session, error) {
	var err error

	ctx = ctx.Sub("Session(memberId=%v)", m.Id.String()[:8])
	defer func() {
		if err != nil {
			ctx.Close()
		}
	}()

	ctx.Control().Defer(func(error) {
		d.Net.Close()
	})

	ret := &session{
		ctx:    ctx,
		logger: ctx.Logger(),
		login:  l,
		core:   m,
		shard:  a,
		net:    d.Net,
		rand:   d.Rand,
		tokens: make(chan SignedToken),
		opts:   s,
	}
	ret.start(t)
	return ret, nil
}

// Closes the session
func (s *session) Close() error {
	return s.ctx.Close()
}

// Returns a signed authentication token.
func (s *session) token(cancel <-chan struct{}) (SignedToken, error) {
	select {
	case <-s.ctx.Control().Closed():
		return SignedToken{}, errors.WithStack(common.ClosedError)
	case <-cancel:
		return SignedToken{}, errors.WithStack(common.CanceledError)
	case t := <-s.tokens:
		return t, nil
	}
}

// Returns a *new* signed authentication token.  This forces a call to the server.
func (s *session) auth(cancel <-chan struct{}) (SignedToken, error) {
	creds := s.login()
	defer creds.Destroy()

	auth, err := creds.Auth(s.rand)
	if err != nil {
		return SignedToken{}, errors.WithStack(err)
	}

	token, err := s.net.Authenticate(cancel, creds.MemberLookup(), creds.AuthId(), auth, s.opts.TokenTtl)
	return token, errors.WithStack(err)
}

// Starts the session.  Currently, this amounts to just renewing tokens.
func (s *session) start(t SignedToken) {
	token := &t
	go func() {
		timeout := 1 * time.Second
		for {
			if token == nil {
				s.logger.Info("Renewing session token [%v]", s.core.Id)

				t, err := s.auth(s.ctx.Control().Closed())
				if err != nil {
					if timeout < 64*time.Second {
						timeout *= 2
					}
					time.Sleep(timeout)
					continue
				}

				s.logger.Info("Successfully renewed token [%v]", t.Expires)
				token = &t
			}

			// FIXME: should be based on current token expiration!
			timer := time.NewTimer(s.opts.TokenTtl / 2)
			select {
			case <-timer.C:
				token = nil
			case <-s.ctx.Control().Closed():
				return
			case s.tokens <- *token:
			}
		}
	}()
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order to share with other users.
func (s *session) MyId() uuid.UUID {
	return s.core.Id
}

// Returns the session owner's public signing key.  This key and its id may be shared freely.
func (s *session) MyKey() PublicKey {
	return s.core.SigningKey.Pub
}

// Returns the session owner's secret.  This should be destroyed promptly after use.
func (s *session) mySecret() (Secret, error) {
	secret, err := s.core.secret(s.shard, s.login)
	return secret, errors.WithStack(err)
}

// Returns the personal encryption seed of this subscription.
func (s *session) myEncryptionSeed(secret Secret) ([]byte, error) {
	seed, err := s.core.encryptionSeed(secret)
	return seed, errors.WithStack(err)
}

// Returns the signing key associated with this session. Should be promptly destroyed.
func (s *session) mySigningKey(secret Secret) (PrivateKey, error) {
	key, err := s.core.signingKey(secret)
	return key, errors.WithStack(err)
}

// Returns the invitation key associated with this session. Should be promptly destroyed.
func (s *session) myInvitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.core.invitationKey(secret)
	return key, errors.WithStack(err)
}

// Lists the session owner's currently pending invitations.
func (s *session) MyInvitations(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Invitation, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	invites, err := s.net.InvitationsByMember(cancel, token, s.MyId(), buildPagingOptions(fns...))
	return invites, errors.WithStack(err)
}

// Lists the session owner's currently active certificates.
func (s *session) MyCertificates(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]SignedCertificate, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	certs, err := s.net.CertsByMember(cancel, token, s.MyId(), buildPagingOptions(fns...))
	return certs, errors.WithStack(err)
}

// Keys that have been in some way trusted by the owner of the session.
func (s *session) MyTrusts(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Trust, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	trusts, err := s.net.TrustsByMember(cancel, token, s.MyId(), buildPagingOptions(fns...))
	return trusts, errors.WithStack(err)
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *session) LoadTrust(cancel <-chan struct{}, id uuid.UUID) (Trust, bool, error) {
	token, err := s.token(cancel)
	if err != nil {
		return Trust{}, false, errors.WithStack(err)
	}

	trust, ok, err := s.net.TrustById(cancel, token, id)
	return trust, ok, errors.WithStack(err)
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *session) LoadInvitation(cancel <-chan struct{}, id uuid.UUID) (Invitation, bool, error) {
	token, err := s.token(cancel)
	if err != nil {
		return Invitation{}, false, errors.WithStack(err)
	}

	trust, ok, err := s.net.InvitationById(cancel, token, id)
	return trust, ok, errors.WithStack(err)
}

// Loads the certificates for the given trust.
func (s *session) LoadCertificatesByTrust(cancel <-chan struct{}, t Trust, fns ...func(*PagingOptions)) ([]SignedCertificate, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	certs, err := s.net.CertsByTrust(cancel, token, t.Id, buildPagingOptions(fns...))
	return certs, errors.WithStack(err)
}

// Loads the invitations for the given trust.
func (s *session) LoadInvitationsByTrust(cancel <-chan struct{}, t Trust, fns ...func(*PagingOptions)) ([]Invitation, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	invites, err := s.net.InvitationsByTrust(cancel, token, t.Id, buildPagingOptions(fns...))
	return invites, errors.WithStack(err)
}

// Generates a new trust.
func (s *session) NewTrust(cancel <-chan struct{}, strength Strength) (Trust, error) {
	token, err := s.token(cancel)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	mySecret, err := s.mySecret()
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	mySigningKey, err := s.mySigningKey(mySecret)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	trust, err := newTrust(s.rand, s.MyId(), mySecret, mySigningKey)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	if err := s.net.TrustRegister(cancel, token, trust); err != nil {
		return Trust{}, errors.WithStack(err)
	}

	return trust, nil
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *session) Invite(cancel <-chan struct{}, t Trust, memberId uuid.UUID, opts ...func(*InvitationOptions)) (Invitation, error) {
	inv, err := t.invite(cancel, s, memberId, opts...)
	return inv, errors.WithStack(err)
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *session) Accept(cancel <-chan struct{}, i Invitation) error {
	return errors.WithStack(i.accept(cancel, s))
}

// Revokes trust from the given subscriber for the given trust.
func (s *session) Revoke(cancel <-chan struct{}, t Trust, memberId uuid.UUID) error {
	return errors.WithStack(t.revokeCertificate(cancel, s, memberId))
}

// Renew's the session owner's certificate with the trust.  Only members with Member of
// greater trust level may reissue a certificate.
func (s *session) Renew(cancel <-chan struct{}, t Trust) (Trust, error) {
	trust, err := t.renewCertificate(cancel, s)
	return trust, errors.WithStack(err)
}

// Transfer will issue an invitation to the recipient with Grantor level trust and revoke
// the session owner's certificate
func (s *session) Transfer(cancel <-chan struct{}, t Trust, recipientId uuid.UUID) error {
	_, err := s.Invite(cancel, t, recipientId, func(o *InvitationOptions) {
		o.Lvl = Owner
		o.Exp = OneHundredYears
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(s.Revoke(cancel, t, s.MyId()))
}
