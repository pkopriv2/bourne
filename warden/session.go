package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
)

var (
	UnsupportedLoginError = errors.New("Warden:UnsupportedLogin")
)

type SessionOptions struct {
	NetTimeout      time.Duration
	TokenExpiration time.Duration
	TokenHash       Hash
}

func buildSessionOptions(fns ...func(*SessionOptions)) SessionOptions {
	ret := SessionOptions{30 * time.Second, 30 * time.Second, SHA256}
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
type Session struct {
	// the core context
	ctx common.Context

	// the core logger
	logger common.Logger

	// the login credentials
	login func(KeyPad) error

	// the session's subscriber info
	mem Member

	// the access shard used for this session
	code MemberCode

	// the transport mechanism. (expected to be secure).
	net Transport

	// the random source.  should be cryptographically strong.
	rand io.Reader

	// the token pool.
	tokens chan Token

	// session options
	opts SessionOptions
}

func newSession(ctx common.Context, m Member, a MemberCode, t Token, l func(KeyPad) error, s SessionOptions, d Dependencies) (*Session, error) {
	var err error

	ctx = ctx.Sub("Session: %v", m.Id.String()[:8])
	defer func() {
		if err != nil {
			ctx.Close()
		}
	}()

	session := &Session{
		ctx:    ctx,
		logger: ctx.Logger(),
		login:  l,
		mem:    m,
		code:   a,
		net:    d.Net,
		rand:   d.Rand,
		tokens: make(chan Token),
		opts:   s,
	}
	session.start(t)
	return session, nil
}

// Closes the session
func (s *Session) Close() error {
	return s.ctx.Close()
}

// Returns an authentication token.
func (s *Session) token(cancel <-chan struct{}) (Token, error) {
	select {
	case <-s.ctx.Control().Closed():
		return Token{}, errors.WithStack(common.ClosedError)
	case <-cancel:
		return Token{}, errors.WithStack(common.CanceledError)
	case t := <-s.tokens:
		return t, nil
	}
}

func (s *Session) auth(cancel <-chan struct{}) (Token, error) {
	creds, err := enterCreds(s.login)
	if err != nil {
		return Token{}, errors.WithStack(err)
	}

	token, err := auth(cancel, s.rand, s.net, creds, s.opts)
	return token, errors.WithStack(err)
}

func (s *Session) start(t Token) {
	token := &t
	go func() {
		timeout := 1 * time.Second
		for {
			if token == nil {
				s.logger.Info("Renewing session token [%v]", s.mem.Id)

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

			timer := time.NewTimer(s.opts.TokenExpiration / 2)
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

// Returns the session owner's secret.  This should be destroyed promptly after use.
func (s *Session) mySecret() (Secret, error) {
	secret, err := s.mem.secret(s.code, s.login)
	return secret, errors.WithStack(err)
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
	seed, err := s.mem.encryptionSeed(secret)
	return seed, errors.WithStack(err)
}

// Returns the signing key associated with this session. Should be promptly destroyed.
func (s *Session) mySigningKey(secret Secret) (PrivateKey, error) {
	key, err := s.mem.signingKey(secret)
	return key, errors.WithStack(err)
}

// Returns the invitation key associated with this session. Should be promptly destroyed.
func (s *Session) myInvitationKey(secret Secret) (PrivateKey, error) {
	key, err := s.mem.invitationKey(secret)
	return key, errors.WithStack(err)
}

// Destroys the session's memory - zeroing out any sensitive info
func (s *Session) Destroy() {
}

// Returns the subscriber id associated with this session.  This uniquely identifies
// an account to the world.  This may be shared over other (possibly unsecure) channels
// in order to share with other users.
func (s *Session) MyId() uuid.UUID {
	return s.mem.Id
}

// Returns the session owner's public signing key.  This key and its id may be shared freely.
func (s *Session) MyKey() PublicKey {
	return s.mem.SigningKey.Pub
}

// Lists the session owner's currently pending invitations.
func (s *Session) MyInvitations(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Invitation, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	invites, err := s.net.InvitationsBySubscriber(cancel, token, s.MyId(), buildPagingOptions(fns...))
	return invites, errors.WithStack(err)
}

// Lists the session owner's currently active certificates.
func (s *Session) MyCertificates(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Certificate, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	certs, err := s.net.CertsBySubscriber(cancel, token, s.MyId(), buildPagingOptions(fns...))
	return certs, errors.WithStack(err)
}

// Keys that have been in some way trusted by the owner of the session.
func (s *Session) MyTrusts(cancel <-chan struct{}, fns ...func(*PagingOptions)) ([]Trust, error) {
	token, err := s.token(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.TrustsByMember(cancel, token, s.MyId(), opts)
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) LoadTrustById(cancel <-chan struct{}, id uuid.UUID) (Trust, bool, error) {
	token, err := s.token(cancel)
	if err != nil {
		return Trust{}, false, errors.WithStack(err)
	}

	trust, ok, err := s.net.TrustById(cancel, token, id)
	return trust, ok, errors.WithStack(err)
}

// Loads the trust with the given signing key.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) LoadTrustByKey(cancel <-chan struct{}, key string) (Trust, bool, error) {
	return Trust{}, false, nil
}

// Loads the trust with the given id.  The trust will be returned only
// if your public key has been invited to manage the trust and the invitation
// has been accepted.
func (s *Session) LoadInvitationById(cancel <-chan struct{}, id uuid.UUID) (Invitation, bool, error) {
	token, err := s.token(cancel)
	if err != nil {
		return Invitation{}, false, errors.WithStack(err)
	}

	trust, ok, err := s.net.InvitationById(cancel, token, id)
	return trust, ok, errors.WithStack(err)
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *Session) NewSecureTrust(cancel <-chan struct{}, name string, fns ...func(t *TrustOptions)) (Trust, error) {
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

	trust, err := newTrust(s.rand, s.MyId(), mySecret, mySigningKey, name, fns...)
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
func (s *Session) Invite(cancel <-chan struct{}, t Trust, memberId uuid.UUID, opts ...func(*InvitationOptions)) (Invitation, error) {
	return Invitation{}, nil
}

// Accepts the invitation.  The invitation must be valid and must be addressed
// to the owner of the session, or the session owner must be acting as a proxy.
func (s *Session) AcceptTrust(cancel <-chan struct{}, i Invitation) error {
	return errors.WithStack(i.acceptInvitation(cancel, s))
}

// Revokes trust from the given subscriber for the given trust.
func (s *Session) RevokeTrust(cancel <-chan struct{}, t Trust, sub uuid.UUID) error {
	return errors.WithStack(t.revokeCertificate(cancel, s, sub))
}

// Renew's the session owner's certificate with the trust.
func (s *Session) RenewTrust(cancel <-chan struct{}, t Trust) (Trust, error) {
	trust, err := t.renewCertificate(cancel, s)
	return trust, errors.WithStack(err)
}
