package warden

import (
	"crypto/rand"
	"time"

	"github.com/pkg/errors"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

type DomainOptions struct {
	oracleOptions

	SigningAlgorithm KeyAlgorithm
	SigningStrength  int
	SigningCipher    SymmetricCipher
	SigningHash      Hash
	SigningIter      int
	SigningSalt      int
}

func buildDomainOptions(fns ...func(*DomainOptions)) DomainOptions {
	ret := DomainOptions{defaultOracleOptions(), RSA, 2048, AES_256_GCM, SHA256, 1024, 32}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// The domain is two things.  1) it is a description of a domain
type Domain struct {

	// the identifier of the domain.
	Id string

	// the domain description
	Description string

	// the session owner's certificate with the domain.
	cert Certificate

	// the domain's public key.  (Only available for trusted users)
	pub PublicKey

	// the domain's encrypted oracle. (Only available for trusted users)
	oracle oracle

	// the encrypted oracle. (Only available for trusted users)
	oracleKey oracleKey

	// the encrypted signing key.  (Only available for trusted users)
	signingKey SigningKey
}

func generateDomain(s Session, desc string, fns ...func(s *DomainOptions)) (Domain, error) {
	opts := buildDomainOptions(fns...)

	priv, err := opts.SigningAlgorithm.Gen(s.rand, opts.SigningStrength)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating domain key [%v]: %v", opts.SigningAlgorithm, opts.SigningStrength)
	}
	defer priv.Destroy()

	pub, domId, myId := priv.Public(), priv.Public().Id(), s.MyId()

	oracle, curve, err := generateOracle(rand.Reader, domId)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating domain: %v", desc)
	}
	defer curve.Destroy()

	oracleKey, err := generateOracleKey(s.rand, domId, myId, curve, s.myOracle(), opts.oracleOptions)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating private oracle key [%v]", myId)
	}

	sign, err := genSigningKey(s.rand, priv, curve.Bytes(), opts.SigningCipher, opts.SigningHash, opts.SigningSalt, opts.SigningIter)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating signing key [%v]: %v", opts.SigningAlgorithm, opts.SigningStrength)
	}

	cert := newCertificate(domId, myId, myId, Destroy, OneHundredYears)
	return Domain{
		domId,
		desc,
		cert,
		pub,
		oracle,
		oracleKey,
		sign,
	}, nil
}

// Decrypts the domain oracle.  Requires *Encrypt* level trust
func (d Domain) unlockCurve(s Session) (line, error) {
	if err := Encrypt.Verify(d.cert.Level); err != nil {
		return line{}, errors.WithStack(err)
	}

	return d.oracle.Unlock(d.oracleKey, s.myOracle())
}

func (d Domain) unlockSigningKey(s Session) (PrivateKey, error) {
	if err := Sign.Verify(d.cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	curve, err := d.unlockCurve(s)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return d.signingKey.Decrypt(curve.Bytes())
}

// Loads all the trust certificates that have been issued by this domain.
func (d Domain) RenewCertificate(cancel <-chan struct{}, s Session) (Domain, error) {
	if err := Invite.Verify(d.cert.Level); err != nil {
		return Domain{}, errors.WithStack(err)
	}

	myId := s.MyId()

	mySigningKey, err := s.MySigningKey()
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	domSigningKey, err := d.unlockSigningKey(s)
	if err != nil {
		return Domain{}, errors.WithStack(err)
	}
	defer domSigningKey.Destroy()

	cert := newCertificate(d.Id, myId, myId, d.cert.Level, d.cert.Duration())

	mySig, err := d.cert.Sign(s.rand, mySigningKey, d.oracle.opts.SigHash)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error signing cert with session signing key [%v]", s.MyId())
	}

	domSig, err := d.cert.Sign(s.rand, domSigningKey, d.oracle.opts.SigHash)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error signing with domain key [%v]", d.Id)
	}

	token, err := s.auth(cancel)
	if err != nil {
		return Domain{}, errors.WithStack(err)
	}

	if err := s.net.Certs.Register(cancel, token, cert, d.oracleKey, domSig, mySig, mySig); err != nil {
		return Domain{}, errors.Wrapf(err, "Error registering domain")
	}

	return Domain{
		d.Id,
		d.Description,
		cert,
		d.pub,
		d.oracle,
		d.oracleKey,
		d.signingKey,
	}, nil
}

// Loads all the trust certificates that have been issued by this domain.
func (d Domain) IssuedCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.Verify(d.cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return s.net.Certs.ActiveByDomain(cancel, auth, d.Id, opts.Beg, opts.End)
}

// Revokes all issued certificates by this domain for the given subscriber.
func (d Domain) RevokeCertificate(cancel <-chan struct{}, s Session, trustee string) error {
	if err := Revoke.Verify(d.cert.Level); err != nil {
		return errors.WithStack(err)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	cert, ok, err := s.net.Certs.ActiveBySubscriberAndDomain(cancel, auth, trustee, d.Id)
	if err != nil {
		return errors.Wrapf(err, "Error loading certificates for domain [%v] and subscriber [%v]", d.Id, trustee)
	}

	if !ok {
		return errors.Wrapf(DomainInvariantError, "No existing trust certificate from domain [%v] to subscriber [%v]", d.Id, trustee)
	}

	if err := s.net.Certs.Revoke(cancel, auth, cert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", cert.Id, trustee)
	}

	return nil
}

// Issues an invitation to the given key.
func (d Domain) IssueInvitation(cancel <-chan struct{}, s Session, trustee string, opts ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.Verify(d.cert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, d.cert.Level)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	line, err := d.unlockCurve(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock domain oracle [%v]", d.Id)
	}

	defer line.Destroy()

	domainKey, err := d.signingKey.Decrypt(line.Format())
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving domain signing key [%v]", d.Id)
	}
	defer domainKey.Destroy()

	issuerKey, err := s.MySigningKey()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	trusteeKey, err := s.net.Keys.BySubscriber(cancel, auth, trustee)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving public key [%v]", trustee)
	}

	inv, err := generateInvitation(s.rand, line, domainKey, issuerKey, trusteeKey, opts...)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for domain [%v]", trustee, d.Id)
	}

	if err := s.net.Invites.Upload(cancel, auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
