package warden

import (
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

type DomainOptions struct {
	Oracle OracleOptions
	Sign   KeyPairOptions
}

func buildDomainOptions(fns ...func(*DomainOptions)) DomainOptions {
	ret := DomainOptions{buildOracleOptions(), buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A domain is an individual subscriber's perspective of a domain.
//
// Elements of the domain will be populated based on the subscriber's
// trust level.
type Domain struct {
	Id uuid.UUID

	// the session owner's certificate with the domain.
	cert SignedCertificate

	// the domain's encrypted oracle. (Only available for trusted users)
	oracle SignedOracle

	// the encrypted oracle. (Only available for trusted users)
	oracleKey SignedOracleKey

	// the encrypted signing key.  (Only available for trusted users)
	ident KeyPair
}

// generates a domain, but has no server-side effects.  the domain must still
// be registered.
func generateDomain(s Session, desc string, fns ...func(s *DomainOptions)) (Domain, error) {
	opts := buildDomainOptions(fns...)

	mySigningKey, err := s.MySigningKey()
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	priv, err := opts.Sign.Algorithm.Gen(s.rand, opts.Sign.Strength)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating domain key [%v]: %v", opts.Sign.Algorithm, opts.Sign.Strength)
	}
	defer priv.Destroy()

	domId, myId := uuid.NewV1(), s.MyId()

	oracle, curve, err := genOracle(s.rand, opts.Oracle)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating domain: %v", desc)
	}
	defer curve.Destroy()

	oracleKey, err := genOracleKey(s.rand, curve, s.myOracle(), opts.Oracle)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating private oracle key [%v]", myId)
	}

	ident, err := genKeyPair(s.rand, priv, curve.Bytes(), opts.Sign)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error generating signing key with opts: %+v", opts.Sign)
	}

	cert := newCertificate(domId, myId, myId, Creator, OneHundredYears)

	signedOracle, err := oracle.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Domain{}, err
	}

	signedOracleKey, err := oracleKey.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Domain{}, err
	}

	domSig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Domain{}, err
	}

	mySig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Domain{}, err
	}

	return Domain{
		domId,
		SignedCertificate{cert, domSig, mySig, mySig},
		signedOracle,
		signedOracleKey,
		ident,
	}, nil
}

// Extracts the domain oracle curve.  Requires *Encrypt* level trust
func (d Domain) unlockCurve(s Session) (line, error) {
	if err := Encrypt.Verify(d.cert.Level); err != nil {
		return line{}, errors.WithStack(err)
	}

	return d.oracle.Unlock(d.oracleKey.OracleKey, s.myOracle())
}

func (d Domain) unlockSigningKey(s Session) (PrivateKey, error) {
	if err := Sign.Verify(d.cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	curve, err := d.unlockCurve(s)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return d.ident.Decrypt(curve.Bytes())
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

	mySig, err := d.cert.Sign(s.rand, mySigningKey, d.oracle.Opts.SigHash)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error signing cert with session signing key [%v]", s.MyId())
	}

	domSig, err := d.cert.Sign(s.rand, domSigningKey, d.oracle.Opts.SigHash)
	if err != nil {
		return Domain{}, errors.Wrapf(err,
			"Error signing with domain key [%v]", d.Id)
	}

	token, err := s.auth(cancel)
	if err != nil {
		return Domain{}, errors.WithStack(err)
	}

	if err := s.net.Certs.Register(cancel, token, cert, d.oracleKey.OracleKey, domSig, mySig, mySig); err != nil {
		return Domain{}, errors.Wrapf(err, "Error registering domain")
	}

	return Domain{
		d.Id,
		SignedCertificate{cert, domSig, mySig, mySig},
		d.oracle,
		d.oracleKey,
		d.ident,
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
func (d Domain) RevokeCertificate(cancel <-chan struct{}, s Session, trustee uuid.UUID) error {
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
func (d Domain) IssueInvitation(cancel <-chan struct{}, s Session, trustee uuid.UUID, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.Verify(d.cert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, d.cert.Level)
	}

	opts := buildInvitationOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	line, err := d.unlockCurve(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock domain oracle [%v]", d.Id)
	}
	defer line.Destroy()

	domainKey, err := d.ident.Decrypt(line.Format())
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

	cert := newCertificate(d.Id, s.MyId(), trustee, opts.Lvl, opts.Exp)

	inv, err := generateInvitation(s.rand, line, cert, domainKey, issuerKey, trusteeKey, opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for domain [%v]", trustee, d.Id)
	}

	if err := s.net.Invites.Upload(cancel, auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
