package warden

import (
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

type TrustOptions struct {
	Oracle OracleOptions
	Sign   KeyPairOptions
}

func buildTrustOptions(fns ...func(*TrustOptions)) TrustOptions {
	ret := TrustOptions{buildOracleOptions(), buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A trust represents a personal perspective of
//
// Elements of the  will be populated based on the subscriber's
// trust level.
type Trust struct {
	Id uuid.UUID

	// the session owner's certificate with the .
	Cert SignedCertificate

	// the 's encrypted oracle. (Only available for trusted users)
	oracle SignedOracle

	// the encrypted oracle. (Only available for trusted users)
	oracleKey SignedOracleKey

	// the encrypted signing key.  (Only available for trusted users)
	ident KeyPair
}

// generates a trust, but has no server-side effects.
func generateTrust(s *Session, desc string, fns ...func(s *TrustOptions)) (Trust, error) {
	opts := buildTrustOptions(fns...)

	mySigningKey, err := s.mySigningKey()
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	priv, err := opts.Sign.Algorithm.Gen(s.rand, opts.Sign.Strength)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating  key [%v]: %v", opts.Sign.Algorithm, opts.Sign.Strength)
	}
	defer priv.Destroy()

	domId, myId := uuid.NewV1(), s.MyId()

	oracle, curve, err := genOracle(s.rand, opts.Oracle)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating : %v", desc)
	}
	defer curve.Destroy()

	oracleKey, err := genOracleKey(s.rand, curve, s.myOracle(), opts.Oracle)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating private oracle key [%v]", myId)
	}

	ident, err := genKeyPair(s.rand, priv, curve.Bytes(), opts.Sign)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error generating signing key with opts: %+v", opts.Sign)
	}

	cert := newCertificate(domId, myId, myId, Creator, OneHundredYears)

	signedOracle, err := oracle.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Trust{}, err
	}

	signedOracleKey, err := oracleKey.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Trust{}, err
	}

	domSig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Trust{}, err
	}

	mySig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return Trust{}, err
	}

	return Trust{
		domId,
		SignedCertificate{cert, domSig, mySig, mySig},
		signedOracle,
		signedOracleKey,
		ident,
	}, nil
}

// Extracts the  oracle curve.  Requires *Encrypt* level trust
func (d Trust) unlockCurve(s Session) (line, error) {
	if err := Encrypt.verify(d.Cert.Level); err != nil {
		return line{}, errors.WithStack(err)
	}

	return d.oracle.Unlock(d.oracleKey.OracleKey, s.myOracle())
}

func (d Trust) unlockSigningKey(s Session) (PrivateKey, error) {
	if err := Sign.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	curve, err := d.unlockCurve(s)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return d.ident.Decrypt(curve.Bytes())
}

func (d Trust) renewCertificate(cancel <-chan struct{}, s Session) (Trust, error) {
	if err := Invite.verify(d.Cert.Level); err != nil {
		return Trust{}, errors.WithStack(err)
	}

	myId := s.MyId()

	mySigningKey, err := s.mySigningKey()
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	domSigningKey, err := d.unlockSigningKey(s)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}
	defer domSigningKey.Destroy()

	cert := newCertificate(d.Id, myId, myId, d.Cert.Level, d.Cert.Duration())

	mySig, err := d.Cert.Sign(s.rand, mySigningKey, d.oracle.Opts.SigHash)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error signing cert with session signing key [%v]", s.MyId())
	}

	domSig, err := d.Cert.Sign(s.rand, domSigningKey, d.oracle.Opts.SigHash)
	if err != nil {
		return Trust{}, errors.Wrapf(err,
			"Error signing with  key [%v]", d.Id)
	}

	token, err := s.auth(cancel)
	if err != nil {
		return Trust{}, errors.WithStack(err)
	}

	if err := s.net.Certs.Register(cancel, token, cert, d.oracleKey.OracleKey, domSig, mySig, mySig); err != nil {
		return Trust{}, errors.Wrapf(err, "Error registering ")
	}

	return Trust{
		d.Id,
		SignedCertificate{cert, domSig, mySig, mySig},
		d.oracle,
		d.oracleKey,
		d.ident,
	}, nil
}

// Loads all the trust certificates that have been issued by this .
func (d Trust) listCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return s.net.Certs.ActiveByTrust(cancel, auth, d.Id, opts.Beg, opts.End)
}

// Revokes all issued certificates by this  for the given subscriber.
func (d Trust) revoke(cancel <-chan struct{}, s Session, trustee uuid.UUID) error {
	if err := Revoke.verify(d.Cert.Level); err != nil {
		return errors.WithStack(err)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	cert, ok, err := s.net.Certs.ActiveBySubscriberAndTrust(cancel, auth, trustee, d.Id)
	if err != nil {
		return errors.Wrapf(err, "Error loading certificates for  [%v] and subscriber [%v]", d.Id, trustee)
	}

	if !ok {
		return errors.Wrapf(InvariantError, "No existing trust certificate from  [%v] to subscriber [%v]", d.Id, trustee)
	}

	if err := s.net.Certs.Revoke(cancel, auth, cert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", cert.Id, trustee)
	}

	return nil
}

// Issues an invitation to the given key.
func (d Trust) invite(cancel <-chan struct{}, s Session, trustee uuid.UUID, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.verify(d.Cert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, d.Cert.Level)
	}

	opts := buildInvitationOptions(fns...)

	auth, err := s.auth(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	line, err := d.unlockCurve(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", d.Id)
	}
	defer line.Destroy()

	Key, err := d.ident.Decrypt(line.Format())
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving  signing key [%v]", d.Id)
	}
	defer Key.Destroy()

	issuerKey, err := s.mySigningKey()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	trusteeKey, err := s.net.Keys.BySubscriber(cancel, auth, trustee)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving public key [%v]", trustee)
	}

	cert := newCertificate(d.Id, s.MyId(), trustee, opts.Lvl, opts.Exp)

	inv, err := generateInvitation(s.rand, line, cert, Key, issuerKey, trusteeKey, opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for  [%v]", trustee, d.Id)
	}

	if err := s.net.Invites.Upload(cancel, auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
