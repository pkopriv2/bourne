package warden

import (
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const OneHundredYears = 100 * 365 * 24 * time.Hour

type SharingOptions struct {
	SecretOptions
	Sign KeyPairOptions
}

func buildSharingOptions(fns ...func(*SharingOptions)) SharingOptions {
	ret := SharingOptions{buildSecretOptions(), buildKeyPairOptions()}
	for _, fn := range fns {
		fn(&ret)
	}
	return ret
}

// A key ring is the basis of the trust system.
type KeyRing struct {
	Id uuid.UUID

	// the public signing key of the ring.  (This is used to uniquely identify a ring)
	Pub PublicKey

	// the session owner's certificate with the .
	Cert SignedCertificate

	// the key ring's encrypted oracle. (Only available for key ringed users)
	oracle signedPublicShard

	// the encrypted oracle. (Only available for key ringed users)
	oracleKey signedPrivateShard
}

// generates a key ring, but has no server-side effects.
func generateKeyRing(s *Session, name string, fns ...func(s *SharingOptions)) (KeyRing, SignedKeyPair, error) {
	opts := buildSharingOptions(fns...)

	mySigningKey, err := s.mySigningKey()
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error extracting session signing key [%v]", s.MyId())
	}
	defer mySigningKey.Destroy()

	priv, err := opts.Sign.Algorithm.Gen(s.rand, opts.Sign.Strength)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error generating  key [%v]: %v", opts.Sign.Algorithm, opts.Sign.Strength)
	}
	defer priv.Destroy()

	domId, myId := uuid.NewV1(), s.MyId()

	oracle, curve, err := genSharedSecret(s.rand, opts.SecretOptions)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error generating key ring: %v", name)
	}
	defer curve.Destroy()

	oracleKey, err := genPrivateShard(s.rand, curve, s.myOracle(), opts.SecretOptions)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, errors.Wrapf(err,
			"Error generating private oracle key [%v]", myId)
	}

	// ident, err := genKeyPair(s.rand, priv, curve.Bytes(), opts.Sign)
	// if err != nil {
	// return Trust{}, SignedKeyPair{}, errors.Wrapf(err,
	// "Error generating signing key with opts: %+v", opts.Sign)
	// }

	cert := newCertificate(domId, myId, myId, Creator, OneHundredYears)

	signedOracle, err := oracle.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, err
	}

	signedOracleKey, err := oracleKey.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, err
	}

	domSig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, err
	}

	mySig, err := cert.Sign(s.rand, priv, opts.Sign.Hash)
	if err != nil {
		return KeyRing{}, SignedKeyPair{}, err
	}

	return KeyRing{
		domId,
		nil,
		SignedCertificate{cert, domSig, mySig, mySig},
		signedOracle,
		signedOracleKey,
	}, SignedKeyPair{}, nil
}

// Extracts the  oracle curve.  Requires *Encrypt* level key ring
func (d KeyRing) unlockSecret(s *Session) (Secret, error) {
	if err := Encrypt.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}
	return nil, nil
}

func (d KeyRing) unlockSigningKey(cancel <-chan struct{}, s *Session, secret Secret) (PrivateKey, error) {
	if err := Sign.verify(d.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}
	return nil, nil
}

func (d KeyRing) renewCertificate(cancel <-chan struct{}, s *Session) (KeyRing, error) {
	return d, nil
	// if err := Invite.verify(d.Cert.Level); err != nil {
	// return KeyRing{}, errors.WithStack(err)
	// }
	//
	// myId := s.MyId()
	//
	// mySigningKey, err := s.mySigningKey()
	// if err != nil {
	// return KeyRing{}, errors.Wrapf(err,
	// "Error extracting session signing key [%v]", s.MyId())
	// }
	// defer mySigningKey.Destroy()
	//
	// domSigningKey, err := d.unlockSigningKey(cancel, s, )
	// if err != nil {
	// return KeyRing{}, errors.WithStack(err)
	// }
	// defer domSigningKey.Destroy()
	//
	// cert := newCertificate(d.Id, myId, myId, d.Cert.Level, d.Cert.Duration())
	//
	// mySig, err := d.Cert.Sign(s.rand, mySigningKey, d.oracle.Opts.SigHash)
	// if err != nil {
	// return KeyRing{}, errors.Wrapf(err,
	// "Error signing cert with session signing key [%v]", s.MyId())
	// }
	//
	// domSig, err := d.Cert.Sign(s.rand, domSigningKey, d.oracle.Opts.SigHash)
	// if err != nil {
	// return KeyRing{}, errors.Wrapf(err,
	// "Error signing with  key [%v]", d.Id)
	// }
	//
	// if err := s.net.Certs.Register(cancel, s.auth, cert, d.oracleKey.oracleKey, domSig, mySig, mySig); err != nil {
	// return KeyRing{}, errors.Wrapf(err, "Error registering ")
	// }
	//
	// return KeyRing{
	// d.Id,
	// nil,
	// SignedCertificate{cert, domSig, mySig, mySig},
	// d.oracle,
	// d.oracleKey,
	// }, nil
}

// Loads all the key ring certificates that have been issued by this .
func (t KeyRing) listCertificates(cancel <-chan struct{}, s Session, fns ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.verify(t.Cert.Level); err != nil {
		return nil, errors.WithStack(err)
	}

	opts := buildPagingOptions(fns...)
	return s.net.Certs.ActiveByTrust(cancel, s.auth, t.Id, opts.Beg, opts.End)
}

// Revokes all issued certificates by this  for the given subscriber.
func (t KeyRing) revokeCertificate(cancel <-chan struct{}, s *Session, trustee uuid.UUID) error {
	if err := Revoke.verify(t.Cert.Level); err != nil {
		return errors.WithStack(err)
	}

	if err := s.net.Certs.Revoke(cancel, s.auth, t.Cert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", t.Cert.Id, trustee)
	}

	return nil
}

// Issues an invitation to the given key.
func (t KeyRing) invite(cancel <-chan struct{}, s *Session, trustee KeyRing, fns ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.verify(t.Cert.Level); err != nil {
		return Invitation{}, newLevelOfTrustError(Invite, t.Cert.Level)
	}

	opts := buildInvitationOptions(fns...)

	secret, err := t.unlockSecret(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock  oracle [%v]", t.Id)
	}
	defer secret.Destroy()

	ringKey, err := t.unlockSigningKey(cancel, s, secret)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Erro retrieving signing key [%v]", t.Id)
	}
	defer ringKey.Destroy()

	issuerKey, err := s.mySigningKey()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	cert := newCertificate(t.Id, s.MyId(), trustee.Id, opts.Lvl, opts.Exp)

	inv, err := generateInvitation(s.rand, secret, cert, ringKey, issuerKey, trustee.Pub, opts)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for  [%v]", trustee, t.Id)
	}

	if err := s.net.Invites.Upload(cancel, s.auth, inv); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}
