package warden

import "github.com/pkg/errors"

type Domain struct {

	// the identifier of the domain.
	Id string

	// the description
	Description string

	// the strength requirement of the domain.
	strength int

	// the level of trust that was derived when loading (this is only used to prevent unnecessary client calls)
	lvl LevelOfTrust

	// the encrypted oracle. (Only available for trusted users)
	oracle oracle

	// the encrypted oracle. (Only available for trusted users)
	oracleKey oracleKey

	// the encrypted signing key.  (Only available for trusted users)
	signingKey SigningKey
}


// Decrypts the domain oracle.  Requires *Encryption* trust
func (d Domain) unlockOracle(s Session) (Bytes, line, error) {
	if err := Encryption.EnsureExceeded(d.lvl); err != nil {
		return nil, line{}, errors.WithStack(err)
	}
	return d.oracle.Unlock(d.oracleKey, s.seed)
}

// Loads all the trust certificates that have been issued by this domain.
func (d Domain) IssuedCertificates(cancel <-chan struct{}, s Session, opts ...func(*PagingOptions)) ([]Certificate, error) {
	if err := Verify.EnsureExceeded(d.lvl); err != nil {
		return nil, errors.WithStack(err)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return s.net.LoadCertificatesByDomain(cancel, auth, d.Id, opts...)
}

// Revokes all issued certificates by this domain for the given subscriber.
func (d Domain) RevokeCertificate(cancel <-chan struct{}, s Session, subscriber string) error {
	if err := Revoke.EnsureExceeded(d.lvl); err != nil {
		return errors.WithStack(err)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return errors.WithStack(err)
	}

	cert, err := s.net.LoadCertificateByDomainAndTrustee(cancel, auth, d.Id, subscriber)
	if err != nil {
		return errors.Wrapf(err, "Error loading certificates for domain [%v] and subscriber [%v]", subscriber)
	}

	if err := s.net.RevokeCertificate(cancel, auth, cert.Id); err != nil {
		return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", cert.Id, subscriber)
	}

	return nil
}

// Issues an invitation to the given key.
func (d Domain) IssueInvitation(cancel <-chan struct{}, s Session, trustee string, opts ...func(*InvitationOptions)) (Invitation, error) {
	if err := Invite.EnsureExceeded(d.lvl); err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	auth, err := s.auth(cancel)
	if err != nil {
		return Invitation{}, errors.WithStack(err)
	}

	seed, line, err := d.unlockOracle(s)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to unlock domain oracle [%v]", d.Id)
	}

	defer seed.Destroy()
	defer line.Destroy()

	domainKey, err := d.signingKey.Decrypt(seed)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving domain signing key [%v]", d.Id)
	}
	defer domainKey.Destroy()

	issuerKey, err := s.mySigningKey()
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving my signing key [%v]", s.MyId())
	}
	defer issuerKey.Destroy()

	trusteeKey, err := s.net.LoadPublicKey(cancel, auth, trustee)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error retrieving public key [%v]", trustee)
	}

	inv, err := generateInvitation(s.rand, d, line, issuerKey, trusteeKey, opts...)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error generating invitation to trustee [%v] for domain [%v]", trustee, d.Id)
	}

	domainSig, err := inv.Sign(domainKey, SHA256)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing invitation with domain key: %v", inv)
	}

	issuerSig, err := inv.Sign(issuerKey, SHA256)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Error signing invitation with issuer key: %v", inv)
	}

	if err := s.net.RegisterInvitation(cancel, auth, inv, domainSig, issuerSig); err != nil {
		return Invitation{}, errors.Wrapf(err, "Error registering invitation: %v", inv)
	}

	return inv, nil
}

// // A domain represents a group of documents under the control of a single (possibly shared) private key.
// //
// // You may access a domain only if you have established trust.
// type domain interface {
//
// // The unique identifier of the domain
// Id() string
//
// // The public key of the domain.
// PublicKey() PublicKey
//
// // A short, publicly viewable description of the domain (not advertised, but not public)
// Description() string
//
// // Loads all the trust certificates that have been issued by this domain
// IssuedCertificates(cancel <-chan struct{}, beg int, end int) ([]Certificate, error)
//
// // Revokes all certificates for the given key.  The trustee will no longer be able to act in the management of the domain.
// RevokeCertificates(cancel <-chan struct{}, key string) error
//
// // Loads all the issued invitations that have been issued by this domain
// IssuedInvitations(cancel <-chan struct{}, session Session) ([]Invitation, error)
//
// // Issues an invitation to the given key.
// IssueInvitation(cancel <-chan struct{}, session Session, key string, level LevelOfTrust, ttl time.Duration) (Invitation, error)
//
// // Issues an invitation to the given key.
// RevokeInvitation(cancel <-chan struct{}, session Session, key string, level LevelOfTrust, ttl time.Duration) (Invitation, error)
//
// // // Lists all the document names under the control of this domain
// // ListDocuments(cancel <-chan struct{}, session Session) ([]string, error)
// //
// // // Loads a specific document.
// // LoadDocument(cancel <-chan struct{}, session Session, name []byte) (Document, error)
// //
// // // Stores a document under the domain
// // StoreDocument(cancel <-chan struct{}, session Session, name []byte, ver int, contents []byte) (Document, error)
// //
// // // Stores a document under the domain
// // DeleteDocument(cancel <-chan struct{}, session Session, name []byte, ver int) error
// }
