package warden

import (
	"time"

	"github.com/pkg/errors"
)

type Domain struct {

	// the identifier of the domain.
	Id string

	// the description
	Description string

	// the public key (available to view by anyone)
	PublicKey PublicKey

	// the level of trust that was derived when loading (this is only used to prevent unnecessary client calls)
	lvl LevelOfTrust

	// the encrypted master key. (Only available for trusted users)
	masterKey oracle

	// the encrypted signing key.  (Only available for trusted users)
	signingKey signingKey
}

// Loads all the trust certificates that have been issued by this domain.
func (d Domain) IssuedCertificates(cancel <-chan struct{}, s Session, beg int, end int) ([]Certificate, error) {
	return s.net.LoadCertificatesByDomain(cancel, s.auth, d.Id, beg, end)
}

// Revokes all issued certificates by this domain for the given subscriber.
func (d Domain) RevokeCertificates(cancel <-chan struct{}, s Session, subscriber string) error {
	all, err := s.net.LoadCertificatesByDomainAndKey(cancel, s.auth, d.Id, subscriber)
	if err != nil {
		return errors.Wrapf(err, "Error loading certificates for domain [%v] and subscriber [%v]", subscriber)
	}

	for _, cert := range all {
		if err := s.net.RevokeCertificate(cancel, s.auth, cert.Id); err != nil {
			return errors.Wrapf(err, "Unable to revoke certificate [%v] for subscriber [%v]", cert.Id, subscriber)
		}
	}

	return nil
}

// Issues an invitation to the given key.
func (d Domain) IssueInvitation(cancel <-chan struct{}, session Session, key string, lvl LevelOfTrust, ttl time.Duration) (Invitation, error) {
	return Invitation{}, nil
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
