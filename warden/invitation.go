package warden

import (
	"io"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type InvitationOptions struct {
	Cipher SymmetricCipher
	Hash   Hash
	Lvl    LevelOfTrust
	Ttl    time.Duration
}

func buildInvitationOptions(opts ...func(*InvitationOptions)) InvitationOptions {
	def := InvitationOptions{AES_128_GCM, SHA256, Encryption, 365 * 24 * time.Hour}
	for _, fn := range opts {
		fn(&def)
	}
	return def
}

// An invitation is a cryptographically secured message asking the recipient to share in the
// management of a domain. The invitation may only be accepted by the intended recipient.
// These technically can be shared publicly, but exposure should be limited (typically only the
// trust system needs to know).
type Invitation struct {
	Id uuid.UUID

	Domain  string
	Issuer  string
	Trustee string

	Level LevelOfTrust

	IssuedAt  time.Time
	StartsAt  time.Time
	ExpiresAt time.Time

	key KeyExchange
	msg CipherText
}

func generateInvitation(rand io.Reader, d Domain, oracleLine line, issuerKey PrivateKey, trusteeKey PublicKey, fns ...func(*InvitationOptions)) (Invitation, error) {
	opts := buildInvitationOptions(fns...)

	pt, err := generatePoint(rand, oracleLine, d.strength)
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}
	defer pt.Destroy()

	exchg, msg, err := AsymmetricEncrypt(rand, trusteeKey, opts.Hash, opts.Cipher, pt.Bytes())
	if err != nil {
		return Invitation{}, errors.Wrapf(err, "Unable to generate invitation for trustee [%v] to join [%v]", trusteeKey.Id(), d.Id)
	}

	now := time.Now()
	return Invitation{
		uuid.NewV1(),
		d.Id,
		issuerKey.Public().Id(),
		trusteeKey.Id(),
		opts.Lvl,
		now,
		now,
		now.Add(opts.Ttl),
		exchg,
		msg,
	}, nil
}

//
func (i Invitation) Bytes() []byte {
	return nil
}

// Verifies that the signature matches the certificate contents.
func (c Invitation) Verify(key PublicKey, signature Signature) error {
	return nil
}

// Signs the certificate with the private key.
func (c Invitation) Sign(key PrivateKey, hash Hash) (Signature, error) {
	return Signature{}, nil
}
