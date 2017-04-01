package warden

import uuid "github.com/satori/go.uuid"

type transport interface {
	LoadPublicKey(cancel <-chan struct{}, a authToken, domain string, key string) (PublicKey, error)
	LoadPartialSecretKey(cancel <-chan struct{}, a authToken, domain string, key string) (PartialSecretKey, error)
	LoadPartialPrivateKey(cancel <-chan struct{}, a authToken, domain string, key string) (PartialPrivateKey, error)

	LoadSignature(cancel <-chan struct{}, a authToken, id uuid.UUID) (Signature, error)

	// ListDocuments(s authToken, domain string) ([]Document, error)
	// LoadDocument(s authToken, id uuid.UUID) (Document, error)
}
