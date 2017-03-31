package warden

import uuid "github.com/satori/go.uuid"

type transport interface {
	LoadSignature(s Session, domain string, id uuid.UUID) (Signature, error)
	StoreSignature(s Session, domain string, sig Signature) (uuid.UUID, error)

	ListDocuments(s Session, domain string) ([]Document, error)
	LoadDocument(s Session, id uuid.UUID) (Document, error)
}
