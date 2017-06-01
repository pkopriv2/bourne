package warden

import (
	"io"

	"github.com/pkg/errors"
)

// A credential manages the client-side components of the login protocol.  In order
// to initiate the login, the consumer must provide two pieces of knowledge:
//
//   * an account lookup
//   * an authentication value
//
//
// For security reasons, credentials are short-lived.
//
type credential interface {
	Destroyer

	// Account lookup.
	MemberLookup() []byte

	// Auth identifier.  Usually associated to account lookup, but not necessary.
	AuthId() []byte


	// The protocol used to negotiate compatibility.
	Protocol() authProtocol

	// The arguments to be sent to the authenticator.  This value will be sent over
	// the wire, so this should
	//
	// Caution:  Implementations MUST NEVER return a value that can derive the encryption
	// key of the corresponding member shard. Doing so would eliminate the zero-knowledge
	// guarantees of the protocol.
	Auth(io.Reader) ([]byte, error)

	// Derives an encryption seed that may be used to decrypt the member's shard.
	DecryptShard(memberShard) (Shard, error)

	// Encrypts and signs a shard, producing a member shard.
	EncryptShard(io.Reader, Signer, Shard) (memberShard, error)
}

// Extract credentials from a closure
func extractCreds(fn func() credential) (credential, error) {
	creds := fn()
	if creds == nil {
		return nil, errors.Wrap(AuthError, "No credentials entered.")
	}
	return creds, nil
}
