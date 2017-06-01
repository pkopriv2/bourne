package warden

type authProtocol string

// FIXME: Figure out versioning and compatibility strategy.
const (
	PassV1 authProtocol = "Pass/0.0.1" // try to use semantic versioning.
	SignV1              = "Sign/0.0.1"
)

// The authenticator is the server component responsible for authenticating a user.
type authenticator interface {
	Protocol() authProtocol
	Authenticate([]byte) error
}
