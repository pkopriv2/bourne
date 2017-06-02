package warden

import uuid "github.com/satori/go.uuid"

// A subscription
type Subscription struct {
	Id         uuid.UUID
	Version    uint64
	Enterprise bool
	Enabled    bool
	MaxMembers uint64
	MaxTrusts  uint64
	MaxGb      uint64
}

type SubscriptionManager interface {

	// Cancels the subscription
	Cancel() error

	// Disables the member from the subscription (with the given email)
	DisableMemberByEmail(email string) error

	// Disables the key from the subscription
	DisableMemberByKey(key string) error

	// Disables the key from being able to login
	DisableLoginKey(key string) error

	// Sends a registration token via email.
	SendRegistration(email string, role Role) error

	// Creates and returns a new registration token
	CreateRegistrationToken(role Role) (SignedToken, error)
}
