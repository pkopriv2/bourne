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
