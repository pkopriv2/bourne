package kayak

import (
	"errors"

	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	EvictedError   = errors.New("Kayak:Evicted")
	FailedError    = errors.New("Kayak:Failed")
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	TimeoutError   = errors.New("Kayak:TimeoutError")
)

type event interface {
	scribe.Writable
}

type Parser func(scribe.Reader) (event, error)
