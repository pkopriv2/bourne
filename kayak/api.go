package kayak

import (
	"errors"
	"fmt"
	"time"

	"github.com/pkopriv2/bourne/scribe"
)

// Public Error Types
var (
	EvictedError   = errors.New("Kayak:Evicted")
	FailedError    = errors.New("Kayak:Failed")
	ClosedError    = errors.New("Kayak:Closed")
	NotLeaderError = errors.New("Kayak:NotLeader")
	NoLeaderError  = errors.New("Kayak:NoLeader")
)

type TimeoutError struct {
	timeout time.Duration
	msg     string
}

func NewTimeoutError(timeout time.Duration, msg string) TimeoutError {
	return TimeoutError{timeout, msg}
}

func (t TimeoutError) Error() string {
	return fmt.Sprintf("Timeout[%v]: %v", t.timeout, t.msg)
}

type event interface {
	scribe.Writable
}

type Parser func(scribe.Reader) (event, error)
