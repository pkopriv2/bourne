package kayak

import "errors"

// Public Error Types
var (
	EvictedError = errors.New("Convoy:Evicted")
	FailedError  = errors.New("Convoy:Failed")
	ClosedError  = errors.New("Convoy:Closed")
)
