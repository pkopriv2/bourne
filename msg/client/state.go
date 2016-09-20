package client

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	AtomicStateMachineWait = 5 * time.Millisecond
	EmptyAtomicState       = 0
	AnyAtomicState         = (1<<32)-1
)

type StateError struct {
	expected AtomicState
	actual AtomicState
}

func NewStateError(e AtomicState, a AtomicState) *StateError {
	return &StateError{e, a}
}

func (c *StateError) Error() string {
	return fmt.Sprintf("Unexpected AtomicState.  Expected: [%b] Actual: [%b]", c.expected, c.actual)
}

type AtomicState uint32

func NewAtomicState(state AtomicState) (*AtomicState) {
	var ret *AtomicState

	ret = &state
	return ret
}

func (s *AtomicState) Get() AtomicState {
	return AtomicState(atomic.LoadUint32((*uint32)(s)))
}

func (c *AtomicState) Is(state AtomicState) bool {
	return c.Get()&state != EmptyAtomicState
}

func (c *AtomicState) Transition(from AtomicState, to AtomicState) bool {
	return atomic.CompareAndSwapUint32((*uint32)(c), (uint32)(from), (uint32)(to))
}

func (c *AtomicState) WaitUntil(state AtomicState) AtomicState {
	// just spin, waiting for an appropriate state
	for {
		cur := c.Get()
		if cur&state != EmptyAtomicState {
			return cur
		}

		time.Sleep(100 * time.Millisecond)
	}
}