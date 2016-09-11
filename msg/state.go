package msg

import (
	"fmt"
	"sync"
	"time"
)

const (
	StateMachineWait = 5 * time.Millisecond
	EmptyState       = 0
)

type StateError struct {
	expected State
	actual   State
}

func NewStateError(e State, a State) *StateError {
	return &StateError{e, a}
}

func (c *StateError) Error() string {
	return fmt.Sprintf("Unexpected State.  Expected: [%b], Actual: [%b]", c.expected, c.actual)
}

type State uint32

type StateMachine struct {
	sync.RWMutex
	cur State
}

func To(s State) func() State {
	return func() State { return s }
}

func NewStateMachine(init State) *StateMachine {
	return &StateMachine{cur: init}
}

func (c *StateMachine) Get() State {
	c.RLock()
	defer c.RUnlock()
	return c.cur
}

func (c *StateMachine) Is(state State) bool {
	return c.Get()&state != EmptyState
}

func (c *StateMachine) If(state State, fn func()) error {
	cur := c.Get()
	if cur&state == EmptyState {
		return NewStateError(state, cur)
	}

	fn()
	return nil
}

func (c *StateMachine) Transition(from State, fn func() State) error {
	c.Lock()
	defer c.Unlock()
	if c.cur&from == EmptyState {
		return NewStateError(from, c.cur)
	}

	c.cur = fn()
	return nil
}

func (c *StateMachine) WaitUntil(state State) State {
	// just spin, waiting for an appropriate state
	for {
		cur := c.Get()
		if cur&state != EmptyState {
			return cur
		}

		time.Sleep(100 * time.Millisecond)
	}
}
