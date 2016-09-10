package msg

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrUnexpectedState = errors.New("STATE_MACHINE:UNEXPECTED_STATE")
)

const (
	StateMachineWait = 5 * time.Millisecond
	EmptyState = 0
)

type State uint32

type StateMachine struct {
	sync.RWMutex
	cur State
}

func To(s State) func() State {
	return func() State {return s}
}

func NewStateMachine(init State) *StateMachine {
	return &StateMachine{cur: init}
}

func (c *StateMachine) Get() State {
	c.RLock()
	defer c.RUnlock()
	return c.cur
}

func (c *StateMachine) ApplyIf(state State, fn func()) (State, error) {
	c.RLock()
	defer c.RUnlock()

	if c.cur&state == EmptyState {
		return c.cur, ErrUnexpectedState
	}

	fn()
	return c.cur, nil
}

func (c *StateMachine) Transition(from State, fn func() State) (State, error) {
	c.Lock()
	defer c.Unlock()

	if c.cur&from == EmptyState {
		return c.cur, ErrUnexpectedState
	}

	c.cur = fn()
	return c.cur, nil
}

func (c *StateMachine) WaitUntil(state State) State {
	// just spin, waiting for an appropriate state
	for {
		if c.Get()&state != EmptyState {
			return c.Get() & state
		}

		time.Sleep(StateMachineWait)
	}
}
