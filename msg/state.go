package msg

import (
	"errors"
	"sync"
	"time"
)

var (
	UnexpectedState = errors.New("STATE_MACHINE:UNEXPECTED_STATE")
)

const (
	StateMachineWait = 5*time.Millisecond
)

type State uint32

type StateMachine struct {
	sync.RWMutex
	cur State
}

func NewStateMachine(init State) *StateMachine {
	return &StateMachine{cur: init}
}

func (c *StateMachine) Get() State {
	c.RLock()
	defer c.RUnlock()
	return c.cur
}

func (c *StateMachine) ApplyIf(expected State, fn func()) error {
	c.RLock()
	defer c.RUnlock()

	if c.cur != expected {
		return UnexpectedState
	}

	fn()
	return nil
}

func (c *StateMachine) Transition(from State, fn func() State) error {
	c.Lock()
	defer c.Unlock()

	if c.cur != from {
		return UnexpectedState
	}

	c.cur = fn()
	return nil
}

func (c *StateMachine) WaitUntil(states ...State) State {
	// just spin, waiting for an appropriate state
	for {
		s := c.Get()
		for _, t := range states {
			if s == t {
				return s
			}
		}

		time.Sleep(StateMachineWait)
	}
}
