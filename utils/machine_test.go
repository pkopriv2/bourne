package utils

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestState_noResult(t *testing.T) {
	state := newState(1, func(c StateController) {})

	master := state.Run()

	result := <-master.Done()
	assert.Equal(t, TerminalState, result.Target)
	assert.Nil(t, result.Failure)
}

func TestState_workerfail(t *testing.T) {
	e := errors.New("error")

	state := newState(1, func(c StateController) {
		select {
		case <-c.Done():
		case c.Fail() <- e:
		}
	})

	controller := state.Run()
	result := <-controller.Done()

	assert.Equal(t, TerminalState, result.Target)
	assert.Equal(t, e, result.Failure)
}

func TestState_externalFail(t *testing.T) {
	e := errors.New("error")

	returned := false
	state := newState(1, func(c StateController) {
		<-c.Done()
		returned = true
	})

	controller := state.Run()
	controller.Fail() <- e
	result := <-controller.Done()

	assert.Equal(t, TerminalState, result.Target)
	assert.Equal(t, e, result.Failure)
	assert.True(t, returned)
}

func TestStateMachine_empty(t *testing.T) {
	factory := BuildStateMachine()
	machine := factory.Start(1)

	// data race here.
	c, err := machine.Control()
	if err != nil {
		assert.Nil(t, c)
		return
	}

	result := <-c.Done()
	assert.NotNil(t, result.Failure)
}

func TestStateMachine_IllegalTransition(t *testing.T) {
	factory := BuildStateMachine()
	factory.AddState(1, func(c StateController) {
		c.Next() <- 2
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.NotNil(t, result.Failure)
}

func TestStateMachine_SingleState(t *testing.T) {
	factory := BuildStateMachine()
	factory.AddState(1, func(c StateController) {})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.Nil(t, result.Failure)
	assert.Equal(t, []int{1}, result.Transitions)
}

func TestStateMachine_MultiState(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController) {
		c.Next() <- 2
	})
	factory.AddState(2, func(c StateController) {
		c.Next() <- 3
	})
	factory.AddState(3, func(c StateController) {
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.Nil(t, result.Failure)
	assert.Equal(t, []int{1, 2, 3}, result.Transitions)
}

func TestStateMachine_ExternalTransition(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController) {
		time.Sleep(100 * time.Millisecond)

		select {
		case c.Next() <- 2:
		case <-c.Done():
		}
	})
	factory.AddState(2, func(c StateController) {
		c.Next() <- 3
	})
	factory.AddState(3, func(c StateController) {
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	// skip 2
	var result MachineResult
	select {
	case result = <-c.Done():
	case c.Next() <- 3:
		result = <-c.Done()
	}

	assert.Nil(t, result.Failure)
	assert.Equal(t, []int{1, 3}, result.Transitions)
}

func TestStateMachine_ExternalFailure(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController) {
		time.Sleep(100 * time.Millisecond)

		select {
		case c.Next() <- 2:
		case <-c.Done():
		}
	})
	factory.AddState(2, func(c StateController) {
		c.Next() <- 3
	})
	factory.AddState(3, func(c StateController) {
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	// fail
	e := errors.New("error")
	var result MachineResult
	select {
	case result = <-c.Done():
	case c.Fail() <- e:
		result = <-c.Done()
	}

	assert.Equal(t, e, result.Failure)
	assert.Equal(t, []int{1}, result.Transitions)
}
