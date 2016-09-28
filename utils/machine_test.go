package utils

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestState_noResult(t *testing.T) {
	state := newState(1, func(c StateController, args []interface{}) {})

	master := state.Run([]interface{}{})

	result := <-master.Done()
	assert.Nil(t, result.Transition)
	assert.Nil(t, result.Failure)
}

func TestState_workerfail(t *testing.T) {
	e := errors.New("error")

	state := newState(1, func(c StateController, args []interface{}) {
		c.Fail(e)
	})

	controller := state.Run([]interface{}{})
	result := <-controller.Done()

	assert.Nil(t, result.Transition)
	assert.Equal(t, e, result.Failure)
}

func TestState_externalFail(t *testing.T) {
	e := errors.New("error")

	returned := false
	state := newState(1, func(c StateController, args []interface{}) {
		<-c.Done()
		returned = true
	})

	controller := state.Run([]interface{}{})
	result := controller.Fail(e)

	assert.Nil(t, result.Transition)
	assert.Equal(t, e, result.Failure)
	assert.True(t, returned)
}

func TestState_multiWorker(t *testing.T) {
	e := errors.New("error")

	returned1 := false
	worker1 := func(c StateController, args []interface{}) {
		c.Fail(e)
		returned1 = true
	}

	returned2 := false
	worker2 := func(c StateController, args []interface{}) {
		time.Sleep(1 * time.Second)
		returned2 = true
	}

	returned3 := false
	worker3 := func(c StateController, args []interface{}) {
		time.Sleep(1 * time.Second)
		returned3 = true
	}

	state := newState(1, worker1, worker2, worker3)

	controller := state.Run([]interface{}{})
	result := <-controller.Done()

	assert.Nil(t, result.Transition)
	assert.Equal(t, e, result.Failure)
	assert.True(t, returned1)
	assert.True(t, returned2)
	assert.True(t, returned3)
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
	factory.AddState(1, func(c StateController, args []interface{}) {
		c.Next(2)
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.NotNil(t, result.Failure)
}

func TestStateMachine_SingleState(t *testing.T) {
	factory := BuildStateMachine()
	factory.AddState(1, func(c StateController, args []interface{}) {})

	machine := factory.Start(1, "args")

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.Nil(t, result.Failure)
	assert.Equal(t, []Transition{Transition{1, []interface{}{"args"}}}, result.Transitions)
}

func TestStateMachine_MultiState(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController, args []interface{}) {
		c.Next(2, "2")
	})
	factory.AddState(2, func(c StateController, args []interface{}) {
		c.Next(3, "3")
	})
	factory.AddState(3, func(c StateController, args []interface{}) {
	})

	machine := factory.Start(1, "1")

	c, err := machine.Control()
	assert.Nil(t, err)

	result := <-c.Done()
	assert.Nil(t, result.Failure)
	assert.Equal(t, []Transition{
		Transition{1, []interface{}{"1"}},
		Transition{2, []interface{}{"2"}},
		Transition{3, []interface{}{"3"}}}, result.Transitions)
}

func TestStateMachine_ExternalTransition(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController, args []interface{}) {
		time.Sleep(100 * time.Millisecond)
		c.Next(2)
	})
	factory.AddState(2, func(c StateController, args []interface{}) {
		c.Next(3)
	})
	factory.AddState(3, func(c StateController, args []interface{}) {
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	// skip 2
	result := c.Transition(3)
	assert.Nil(t, result.Failure)
	assert.Equal(t, []Transition{
		Transition{1, nil},
		Transition{3, nil}}, result.Transitions)
}

func TestStateMachine_ExternalFailure(t *testing.T) {
	factory := BuildStateMachine()

	factory.AddState(1, func(c StateController, args []interface{}) {
		time.Sleep(100 * time.Millisecond)
		c.Next(2)
	})
	factory.AddState(2, func(c StateController, args []interface{}) {
		c.Next(3)
	})
	factory.AddState(3, func(c StateController, args []interface{}) {
	})

	machine := factory.Start(1)

	c, err := machine.Control()
	assert.Nil(t, err)

	// fail
	e := errors.New("error")
	result := c.Fail(e)
	assert.Equal(t, e, result.Failure)
	assert.Equal(t, []Transition{Transition{1, nil}}, result.Transitions)
}
