package machine

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStage_NoTransition(t *testing.T) {
	state := newStage(1, func(c WorkerSocket, args []interface{}) {})

	controller := state.run([]interface{}{})

	result := <-controller.Result()
	assert.Equal(t, TerminalState, result.Id())
	assert.Nil(t, result.Args())
}

func TestStage_Workerfail(t *testing.T) {
	e := errors.New("error")

	state := newStage(1, func(c WorkerSocket, args []interface{}) {
		c.Fail(e)
	})

	controller := state.run([]interface{}{})
	result := <-controller.Result()

	assert.Equal(t, FailureState, result.Id())
	assert.Equal(t, []interface{}{e}, result.Args())
}

func TestStage_ExternalFail(t *testing.T) {
	var wait sync.WaitGroup

	wait.Add(1)
	state := newStage(1, func(c WorkerSocket, args []interface{}) {
		<-c.Closed()
		wait.Done()
	})

	e := errors.New("error")

	controller := state.run([]interface{}{})
	controller.Transition() <- NewFailureState(e)

	wait.Wait()

	result := <-controller.Result()
	assert.Equal(t, FailureState, result.Id())
	assert.Equal(t, []interface{}{e}, result.Args())
}

func TestStage_MultiWorker(t *testing.T) {
	e := errors.New("error")

	var wait sync.WaitGroup

	wait.Add(1)
	worker1 := func(c WorkerSocket, args []interface{}) {
		c.Fail(e)
		wait.Done()
	}

	wait.Add(1)
	worker2 := func(c WorkerSocket, args []interface{}) {
		time.Sleep(1 * time.Second)
		wait.Done()
	}

	wait.Add(1)
	worker3 := func(c WorkerSocket, args []interface{}) {
		time.Sleep(1 * time.Second)
		wait.Done()
	}

	state := newStage(1, worker1, worker2, worker3)

	controller := state.run([]interface{}{})
	result := <-controller.Result()

	wait.Wait()

	assert.Equal(t, e, extractFailure(result))
}

func TestStateMachine_Empty(t *testing.T) {
	builder := NewStateMachine()
	machine := builder.Start(1)

	// should just close...with an error!
	assert.NotNil(t, machine.Wait())
}

func TestStateMachine_IllegalTransition(t *testing.T) {
	builder := NewStateMachine()
	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		c.Next(2)
	})

	machine := builder.Start(1)
	assert.NotNil(t, machine.Wait())
}

func TestStateMachine_SingleState(t *testing.T) {
	builder := NewStateMachine()
	builder.AddState(1, func(c WorkerSocket, args []interface{}) {})

	machine := builder.Start(1, "args")

	assert.Nil(t, machine.Wait())
	assert.Equal(t, NewState(1, "args"), ExtractNthState(machine, 0))
	assert.Equal(t, NewTerminalState(), ExtractNthState(machine, 1))
}

func TestStateMachine_MultiState(t *testing.T) {
	builder := NewStateMachine()

	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		assert.Equal(t, "1", args[0].(string))
		c.Next(2, "2")
	})
	builder.AddState(2, func(c WorkerSocket, args []interface{}) {
		assert.Equal(t, "2", args[0].(string))
		c.Next(3, "3")
	})
	builder.AddState(3, func(c WorkerSocket, args []interface{}) {
		assert.Equal(t, "3", args[0].(string))
	})

	machine := builder.Start(1, "1")

	assert.Nil(t, machine.Wait())
	assert.Equal(t, NewState(1, "1"), ExtractNthState(machine, 0))
	assert.Equal(t, NewState(2, "2"), ExtractNthState(machine, 1))
	assert.Equal(t, NewState(3, "3"), ExtractNthState(machine, 2))
	assert.Equal(t, NewTerminalState(), ExtractNthState(machine, 3))
}

func TestStateMachine_ExternalTransition(t *testing.T) {
	builder := NewStateMachine()

	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		time.Sleep(100 * time.Millisecond)
		c.Next(2)
	})
	builder.AddState(2, func(c WorkerSocket, args []interface{}) {
		c.Next(3)
	})
	builder.AddState(3, func(c WorkerSocket, args []interface{}) {
	})

	machine := builder.Start(1)
	// susceptible to timing errors...
	assert.Nil(t, machine.Move(3))
	assert.Nil(t, machine.Wait())
	assert.Nil(t, machine.Wait())
	assert.Equal(t, NewState(1), ExtractNthState(machine, 0))
	assert.Equal(t, NewState(3), ExtractNthState(machine, 1))
	assert.Equal(t, NewTerminalState(), ExtractNthState(machine, 2))
}

func TestStateMachine_ExternalFailure(t *testing.T) {
	builder := NewStateMachine()

	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		time.Sleep(100 * time.Millisecond)
		c.Next(2)
	})
	builder.AddState(2, func(c WorkerSocket, args []interface{}) {
		c.Next(3)
	})
	builder.AddState(3, func(c WorkerSocket, args []interface{}) {
	})

	machine := builder.Start(1)

	// fail
	e := errors.New("error")
	machine.Fail(e)
	assert.Equal(t, e, machine.Wait())
}
