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
	controller.Transition() <- Failure(e)

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

	// should just close...
	<-machine.Closed()
}

func TestStateMachine_IllegalTransition(t *testing.T) {
	builder := NewStateMachine()
	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		c.Next(2)
	})

	machine := builder.Start(1)

	<-machine.Closed()
	assert.Error(t, ExtractFailure(machine))
}

func TestStateMachine_SingleState(t *testing.T) {
	builder := NewStateMachine()
	builder.AddState(1, func(c WorkerSocket, args []interface{}) {})

	machine := builder.Start(1, "args")

	<-machine.Closed()

	assert.Equal(t, Transition(1, "args"), ExtractNth(machine, 0))
	assert.Equal(t, Terminal(), ExtractNth(machine, 1))
}

func TestStateMachine_MultiState(t *testing.T) {
	builder := NewStateMachine()

	builder.AddState(1, func(c WorkerSocket, args []interface{}) {
		c.Next(2, "2")
	})
	builder.AddState(2, func(c WorkerSocket, args []interface{}) {
		c.Next(3, "3")
	})
	builder.AddState(3, func(c WorkerSocket, args []interface{}) {
	})

	machine := builder.Start(1, "1")
	<-machine.Closed()

	assert.Equal(t, Transition(1, "1"), ExtractNth(machine, 0))
	assert.Equal(t, Transition(2, "2"), ExtractNth(machine, 1))
	assert.Equal(t, Transition(3, "3"), ExtractNth(machine, 2))
	assert.Equal(t, Terminal(), ExtractNth(machine, 3))
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
	assert.True(t, Update(machine, 3))

	assert.Equal(t, Transition(1), ExtractNth(machine, 0))
	// assert.Equal(t, TransitionTo(3), ExtractNth(machine, 1))
	// assert.Equal(t, TransitionToTerminal(), ExtractNth(machine, 2))
}

//
// func TestStateMachine_ExternalFailure(t *testing.T) {
// builder := BuildStateMachine()
//
// builder.AddState(1, func(c WorkerSocket, args []interface{}) {
// time.Sleep(100 * time.Millisecond)
// c.Next(2)
// })
// builder.AddState(2, func(c WorkerSocket, args []interface{}) {
// c.Next(3)
// })
// builder.AddState(3, func(c WorkerSocket, args []interface{}) {
// })
//
// machine := builder.Start(1)
//
// c := machine.Control()
//
// // fail
// e := errors.New("error")
// select {
// case <-c.Wait():
// t.Fail()
// return
// case c.Transition() <- Fail(e):
// }
//
// assert.Equal(t, e, <-c.Wait())
// }
