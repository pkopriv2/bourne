package utils

import (
	"fmt"
	"sync"
)

// This class serves to simplify much of the concurrency issues that arise
// in objects that have complex lifecycles.  In objects that require more
// than a simple "on/off" switch, it is very natural to manage the behavior
// through the use of Finite-State-Automata.  Each state in the automata
// may have very different concurrency models.  Any shared state must be
// be managed with extreme care.  This allows authors to bind each separate
// concurrency model with a particule state in the lifecycle.

// Return
func BuildStateMachine() StateMachineFactory {
	return &stateMachineFactory{make(map[int]*state)}
}

// A worker is the runtime implementation of a particular state in the
// state machine.  Each worker is maintained in its own go routine.
// Each routine is free to create additional routines as necessary, but it
// is their repsonsibity to ensure there are no leaks
type Worker func(StateController, []interface{})

type StateController interface {

	// Signals to the worker that it has been requested to die.  The worker
	// should perform any necessary cleanup and return.
	Done() <-chan StateResult

	// Signals to the stateController that the machine should transition to the given state.
	// This will cause an item to be put on the stateController's done channel, giving it a
	// chance to perform an necessary cleanup and die.
	Next(int, ...interface{}) StateResult

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail(error) StateResult
}

type MachineController interface {

	// Signals to the worker that it has been requested to die.  The worker
	// should perform any necessary cleanup and return.
	Done() <-chan MachineResult

	// Signals to the stateController that the machine should transition to the given state.
	// This will cause an item to be put on the stateController's done channel, giving it a
	// chance to perform an necessary cleanup and die.
	Transition(int, ...interface{}) MachineResult

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail(error) MachineResult
}

type StateResult struct {
	Transition *Transition
	Failure    error
}

type Transition struct {
	Target int
	Args   []interface{}
}

type MachineResult struct {
	Transitions []Transition
	Failure     error
}

type StateMachineFactory interface {
	AddState(int, ...Worker) StateMachineFactory
	Start(int, ...interface{}) StateMachine
}

type StateMachine interface {
	Control() (MachineController, error)
}

type stateController struct {
	lock sync.Mutex
	dead bool

	next chan Transition
	fail chan error
	done chan StateResult

	children []*stateController
}

func newRootController() *stateController {
	return &stateController{
		next:     make(chan Transition),
		fail:     make(chan error),
		done:     make(chan StateResult, 1),
		children: make([]*stateController, 0, 1)}
}

func newChildController(parent *stateController) *stateController {
	return &stateController{
		next:     parent.next,
		fail:     parent.fail,
		done:     make(chan StateResult, 1),
		children: make([]*stateController, 0, 1)}
}

func (m *stateController) spawn() (StateController, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.dead {
		return nil, fmt.Errorf("Cannot spawn a child stateController from a dead parent")
	}

	child := newChildController(m)
	m.children = append(m.children, child)
	return child, nil
}

func (m *stateController) broadcast(result StateResult) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.dead {
		return fmt.Errorf("Cannot broadcast from a dead stateController")
	}

	for _, c := range m.children {
		c.done <- result
	}

	m.dead = true
	return nil
}

func (m *stateController) Done() <-chan StateResult {
	return m.done
}

func (m *stateController) Next(target int, args ...interface{}) StateResult {
	select {
	case m.next <- Transition{target, args}:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

func (m *stateController) Fail(e error) StateResult {
	select {
	case m.fail <- e:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

type state struct {
	id      int
	workers []Worker
}

func newState(id int, workers ...Worker) *state {
	return &state{id, workers}
}

func (s *state) Id() int {
	return s.id
}

func (s *state) Run(args []interface{}) *stateController {
	root := newRootController()

	triggers := make([]chan bool, 0)
	for _, w := range s.workers {
		worker := w
		trigger := make(chan bool)
		triggers = append(triggers, trigger)
		child, _ := root.spawn()
		go func() {
			worker(child, args)
			trigger <- true
		}()
	}

	ret := CombineTriggers(triggers)
	go func() {
		var result StateResult
		var returned bool

		select {
		case returned = <-ret:
			result = StateResult{nil, nil}
		case target := <-root.next:
			result = StateResult{&target, nil}
		case failure := <-root.fail:
			result = StateResult{nil, failure}
		}

		root.broadcast(result)
		if !returned {
			<-ret
		}

		root.done <- result
	}()

	return root
}

type stateMachineFactory struct {
	states map[int]*state
}

func (s *stateMachineFactory) AddState(id int, fn ...Worker) StateMachineFactory {
	s.states[id] = newState(id, fn...)
	return s
}

func (s *stateMachineFactory) Start(init int, args ...interface{}) StateMachine {
	cop := make(map[int]*state)
	for k, v := range s.states {
		cop[k] = v
	}

	return newStateMachine(cop, init, args)
}

type machineController struct {
	lock sync.Mutex
	dead bool

	curr chan int
	next chan Transition
	fail chan error
	done chan MachineResult

	children []*machineController
}

func newRootMachineController() *machineController {
	return &machineController{
		curr:     make(chan int),
		next:     make(chan Transition),
		fail:     make(chan error),
		done:     make(chan MachineResult, 1),
		children: make([]*machineController, 0, 1)}
}

func newChildMachineController(parent *machineController) *machineController {
	return &machineController{
		next:     parent.next,
		fail:     parent.fail,
		done:     make(chan MachineResult, 1),
		children: make([]*machineController, 0, 1)}
}

func (m *machineController) spawn() (MachineController, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.dead {
		return nil, fmt.Errorf("Cannot spawn a child controller from a dead parent")
	}

	child := newChildMachineController(m)
	m.children = append(m.children, child)
	return child, nil
}

func (m *machineController) broadcast(result MachineResult) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.dead {
		return fmt.Errorf("Cannot broadcast from a dead controller")
	}

	for _, c := range m.children {
		c.done <- result
	}

	m.dead = true
	return nil
}

func (m *machineController) Done() <-chan MachineResult {
	return m.done
}

func (m *machineController) Transition(t int, args ...interface{}) MachineResult {
	select {
	case m.next <- Transition{t, args}:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

func (m *machineController) Fail(e error) MachineResult {
	select {
	case m.fail <- e:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

type stateMachine struct {
	wait   sync.WaitGroup
	states map[int]*state
	root   *machineController
}

func newStateMachine(states map[int]*state, init int, args []interface{}) StateMachine {
	m := &stateMachine{
		states: states,
		root:   newRootMachineController()}

	m.wait.Add(1)
	go func() {
		defer m.wait.Done()

		transitions := make([]Transition, 0, 1)

		cur, ok := m.states[init]
		if !ok {
			ret := MachineResult{transitions, fmt.Errorf("Could not start machine. State [%v] does not exist", init)}
			m.root.broadcast(ret)
			m.root.done <- ret
			return
		}

		transition := Transition{init, args}
		transitions = append(transitions, transition)

		for {
			control := cur.Run(transition.Args)

			// determine result of current worker(s).
			var result StateResult
			select {
			case target := <-m.root.next:
				result = control.Next(target.Target, target.Args...)
				result = StateResult{&target, result.Failure}
			case failure := <-m.root.fail:
				control.Fail(failure)
				result = StateResult{nil, failure}
			case result = <-control.Done():
			}

			// handle: terminal state/failure
			if result.Transition == nil || result.Failure != nil {
				ret := MachineResult{transitions, result.Failure}
				m.root.broadcast(ret)
				m.root.done <- ret
				return
			}

			// handle: transition
			cur, ok = m.states[result.Transition.Target]
			if !ok {
				ret := MachineResult{transitions, fmt.Errorf("Illegal state transition. Target does not exist [%v]", result.Transition.Target)}
				m.root.broadcast(ret)
				m.root.done <- ret
				return
			}

			transition = *result.Transition
			transitions = append(transitions, transition)
		}
	}()

	return m
}

func (s *stateMachine) Control() (MachineController, error) {
	return s.root.spawn()
}

func CombineTriggers(triggers []chan bool) <-chan bool {
	ret := make(chan bool)
	go func() {
		for _, t := range triggers {
			<-t
		}

		ret <- true
	}()
	return ret
}
