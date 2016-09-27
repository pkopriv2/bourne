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
const (
	TerminalState = -1
)

//
func BuildStateMachine() StateMachineFactory {
	return &stateMachineFactory{make(map[int]*state)}
}

// A worker is the runtime implementation of a particular state in the
// state machine.  Each worker is maintained in its own go routine.
// Each routine is free to create additional routines as necessary, but it
// is their repsonsibity to ensure there are no leaks
type Worker func(StateController)

// A state controller allows both workers and operators the ability to
// to influence the state machine.
//
// Instances should only be used by a single routine.
type StateController interface {

	// Signals to the worker that it has been requested to die.  The worker
	// should perform any necessary cleanup and return.
	Done() <-chan StateResult

	// Signals to the stateController that the machine should transition to the given state.
	// This will cause an item to be put on the stateController's done channel, giving it a
	// chance to perform an necessary cleanup and die.
	Next(int) StateResult

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail(error) StateResult
}

// A state controller allows both workers and operators the ability to
// to influence the state machine.
//
// Instances should only be used by a single routine.
type MachineController interface {
	Done() <-chan MachineResult

	// Signals to the stateController that the machine should transition to the given state.
	// This will cause an item to be put on the stateController's done channel, giving it a
	// chance to perform an necessary cleanup and die.
	Transition(int) MachineResult

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail(error) MachineResult
}

// The result of a completed state within the state machine.
type StateResult struct {
	Target  int
	Failure error
}

// The result of a completed state machine
type MachineResult struct {
	Transitions []int
	Failure     error
}

// A simple factory interface for creating state machines.
type StateMachineFactory interface {
	AddState(int, ...Worker) StateMachineFactory
	Start(int) StateMachine
}

// A running state machine.
type StateMachine interface {

	// Returns a new controller for the machine.
	Control() (MachineController, error)
}

type stateController struct {
	lock sync.Mutex
	dead bool

	next chan int
	fail chan error
	done chan StateResult

	children []*stateController
}

func newRootController() *stateController {
	return &stateController{
		next:     make(chan int),
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

func (m *stateController) Next(t int) StateResult {
	select {
	case m.next<-t:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

func (m *stateController) Fail(e error) StateResult {
	select {
	case m.fail<-e:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

// A state is a suspended worker.
type state struct {
	id     int
	workers []Worker
}

func newState(id int, workers ...Worker) *state {
	return &state{id, workers}
}

func (s *state) Id() int {
	return s.id
}

func (s *state) Run() *stateController {
	root := newRootController()

	triggers := make([]chan bool, 0)
	for _,w := range s.workers {
		worker := w
		trigger := make(chan bool)
		triggers = append(triggers, trigger)
		child,_ := root.spawn()
		go func() {
			worker(child)
			trigger<-true
		}()
	}

	ret := CombineTriggers(triggers)
	go func() {
		var result StateResult
		var returned bool

		select {
		case returned = <-ret:
			result = StateResult{TerminalState, nil}
		case target := <-root.next:
			result = StateResult{target, nil}
		case failure := <-root.fail:
			result = StateResult{TerminalState, failure}
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

func (s *stateMachineFactory) Start(init int) StateMachine {
	cop := make(map[int]*state)
	for k, v := range s.states {
		cop[k] = v
	}

	return newStateMachine(cop, init)
}

type machineController struct {
	lock sync.Mutex
	dead bool

	curr chan int
	next chan int
	fail chan error
	done chan MachineResult

	children []*machineController
}

func newRootMachineController() *machineController {
	return &machineController{
		curr:     make(chan int),
		next:     make(chan int),
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

func (m *machineController) Transition(t int) MachineResult {
	select {
	case m.next<-t:
		return <-m.done
	case result := <-m.done:
		return result
	}
}

func (m *machineController) Fail(e error) MachineResult {
	select {
	case m.fail<-e:
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

func newStateMachine(states map[int]*state, init int) StateMachine {
	m := &stateMachine{
		states: states,
		root:   newRootMachineController()}

	m.wait.Add(1)
	go func() {
		defer m.wait.Done()

		transitions := make([]int, 0, 1)

		cur, ok := m.states[init]
		if !ok {
			ret := MachineResult{transitions, fmt.Errorf("Could not start machine. State [%v] does not exist", init)}
			m.root.broadcast(ret)
			m.root.done <- ret
			return
		}

		transitions = append(transitions, init)

		for {
			control := cur.Run()

			// determine result of current worker(s).
			var result StateResult
			select {
			case target := <-m.root.next:
				result = control.Next(target)
				result = StateResult{target, result.Failure}
			case failure := <-m.root.fail:
				control.Fail(failure)
				result = StateResult{TerminalState, failure}
			case result = <-control.Done():
			}

			// handle: terminal state/failure
			if result.Target == TerminalState || result.Failure != nil {
				ret := MachineResult{transitions, result.Failure}
				m.root.broadcast(ret)
				m.root.done <- ret
				return
			}

			// handle: transition
			cur, ok = m.states[result.Target]
			if !ok {
				ret := MachineResult{transitions, fmt.Errorf("Illegal state transition. Target does not exist [%v]", result.Target)}
				m.root.broadcast(ret)
				m.root.done <- ret
				return
			}

			transitions = append(transitions, result.Target)
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
		for _,t := range triggers {
			<-t
		}

		ret<-true
	}()
	return ret
}
