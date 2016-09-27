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
	Next() chan<- int

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail() chan<- error
}

// A state controller allows both workers and operators the ability to
// to influence the state machine.
//
// Instances should only be used by a single routine.
type MachineController interface {

	// Signals to the consumer
	Done() <-chan MachineResult

	// Signals to the stateController that the machine should transition to the given state.
	// This will cause an item to be put on the stateController's done channel, giving it a
	// chance to perform an necessary cleanup and die.
	Next() chan<- int

	// Signals to the stateController that an irrecoverable error has occured in a worker.
	// This will cause an item to be put on the stateController's done channel, meaning
	// This puts the machine into a terminally failed state.
	Fail() chan<- error
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
	AddState(int, Worker) StateMachineFactory
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

func (m *stateController) Next() chan<- int {
	return m.next
}

func (m *stateController) Fail() chan<- error {
	return m.fail
}

// a state is basically a suspended worker.
type state struct {
	id     int
	worker Worker
}

func newState(id int, worker Worker) *state {
	return &state{id, worker}
}

func (s *state) Id() int {
	return s.id
}

func (s *state) Run() StateController {
	root := newRootController()
	child, _ := root.spawn()

	ret := make(chan bool)
	go func() {
		s.worker(child)
		ret <- true
	}()

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

func (s *stateMachineFactory) AddState(id int, fn Worker) StateMachineFactory {
	s.states[id] = newState(id, fn)
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

	next chan int
	fail chan error
	done chan MachineResult

	children []*machineController
}

func newRootMachineController() *machineController {
	return &machineController{
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

func (m *machineController) Next() chan<- int {
	return m.next
}

func (m *machineController) Fail() chan<- error {
	return m.fail
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

	// Attempts to force the currently running state to transition to the target state.
	// Any errors returned by the stateController will override the transition
	next := func(c StateController, target int) StateResult {
		ret := StateResult{target, nil}
		select {
		case c.Next() <- target:
			<-c.Done()
		case result := <-c.Done():
			if result.Failure != nil {
				return result
			}
		}

		return ret
	}

	// Forces the current stateController to fail.  (even if it actually succeeded)
	fail := func(c StateController, failure error) StateResult {
		select {
		case <-c.Done():
		case c.Fail() <- failure:
			<-c.Done()
		}

		return StateResult{TerminalState, failure}
	}

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
				result = next(control, target)
			case failure := <-m.root.fail:
				result = fail(control, failure)
			case result = <-control.Done():
			}

			// handle: terminal state/failure
			if result.Target == TerminalState || result.Failure != nil {
				ret := MachineResult{transitions, result.Failure}
				m.root.broadcast(ret)
				m.root.done <- ret
				return
			}

			// handle: normal transition
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
