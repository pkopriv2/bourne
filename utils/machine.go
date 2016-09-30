package utils

import "sync"

// This class serves to simplify much of the concurrency issues that arise
// in objects that have complex lifecycles.  In objects that require more
// than a simple "on/off" switch, it is very natural to manage the behavior
// through the use of Finite-State-Automata.  Each state in the automata
// may have very different concurrency models.  Any shared state must be
// be managed with extreme care.  This allows authors to bind each separate
// concurrency model with a particule state in the lifecycle.

const (
	InitialState  = -1
	TerminalState = -2
	FailureState  = -3
)

// func BuildStateMachine() StateMachineFactory {
// return &stateMachineFactory{make(map[int]*state)}
// }

// A worker is the runtime implementation of a particular state in the state machine.
type Worker func(Controller, []interface{})

type Transition struct {
	Target int
	Args   []interface{}
}

func Terminal() Transition {
	return Next(TerminalState)
}

func Fail(e error) Transition {
	return Next(FailureState, e)
}

func Next(target int, args ...interface{}) Transition {
	return Transition{target, args}
}

type Controller interface {
	Close() <-chan struct{}
	Transition() chan<- Transition
	Next(int, ...interface{}) bool
	Fail(error) bool
}

type StateController interface {
	Wait() <-chan Transition
	Transition() chan<- Transition
}

type MachineController interface {
	Current() int
	Summary() []Transition
	Wait() <-chan error
	Transition() chan<- Transition
}

type StateMachineFactory interface {
	AddState(int, ...Worker) StateMachineFactory
	Start(int, ...interface{}) StateMachine
}

type StateMachine interface {
	Control() MachineController
}

type wait struct {
	inner sync.WaitGroup
}

func (w *wait) Done() {
	w.inner.Done()
}

func (w *wait) Add() {
	w.inner.Add(1)
}

func (w *wait) Wait() <-chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		w.inner.Wait()
		done <- struct{}{}
	}()
	return done
}

type workerController struct {
	sharedWait       *wait
	sharedTransition chan<- Transition

	close chan struct{}
}

func newWorkerController(wait *wait, result chan<- Transition) *workerController {
	return &workerController{wait, result, make(chan struct{}, 1)}
}

func (w *workerController) Done() {
	w.sharedWait.Done()
}

func (w *workerController) Close() <-chan struct{} {
	return w.close
}

func (w *workerController) Transition() chan<- Transition {
	return w.sharedTransition
}

func (w *workerController) Next(target int, args ...interface{}) bool {
	select {
	case <-w.close:
		return false
	case w.sharedTransition <- Transition{target, args}:
		return true
	}
}

func (w *workerController) Fail(e error) bool {
	select {
	case <-w.close:
		return false
	case w.sharedTransition <- Transition{FailureState, []interface{}{e}}:
		return true
	}
}

type stateController struct {
	wait      *wait
	resultIn  chan Transition
	resultOut chan Transition
	workers   []*workerController
}

func newStateController() *stateController {
	s := &stateController{&wait{},
		make(chan Transition),
		make(chan Transition, 1),
		make([]*workerController, 0, 10)}

	go func() {
		var result Transition
		select {
		case <-s.wait.Wait():
			s.resultOut <- Transition{TerminalState, nil}
			return
		case result = <-s.resultIn:
		}

		// broadcast the result
		for _, w := range s.workers {
			w.close <- struct{}{}
		}

		// wait for the workers to close
		<-s.wait.Wait()

		// finally, send the result out
		s.resultOut <- result
	}()

	return s
}

func (s *stateController) Spawn() *workerController {
	defer s.wait.Add()

	worker := newWorkerController(s.wait, s.resultIn)
	s.workers = append(s.workers, worker)
	return worker
}

func (s *stateController) Wait() <-chan Transition {
	return s.resultOut
}

func (s *stateController) Transition() chan<- Transition {
	return s.resultIn
}

type state struct {
	id      int
	workers []Worker
}

func newState(id int, workers ...Worker) *state {
	return &state{id, workers}
}

func (s *state) run(args []interface{}) *stateController {
	controller := newStateController()

	for _, w := range s.workers {
		worker := w
		child := controller.Spawn()
		go func() {
			defer child.Done()
			worker(child, args)
		}()
	}

	return controller
}

type history struct {
	lock sync.Mutex
	transitions []Transition
}

func (h* history) Get() []Transition {
	h.lock.Lock()
	defer h.lock.Unlock()

	ret := make([]Transition, len(h.transitions))
	copy(ret, h.transitions)
	return ret
}


func (h* history) Append(t Transition) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.transitions = append(h.transitions, t)
}

type machineController struct {
	history *history
	next    chan<- Transition
	done    chan []Transition
}

func newMachineController(history *history, next chan<- Transition) *machineController {
	return &machineController{history, next, make(chan []Transition, 1)}
}

func (m *machineController) Current() int {
	summary := m.Summary()

	len := len(summary)
	if len == 0 {
		return TerminalState
	}

	return summary[len-1].Target
}

func (m *machineController) Summary() []Transition {
	return m.history.Get()
}

func (m *machineController) Wait() <-chan []Transition {
	return m.done
}

func (m *machineController) Transition() chan<- Transition {
	return m.next
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

type stateMachine struct {
	lock        sync.Mutex
	states      map[int]*state
	history     *history
	controllers []*machineController
	result      chan Transition
}

func newStateMachine(states map[int]*state, init int, args []interface{}) *stateMachine {
	s := &stateMachine{
		states:      states,
		history:     &history{transitions: make([]Transition, 0, 1)},
		controllers: make([]*machineController,0,1),
		result:      make(chan Transition)}

	go func() {
		cur, ok := s.states[init]
		if !ok {
			s.history.Append(Fail(fmt.Errorf("Could not start machine. State [%v] does not exist", init)))

			m.root.broadcast(ret)
			m.root.done <- ret
			return
		}

	}()

	return s
}

func (s *stateMachine) Control() MachineController {
	s.lock.Lock()
	defer s.lock.UnLock()

	ret := newMachineController(s.history, s.result)
	s.controllers = append(s.controllers, ret)
	return ret
}

//
// cur, ok := m.states[init]
// if !ok {
// ret := Summary{transitions, fmt.Errorf("Could not start machine. State [%v] does not exist", init)}
// m.root.broadcast(ret)
// m.root.done <- ret
// return
// }
//
// transition := Transition{init, args}
// transitions = append(transitions, transition)
//
// for {
// control := cur.Run(transition.Args)
//
// // determine result of current worker(s).
// var result Transition
// select {
// case target := <-m.root.next:
// result = control.Transition(target.Target, target.Args...)
// result = Transition{&target, result.Failure}
// case failure := <-m.root.fail:
// control.Fail(failure)
// result = Transition{nil, failure}
// case result = <-control.Close():
// }
//
// // handle: terminal state/failure
// if result.Transition == nil || result.Failure != nil {
// ret := Summary{transitions, result.Failure}
// m.root.broadcast(ret)
// m.root.done <- ret
// return
// }
//
// // handle: transition
// cur, ok = m.states[result.Transition.Target]
// if !ok {
// ret := Summary{transitions, fmt.Errorf("Illegal state transition. Target does not exist [%v]", result.Transition.Target)}
// m.root.broadcast(ret)
// m.root.done <- ret
// return
// }
//
// transition = *result.Transition
// transitions = append(transitions, transition)
// }
// }()
//
// return m
// }
//
// func (s *stateMachine) Control() (OperatorController, error) {
// return s.root.spawn()
// }
//
// func CombineTriggers(triggers []chan bool) <-chan bool {
// ret := make(chan bool)
// go func() {
// for _, t := range triggers {
// <-t
// }
//
// ret <- true
// }()
// return ret
// }
