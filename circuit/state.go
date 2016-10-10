package circuit

import (
	"fmt"

	"github.com/pkopriv2/bourne/concurrent"
)

const (
	TerminalState = -1
	FailureState  = -2
)

// This class serves to simplify much of the concurrency issues that arise
// in objects that have complex lifecycles.  In objects that require more
// than a simple "on/off" switch, it is very natural to manage the behavior
// through the use of Finite-State-Automata.  Each state in the automata
// may have very different concurrency models.  Any shared state must be
// be managed with extreme care.  This allows authors to bind each separate
// concurrency model with a particule state in the lifecycle.

type Worker func(c WorkerSocket, args []interface{})

func NewStateMachine() StateMachineBuilder {
	return NewStateMachineBuilder()
}

func To(target int, args ...interface{}) Transition {
	return &transition{target, args}
}

func ToTerminal() Transition {
	return To(TerminalState)
}

func ToFailure(e error) Transition {
	return To(FailureState, e)
}

func IsFailure(s Transition) bool {
	return s.Target() == FailureState
}

func IsTerminal(s Transition) bool {
	return s.Target() == TerminalState
}

func Latest(history []Transition) Transition {
	return extractLatest(history)
}

func Failure(s Transition) error {
	return extractFailure(s)
}

func Terminate(s StateMachine) {
	select {
	case <-s.Closed():
		return
	case s.Update() <- ToTerminal():
	}

	<-s.Closed()
}

type Transition interface {
	Target() int
	Args() []interface{}
}

type StateMachineBuilder interface {
	AddState(int, ...Worker) StateMachineBuilder
	Start(int, ...interface{}) StateMachine
}

type StateMachine interface {
	History() []Transition
	Closed() <-chan struct{}
	Update() chan<- Transition
}

type WorkerSocket interface {
	Closed() <-chan struct{}
	Next(int, ...interface{})
	Fail(error)
	Done()
}

// Implementations

type transition struct {
	id   int
	args []interface{}
}

func (s *transition) Target() int {
	return s.id
}

func (s *transition) Args() []interface{} {
	return s.args
}

type workerSocket struct {
	parent *stageController
}

func newWorkerSocket(parent *stageController) *workerSocket {
	parent.wait.Add()
	return &workerSocket{parent}
}

func (w *workerSocket) Done() {
	w.parent.wait.Done()
}

func (w *workerSocket) Closed() <-chan struct{} {
	return w.parent.closed
}

func (w *workerSocket) Fail(e error) {
	select {
	case <-w.parent.closed:
		return
	case w.parent.resultIn <- ToFailure(e):
		return
	}
}

func (w *workerSocket) Next(target int, args ...interface{}) {
	select {
	case <-w.parent.closed:
		return
	case w.parent.resultIn <- To(target, args):
		return
	}
}

type stageController struct {
	wait      concurrent.Wait
	closed    chan struct{}
	resultIn  chan Transition
	resultOut chan Transition
}

func newStageController() *stageController {
	s := &stageController{
		concurrent.NewWait(),
		make(chan struct{}),
		make(chan Transition),
		make(chan Transition, 1)}

	go controlStage(s)
	return s
}

func (s *stageController) NewWorkerSocket() *workerSocket {
	return newWorkerSocket(s)
}

func (s *stageController) Result() <-chan Transition {
	return s.resultOut
}

func (s *stageController) Transition() chan<- Transition {
	return s.resultIn
}

func controlStage(s *stageController) {
	var result Transition
	select {
	case <-s.wait.Wait():
		s.resultOut <- ToTerminal()
		return
	case result = <-s.resultIn:
	}

	close(s.closed)
	<-s.wait.Wait()
	s.resultOut <- result
}

type stage struct {
	id      int
	workers []Worker
}

func newState(id int, workers []Worker) *stage {
	return &stage{id, workers}
}

func (s *stage) run(args []interface{}) *stageController {
	controller := newStageController()

	for _, w := range s.workers {
		go w(controller.NewWorkerSocket(), args)
	}

	return controller
}

type stateMachineBuilder struct {
	states map[int]*stage
}

func NewStateMachineBuilder() StateMachineBuilder {
	return &stateMachineBuilder{make(map[int]*stage)}
}

func (s *stateMachineBuilder) AddState(id int, fn ...Worker) StateMachineBuilder {
	s.states[id] = newState(id, fn)
	return s
}

func (s *stateMachineBuilder) Start(init int, args ...interface{}) StateMachine {
	cop := make(map[int]*stage)
	for k, v := range s.states {
		cop[k] = v
	}

	return newStateMachine(cop, init, args)
}

type stateMachine struct {
	states  map[int]*stage
	closed  chan struct{}
	update  chan Transition
	history concurrent.List
}

func newStateMachine(states map[int]*stage, init int, args []interface{}) StateMachine {
	s := &stateMachine{
		states:  states,
		closed:  make(chan struct{}),
		update:  make(chan Transition),
		history: concurrent.NewList(10)}

	go controlStateMachine(s, init, args)
	return s
}

func (s *stateMachine) History() []Transition {
	return toTransitions(s.history.All())
}

func (s *stateMachine) Closed() <-chan struct{} {
	return s.closed
}

func (s *stateMachine) Update() chan<- Transition {
	return s.update
}

func controlStateMachine(s *stateMachine, init int, args []interface{}) {
	cur := s.states[init]
	if cur == nil {
		s.history.Append(ToFailure(fmt.Errorf("Could not start machine. State [%v] does not exist", init)))
		close(s.closed)
	}

	s.history.Append(To(init, args...))
	for {
		control := cur.run(args)

		var next Transition
		select {
		case next = <-control.Result():
		case next = <-s.update:
			select {
			case tmp := <-control.Result():
				if IsFailure(tmp) {
					next = tmp
				}
			case control.Transition() <- next:
				tmp := <-control.Result()
				if IsFailure(tmp) {
					next = tmp
				}
			}
		}

		if IsFailure(next) || IsTerminal(next) {
			s.history.Append(next)
			close(s.closed)
			return
		}

		cur = s.states[next.Target()]
		if cur == nil {
			s.history.Append(ToFailure(fmt.Errorf("State [%v] does not exist", next.Target())))
			close(s.closed)
			return
		}

		s.history.Append(next)
	}
}

func toTransitions(raw []interface{}) []Transition {
	ret := make([]Transition, len(raw))
	for i, v := range raw {
		ret[i] = v.(Transition)
	}

	return ret
}

func extractLatest(transitions []Transition) Transition {
	if len(transitions) == 0 {
		return nil
	}

	return transitions[len(transitions)-1]
}

func extractFailure(latest Transition) error {
	if latest.Target() != FailureState {
		return nil
	}

	return latest.Args()[0].(error)
}
