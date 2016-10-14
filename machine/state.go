package machine

import (
	"errors"
	"fmt"

	"github.com/pkopriv2/bourne/concurrent"
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
	FailureState  = -2
)

var MachineClosed = errors.New("MACHINE:CLOSED")

type Worker func(c WorkerSocket, args []interface{})

func NewStateMachine() StateMachineBuilder {
	return NewStateMachineBuilder(make(map[int]*stage))
}

func NewState(target int, args ...interface{}) State {
	return &state{target, args}
}

func NewTerminalState() State {
	return NewState(TerminalState)
}

func NewFailureState(e error) State {
	return NewState(FailureState, e)
}

func IsFailureState(t State) bool {
	return t.Id() == FailureState
}

func IsTerminalState(t State) bool {
	return t.Id() == TerminalState
}

func ExtractNthState(machine StateMachine, n int) State {
	return extractNth(machine.History(), n)
}

func Close(m StateMachine) error {
	return m.Close()
}

func Wait(m StateMachine) error {
	return m.Wait()
}

func Fail(m StateMachine, e error) error {
	return m.Fail(e)
}

func Move(m StateMachine, state int, args ...interface{}) error {
	return m.Move(state, args)
}

type State interface {
	Id() int
	Args() []interface{}
}

type WorkerSocket interface {
	Closed() <-chan struct{}
	Next(int, ...interface{})
	Fail(error)
}

type StateMachine interface {
	History() []State
	Move(int, ...interface{}) error
	Wait() error
	Close() error
	Fail(error) error
	Running() bool
	Result() error
}

type StateMachineBuilder interface {
	AddState(int, ...Worker) StateMachineBuilder
	Start(int, ...interface{}) StateMachine
}

// Implementations

type state struct {
	id   int
	args []interface{}
}

func (t *state) Id() int {
	return t.id
}

func (t *state) Args() []interface{} {
	return t.args
}

func (t *state) String() string {
	return fmt.Sprintf("(%v)%v", t.id, t.args)
}

type workerSocket struct {
	parent *stageController
}

func newWorkerSocket(parent *stageController) *workerSocket {
	parent.wait.Inc()
	return &workerSocket{parent}
}

func (w *workerSocket) Done() {
	w.parent.wait.Dec()
}

func (w *workerSocket) Closed() <-chan struct{} {
	return w.parent.closed
}

func (w *workerSocket) Fail(e error) {
	select {
	case <-w.parent.closed:
		return
	case w.parent.resultIn <- NewFailureState(e):
		return
	}
}

func (w *workerSocket) Next(target int, args ...interface{}) {
	select {
	case <-w.parent.closed:
		return
	case w.parent.resultIn <- NewState(target, args...):
		return
	}
}

type stageController struct {
	wait      concurrent.Wait
	closed    chan struct{}
	resultIn  chan State
	resultOut chan State
}

func newStageController() *stageController {
	s := &stageController{
		concurrent.NewWait(),
		make(chan struct{}),
		make(chan State),
		make(chan State, 1)}

	go controlStage(s)
	return s
}

func (s *stageController) NewWorkerSocket() *workerSocket {
	return newWorkerSocket(s)
}

func (s *stageController) Result() <-chan State {
	return s.resultOut
}

func (s *stageController) Transition() chan<- State {
	return s.resultIn
}

func controlStage(s *stageController) {
	var result State
	select {
	case <-s.wait.Wait():
		s.resultOut <- NewTerminalState()
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

func newStage(id int, workers ...Worker) *stage {
	return &stage{id, workers}
}

func (s *stage) run(args []interface{}) *stageController {
	controller := newStageController()

	for _, w := range s.workers {
		work := w
		ctrl := controller.NewWorkerSocket()
		go func() {
			work(ctrl, args)
			ctrl.Done()
		}()
	}

	return controller
}

type stateMachineBuilder struct {
	states map[int]*stage
}

func NewStateMachineBuilder(states map[int]*stage) StateMachineBuilder {
	return &stateMachineBuilder{states}
}

func (s *stateMachineBuilder) AddState(id int, fn ...Worker) StateMachineBuilder {
	s.states[id] = newStage(id, fn...)
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
	update  chan State
	history concurrent.List
}

func newStateMachine(states map[int]*stage, init int, args []interface{}) StateMachine {
	s := &stateMachine{
		states:  states,
		closed:  make(chan struct{}),
		update:  make(chan State),
		history: concurrent.NewList(10)}

	go controlMachine(s, init, args)
	return s
}

func (s *stateMachine) Clone() StateMachineBuilder {
	return NewStateMachineBuilder(copyStages(s.states))
}

func (s *stateMachine) History() []State {
	return toStates(s.history.All())
}

func (s *stateMachine) Closed() <-chan struct{} {
	return s.closed
}

func (s *stateMachine) Transition() chan<- State {
	return s.update
}

func (s *stateMachine) Wait() error {
	<-s.Closed()
	return s.Result()
}

func (s *stateMachine) Running() bool {
	select {
	default:
		return true
	case <-s.Closed():
		return false
	}
}

func (s *stateMachine) Result() error {
	return extractFailure(extractLatest(s.History()))
}

func (s *stateMachine) Move(state int, args ...interface{}) error {
	select {
	case <-s.Closed():
		return MachineClosed
	case s.Transition() <- NewState(state, args...):
		return nil
	}
}

func (s *stateMachine) Close() error {
	s.Move(TerminalState)
	return s.Result()
}

func (s *stateMachine) Fail(e error) error {
	s.Move(FailureState, e)
	return s.Result()
}

func controlMachine(s *stateMachine, init int, args []interface{}) {
	cur := s.states[init]
	if cur == nil {
		s.history.Append(NewFailureState(fmt.Errorf("Could not start machine. State [%v] does not exist", init)))
		close(s.closed)
		return
	}

	s.history.Append(NewState(init, args...))
	for {
		control := cur.run(args)

		var next State
		select {
		case next = <-control.Result():
		case next = <-s.update:
			select {
			case tmp := <-control.Result():
				if IsFailureState(tmp) {
					next = tmp
				}
			case control.Transition() <- next:
				tmp := <-control.Result()
				if IsFailureState(tmp) {
					next = tmp
				}
			}
		}

		if IsFailureState(next) || IsTerminalState(next) {
			s.history.Append(next)
			close(s.closed)
			return
		}

		cur, args = s.states[next.Id()], next.Args()
		if cur == nil {
			s.history.Append(NewFailureState(fmt.Errorf("State [%v] does not exist", next.Id())))
			close(s.closed)
			return
		}

		s.history.Append(next)
	}
}

func toStates(raw []interface{}) []State {
	ret := make([]State, len(raw))
	for i, v := range raw {
		ret[i] = v.(State)
	}

	return ret
}

func extractNth(transitions []State, n int) State {
	if len(transitions)-1 < n {
		return nil
	}

	return transitions[n]
}

func extractLatest(transitions []State) State {
	if len(transitions) == 0 {
		return nil
	}

	return transitions[len(transitions)-1]
}

func extractFailure(latest State) error {
	if latest.Id() != FailureState {
		return nil
	}

	return latest.Args()[0].(error)
}

func copyStages(m map[int]*stage) map[int]*stage {
	ret := make(map[int]*stage)
	for k, v := range m {
		ret[k] = v
	}

	return ret
}
