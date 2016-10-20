package concurrent

import (
	"sync"

	"github.com/emirpasic/gods/stacks"
	"github.com/emirpasic/gods/stacks/arraystack"
)

type Stack interface {
	Push(value interface{})
	Pop() (value interface{})
	Peek() (value interface{})
}

type stack struct {
	lock  sync.Mutex
	inner stacks.Stack
}

func NewArrayStack() Stack {
	return &stack{inner: arraystack.New()}
}

func (s *stack) Push(value interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.inner.Push(value)
}

func (s *stack) Pop() (value interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	val, ok := s.inner.Pop()
	if ! ok {
		return nil
	}

	return val
}

func (s *stack) Peek() (value interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	val, ok := s.inner.Peek()
	if ! ok {
		return nil
	}

	return val
}

