package client

import (
	"container/list"
	"errors"
	"log"
	"sync"
)

var (
	// To be returned if there are no more ids available.
	IdPoolCapacityError = errors.New("IDPOOL:CAPACITY")
)

const (
	// The pool of ids is restricted to the range [256, 65535].
	IdPoolMinId = 256   // exclusive
	IdPoolMaxId = 65535 // exclusive

	// Each time the pool is expanded, it grows by this amount.
	IdPoolExpInc = 10
)

// A memory efficient pool of available ids. The pool will be
// restricted to the range defined by:
//
//  [IdPoolMinId, IdPoolMaxId]
//
// By convention, the pool will grow downward, meaning higher ids are
// favored.  Unlike a sync.Pool, this class does not automatically
// clean up the available pool.  This is to prevent leaving ids
// unavailable in the event of a de-allocation.
//
// This pool does NOT track ownership, which allows someone to return
// an id they did take themselves.  In that event, the same id may be
// given out at the same time.  SO, DON'T DO IT!
//
// *This object is thread-safe*
//
type IdPool struct {
	lock  sync.Mutex
	avail *list.List
	next  uint // used as a low watermark
}

// Creates a new id pool.  The pool is initialized with
// ID_POOL_EXP_INC values.  Each time the pool's values
// are exhausted, it is automatically and safely expanded.
//
func NewIdPool() *IdPool {
	avail := list.New()

	pool := &IdPool{avail: avail, next: IdPoolMaxId}
	pool.expand(IdPoolExpInc)
	return pool
}

// WARNING: Not thread-safe.  Internal use only!
//
// Expands the available ids by numItems or until
// it has reached maximum capacity.
//
func (self *IdPool) expand(numItems uint) error {
	log.Printf("Attemping to expand id pool [%v] by [%v] items\n", self.next, numItems)

	i, prev := self.next, self.next
	for ; i > prev-numItems && i >= IdPoolMinId; i-- {
		self.avail.PushBack(i)
	}

	// if we didn't move i, the pool is full.
	if i == prev {
		return IdPoolCapacityError
	}

	// move the watermark
	self.next = i
	return nil
}

// Takes an available id from the pool.  If one can't be taken
// a non-nil error is returned.
//
// In the event of a non-nil error, the consumer MUST not use the
// returned value.
//
func (self *IdPool) Take() (uint, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	// see if anything is available
	if item := self.avail.Front(); item != nil {
		return self.avail.Remove(item).(uint), nil
	}

	// try to expand the pool
	if err := self.expand(IdPoolExpInc); err != nil {
		return 0, err
	}

	// okay, the pool has been expanded
	return self.avail.Remove(self.avail.Front()).(uint), nil
}

// Returns an id to the pool.
//
// **WARNING:**
//
//  Only ids that have been loaned out should be returned to the
//  pool.
//
func (self *IdPool) Return(id uint) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if id < self.next {
		panic("Returned an invalid id!")
	}

	self.avail.PushFront(id)
}
