package msg

import (
	"container/list"
	"errors"
	"log"
	"sync"
)

// The IdPool is restricted to the range [256, 65535].
var ID_POOL_MAX_ID uint16 = 65535 // exclusive
var ID_POOL_MIN_ID uint16 = 256   // exclusive

// Each time the pool is expanded, it grows by this amount.
var ID_POOL_EXP_INC uint16 = 10

// To be returned if there are no more ids available.
var ID_POOL_CAP_ERROR error = errors.New("Pool has reached capacity!")

// A memory efficient pool of available ids. The pool will be
// restricted to the range defined by:
//
//  [ID_POOL_MIN_ID, ID_POOL_MAX_ID]
//
// By convention, the pool will grow downward, meaning higher ids are
// favored.  Unlike a sync.Pool, this class does not automatically
// clean up the available pool.  This is to prevent leaving ids
// unavailable in the event of a de-allocation.
//
// This pool does NOT track ownership, which allows someone to return
// an id they did take themselves.  In that event, the same id may be
// given out at the same time.
//
// *This object is thread-safe*
//
type IdPool struct {
	lock  *sync.Mutex
	avail *list.List
	next  uint16 // used as a low watermark
}

// Creates a new id pool.  The pool is initialized with
// ID_POOL_EXP_INC values.  Each time the pool's values
// are exhausted, it is automatically and safely expanded.
//
func NewIdPool() *IdPool {
	lock := new(sync.Mutex)
	avail := list.New()

	pool := &IdPool{lock, avail, ID_POOL_MAX_ID}
	pool.expand(ID_POOL_EXP_INC)
	return pool
}

// WARNING: Not thread-safe.  Internal use only!
//
// Expands the available ids by numItems or until
// it has reached maximum capacity.
//
func (self *IdPool) expand(numItems uint16) error {
	log.Printf("Attemping to expand id pool [%v] by [%v] items\n", self.next, numItems)

	i, prev := self.next, self.next
	for ; i > prev-numItems && i >= ID_POOL_MIN_ID; i-- {
		self.avail.PushBack(i)
	}

	// if we didn't move i, the pool is full.
	if i == prev {
		return ID_POOL_CAP_ERROR
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
func (self *IdPool) Take() (uint16, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	// see if anything is available
	if item := self.avail.Front(); item != nil {
		return self.avail.Remove(item).(uint16), nil
	}

	// try to expand the pool
	if err := self.expand(ID_POOL_EXP_INC); err != nil {
		return 0, err
	}

	// okay, the pool has been expanded
	return self.avail.Remove(self.avail.Front()).(uint16), nil
}

// Returns an id to the pool.
//
// **WARNING:**
//
//  Only ids that have been loaned out should be returned to the
//  pool.
//
func (self *IdPool) Return(id uint16) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if id < self.next {
		panic("Returned an invalid id!")
	}

	self.avail.PushFront(id)
}
