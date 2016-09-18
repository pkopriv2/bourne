package client

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrStreamInvalidCommit = errors.New("STREAM:INVALID_COMMIT")
	ErrStreamClosed        = errors.New("STREAM:CLOSED")
)

const (
	StreamLockWait = 5 * time.Millisecond
)

// A ref represents a position within a stream at particular moment in time.
//
type Ref struct {
	offset uint64
	time   time.Time
}

// Generates a new ref from the offset.  Uses time.Now() for time of ref.
//
func NewRef(offset uint64) *Ref {
	return &Ref{offset, time.Now()}
}

// A simple, infinite, reliable stream.  This is the primary data structure
// behind the channel send/receive logic.  A stream is just a circular buffer
// with three offsets instead of two:
//
//  * Head: Where the next write occurs.
//  * Cur:  Where the next read occurs.  In the case of a distributed stream,
//          this represents un-verified sends.
//  * Tail: What can be forgotten.  (ie has been verified)
//
type Stream struct {
	buffer []byte
	lock   sync.RWMutex // just using simple, coarse lock

	tail *Ref
	cur  *Ref
	head *Ref

	closed bool // no more io
}

func NewStream(size int) *Stream {
	ref := NewRef(0)
	return &Stream{
		buffer: make([]byte, size),
		tail:   ref,
		cur:    ref,
		head:   ref}
}

func (s *Stream) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return ErrStreamClosed
	}
	s.closed = true
	return nil
}

func (s *Stream) Closed() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.closed
}

func (s *Stream) Snapshot() (*Ref, *Ref, *Ref, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.tail, s.cur, s.head, s.closed
}

func (s *Stream) Reset() (*Ref, *Ref, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil, nil, ErrStreamClosed
	}

	prev := s.cur

	// moves the read back to the start.
	s.tail = NewRef(s.tail.offset)
	s.cur = s.tail
	return s.cur, prev, nil
}

func (s *Stream) Commit(pos uint64) (*Ref, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil, ErrStreamClosed
	}

	if pos > s.head.offset {
		return nil, ErrStreamInvalidCommit
	}

	if pos <= s.tail.offset {
		return nil, nil
	}

	// committing just equates to moving the tail pointer
	s.tail = NewRef(pos)

	// we may be committing beyond the current read pointer.  in that case, move it too
	if pos > s.cur.offset {
		s.cur = s.tail
	}
	return s.tail, nil
}

func (s *Stream) Data() []byte {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// grab their positions
	r := s.tail.offset
	w := s.head.offset

	len := uint64(len(s.buffer))
	ret := make([]byte, w-r)

	// just start copying until we get to write
	for i := uint64(0); r+i < w; i++ {
		ret[i] = s.buffer[(r+i)%len]
	}

	return ret
}

// Reads from the buffer from the current positon.
//
func (s *Stream) TryRead(in []byte, prune bool) (*Ref, uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil, 0, ErrStreamClosed
	}

	// get the new write
	inLen := uint64(len(in))
	bufLen := uint64(len(s.buffer))

	// grab the current read offset.
	start := s.cur

	// grab current positions
	r := start.offset
	w := s.head.offset

	var i uint64 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.buffer[(r+i)%bufLen]
	}

	s.cur = NewRef(r + i)

	// are we moving the start position?
	if prune {
		s.tail = s.cur
	}

	return start, i, nil
}

func (s *Stream) TryWrite(val []byte) (uint64, *Ref, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return 0, nil, ErrStreamClosed
	}

	// get the new write
	valLen := uint64(len(val))
	bufLen := uint64(len(s.buffer))

	// grab current positions
	r := s.tail.offset
	w := s.head.offset

	// just write until we can't write anymore.
	var i uint64 = 0
	for ; i < valLen && w+i < r+bufLen; i++ {
		s.buffer[(w+i)%bufLen] = val[i]
	}

	s.head = NewRef(w + i)
	return i, s.head, nil
}

func (s *Stream) Write(val []byte) (int, error) {
	valLen := uint64(len(val))

	for {

		num, _, err := s.TryWrite(val)
		if err != nil {
			return 0, err
		}

		val = val[num:]
		if len(val) == 0 {
			break
		}

		time.Sleep(StreamLockWait)
	}

	return int(valLen), nil
}

func (s *Stream) Read(in []byte) (int, error) {
	for {
		_, num, err := s.TryRead(in, true)
		if err != nil {
			return 0, err
		}

		if num > 0 {
			return int(num), nil
		}

		time.Sleep(StreamLockWait)
	}

	panic("Not accessible")
}

// Used to compare relative offsets.
func OffsetComparator(a, b interface{}) int {
	offsetA := a.(uint64)
	offsetB := b.(uint64)

	if offsetA > offsetB {
		return 1
	}

	if offsetA == offsetB {
		return 0
	}

	return -1
}
