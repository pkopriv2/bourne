package msg

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrStreamInvalidCommit = errors.New("STREAM:INVALID_COMMIT")
	ErrStreamClosed = errors.New("STREAM:CLOSED")
)

const (
	StreamLockWait = 5 * time.Millisecond
)

// Used to compare relative offsets.
func OffsetComparator(a, b interface{}) int {
	offsetA := a.(uint32)
	offsetB := b.(uint32)

	if offsetA > offsetB {
		return 1
	}

	if offsetA == offsetB {
		return 0
	}

	return -1
}

// A ref represents a position within a stream at
// particular moment in time.
//
type Ref struct {
	offset uint32
	time   time.Time
}

func NewRef(offset uint32) *Ref {
	return &Ref{offset, time.Now()}
}

// A simple, infinite, reliable stream.  This is the primary data structure
// behind the channel send/receive logic.   The stream is essentially duplicated
// between two locations.  This structure encapsulates that.  In other words,
// this may be thought of a distributed stream.  The basic
//
type Stream struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock

	tail *Ref
	cur  *Ref
	head *Ref

	closed bool
}

func NewStream(size uint) *Stream {
	ref := NewRef(0)
	return &Stream{
		data: make([]byte, size),
		tail: ref,
		cur:  ref,
		head: ref}
}

func (s *Stream) Close() {
	s.lock.RLock()
	defer s.lock.RUnlock()
	s.closed = true
}

func (s *Stream) Refs() (*Ref, *Ref, *Ref) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.tail, s.cur, s.head
}

func (s *Stream) Reset() (*Ref, *Ref) {
	s.lock.Lock()
	defer s.lock.Unlock()

	before := s.cur

	// moves the read back to the start.
	s.tail = NewRef(s.tail.offset)
	s.cur = s.tail
	return s.cur, before
}

func (s *Stream) Commit(pos uint32) (*Ref, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if pos > s.head.offset {
		return nil, ErrStreamInvalidCommit
	}

	if pos < s.tail.offset {
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
	r := s.cur.offset
	w := s.head.offset

	len := uint32(len(s.data))
	ret := make([]byte, w-r)

	// just start copying until we get to write
	for i := uint32(0); r+i < w; i++ {
		ret[i] = s.data[(r+i)%len]
	}

	return ret
}

// Reads from the buffer from the current positon.
//
func (s *Stream) TryRead(in []byte, prune bool) (*Ref, uint32, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return nil, 0, ErrStreamClosed
	}

	// get the new write
	inLen := uint32(len(in))
	bufLen := uint32(len(s.data))

	// grab the current read offset.
	start := s.cur

	// grab current positions
	r := start.offset
	w := s.head.offset

	var i uint32 = 0
	for ; i < inLen && r+i < w; i++ {
		in[i] = s.data[(r+i)%bufLen]
	}

	s.cur = NewRef(r + i)

	// are we moving the start position?
	if prune {
		s.tail = s.cur
	}

	return start, i, nil
}

func (s *Stream) TryWrite(val []byte) (uint32, *Ref, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return 0, nil, ErrStreamClosed
	}

	// get the new write
	valLen := uint32(len(val))
	bufLen := uint32(len(s.data))

	// grab current positions
	r := s.tail.offset
	w := s.head.offset

	// just write until we can't write anymore.
	var i uint32 = 0
	for ; i < valLen && w+i < r+bufLen; i++ {
		s.data[(w+i)%bufLen] = val[i]
	}

	s.head = NewRef(w + i)
	return i, s.head, nil
}

func (s *Stream) Write(val []byte) (int, error) {
	valLen := uint32(len(val))

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
