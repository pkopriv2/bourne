package msg

import (
	"sync"
	"time"
)

// A ref represents a position within a stream at
// particular moment in time.
//
type Ref struct {
	offset uint32
	time   time.Time
}

func NewRef(offset uint32) Ref {
	return Ref{offset, time.Now()}
}

// A simple, infinite, reliable stream.
//
type Stream struct {
	data []byte
	lock sync.RWMutex // just using simple, coarse lock

	tail Ref
	cur  Ref
	head Ref
}

func NewStream(size uint) *Stream {
	return &Stream{data: make([]byte, size)}
}

func (s *Stream) Refs() (Ref, Ref, Ref) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.tail, s.cur, s.head
}

func (s *Stream) Reset() (Ref, Ref) {
	s.lock.Lock()
	defer s.lock.Unlock()

	before := s.cur

	// moves the read back to the start.
	s.tail = NewRef(s.tail.offset)
	s.cur = s.tail
	return before, s.cur
}

func (s *Stream) Commit(pos uint32) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if pos > s.head.offset {
		return ERR_LOG_PRUNE_INVALID
	}

	if pos < s.tail.offset {
		return nil
	}

	s.tail = NewRef(pos)
	if pos > s.cur.offset {
		s.cur = s.tail
	}
	return nil
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
func (s *Stream) TryRead(in []byte, prune bool) (Ref, uint32) {
	s.lock.Lock()
	defer s.lock.Unlock()

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

	return start, i
}

func (s *Stream) TryWrite(val []byte) (uint32, Ref) {
	s.lock.Lock()
	defer s.lock.Unlock()

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
	return i, s.head
}

func (s *Stream) Write(val []byte) (int, error) {
	valLen := uint32(len(val))

	for {
		num, _ := s.TryWrite(val)

		val = val[num:]
		if len(val) == 0 {
			break
		}

		time.Sleep(DATALOG_LOCK_WAIT)
	}

	return int(valLen), nil
}

func (s *Stream) Read(in []byte) (n int, err error) {
	for {
		_, num := s.TryRead(in, true)
		if num > 0 {
			return int(num), nil
		}

		time.Sleep(DATALOG_LOCK_WAIT)
	}

	panic("Not accessible")
}
