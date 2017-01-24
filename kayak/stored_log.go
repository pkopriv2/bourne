package kayak

import (
	"errors"

	uuid "github.com/satori/go.uuid"
)

// Storage apis,errors
var (
	AccessError      = errors.New("Kayak:Access")
	EndOfStreamError = errors.New("Kayak:EndOfStream")
	OutOfBoundsError = errors.New("Kayak:OutOfBounds")
	SwapError        = errors.New("Kayak:Swap")
)

type LogStore interface {
	Get(id uuid.UUID) (StoredLog, error)
	New(uuid.UUID, []byte) (StoredLog, error)
}

type StoredLog interface {
	Id() uuid.UUID
	Active() (StoredSegment, error)
	GetCommit() (int, error)
	SetCommit(int) (int, error)
	Swap(StoredSegment, StoredSegment) (bool, error)
}

type StoredSegment interface {
	PrevIndex() int
	PrevTerm() int
	Snapshot() (StoredSnapshot, error)
	Head() (int, error)
	Get(index int) (LogItem, bool, error)
	Scan(beg int, end int) ([]LogItem, error)
	Append(Event, int, uuid.UUID, int, int) (int, error)
	Insert([]LogItem) (int, error)
	Compact(until int, ch <-chan Event, size int, config []byte) (StoredSegment, error)
	Delete() error
}

type StoredSnapshot interface {
	Size() int
	Config() []byte
	Scan(beg int, end int) ([]Event, error)
	Delete() error
}
