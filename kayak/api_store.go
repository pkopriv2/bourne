package kayak

import uuid "github.com/satori/go.uuid"

// Durability apis

// TODO: Complete the api so that we can have command line utilities for interacting
// with nodes.

type LogStore interface {
	Get(id uuid.UUID) (StoredLog, error)
	New(uuid.UUID, []byte) (StoredLog, error)
	NewSnapshot(int, int, <-chan Event, int, []byte) (StoredSnapshot, error)
}

type StoredLog interface {
	Id() uuid.UUID
	Store() (LogStore, error)
	Last() (int, int, error)
	Truncate(start int) error
	Scan(beg int, end int) ([]Entry, error)
	Append(Event, int, Kind) (Entry, error)
	Get(index int) (Entry, bool, error)
	Insert([]Entry) error
	Install(StoredSnapshot) error
	Snapshot() (StoredSnapshot, error)
}

type StoredSnapshot interface {
	LastIndex() int
	LastTerm() int
	Size() int
	Config() []byte
	Scan(beg int, end int) ([]Event, error)
	Delete() error
}
