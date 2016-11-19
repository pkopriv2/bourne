package convoy

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

// The indexer simply reads from a changelog and writes to the directory.
type indexer struct {
	ctx    common.Context
	log    *changeLog
	dir    *directory
	closed chan struct{}
	closer chan struct{}

	wait sync.WaitGroup
}

func newIndexer(ctx common.Context, log *changeLog, dir *directory) (*indexer, error) {
	e := &indexer{
		ctx:    ctx,
		log:    log,
		dir:    dir,
		closed: make(chan struct{}),
		closer: make(chan struct{}, 1)}

	if err := e.start(); err != nil {
		return nil, err
	}

	return e, nil
}

func (i *indexer) Close() error {
	select {
	case <-i.closed:
		return errors.New("Index already closing")
	case i.closer <- struct{}{}:
	}

	close(i.closed)
	i.wait.Wait()
	return nil
}

func (i *indexer) start() error {
	select {
	default:
	case <-i.closed:
		return errors.New("Index already closed")
	}

	// grab the changelog id.  (we'll be generating)
	id, err := i.log.Id()
	if err != nil {
		return errors.Wrapf(err, "Error retrieving changelog id")
	}

	// Listen to the changelog.
	ch := changeLogListen(i.log)

	// read from log and write to dir.
	i.wait.Add(1)
	go func() {
		defer i.wait.Done()
		i.index(ch, id)
	}()
	return nil
}

func (i *indexer) index(ch <-chan Change, id uuid.UUID) {
	for {
		var c Change
		select {
		case <-i.closed:
			return
		case c = <-ch:
		}

		done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
			i.dir.Apply(changeToDataEvent(c, id))
			return nil
		})

		select {
		case <-i.closed:
			return
		case <-done:
			continue
		case <-timeout:
			return
		}
	}
}

func changeToDataEvent(c Change, id uuid.UUID) *dataEvent {
	return &dataEvent{id, c.Key, c.Val, c.Ver, c.Del}
}
