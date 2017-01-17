package kayak

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func OpenTestLogStash(ctx common.Context) stash.Stash {
	db := OpenTestStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		return initBuckets(tx)
	})
	return db
}

func TestDurableLog_CreateSnapshot_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)

	s, err := createDurableSnapshot(db, NewEventChannel([]Event{}), 0, []byte{})
	assert.Nil(t, err)

	snapshot, ok, err := openDurableSnapshot(db, s.id)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, s.id, snapshot.id)
	assert.Equal(t, s.Config(), snapshot.Config())

	events, err := snapshot.Scan(db, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []Event{}, events)
}

func TestDurableLog_CreateSnapshot_Config(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []byte{0, 1, 2}

	db := OpenTestLogStash(ctx)

	s, err := createDurableSnapshot(db, NewEventChannel([]Event{}), 0, expected)
	assert.Nil(t, err)

	snapshot, ok, err := openDurableSnapshot(db, s.id)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, s.id, snapshot.id)
	assert.Equal(t, expected, s.Config())
	assert.Equal(t, expected, snapshot.Config())
}

func TestDurableLog_CreateSnapshot_Events(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []Event{[]byte{0, 1}, []byte{0, 1}}

	db := OpenTestLogStash(ctx)
	s, err := createDurableSnapshot(db, NewEventChannel(expected), len(expected), []byte{})
	assert.Nil(t, err)

	events, err := s.Scan(db, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, expected, events)
}

func TestDurableLog_CreateSnapshot_MultipleWithEvents(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected1 := []Event{[]byte{0, 1}, []byte{2, 3}}
	expected2 := []Event{[]byte{0, 1, 2}, []byte{3}, []byte{4, 5}}

	db := OpenTestLogStash(ctx)

	snapshot1, err := createDurableSnapshot(db, NewEventChannel(expected1), len(expected1), []byte{})
	assert.Nil(t, err)

	snapshot2, err := createDurableSnapshot(db, NewEventChannel(expected2), len(expected2), []byte{})
	assert.Nil(t, err)

	events1, err := snapshot1.Scan(db, 0, snapshot1.size)
	assert.Nil(t, err)
	assert.Equal(t, expected1, events1)

	events2, err := snapshot2.Scan(db, 0, snapshot2.size)
	assert.Nil(t, err)
	assert.Equal(t, expected2, events2)
}

func TestDurableLog_DeleteSnapshot(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	events := []Event{[]byte{0, 1}, []byte{2, 3}}

	db := OpenTestLogStash(ctx)

	snapshot, err := createDurableSnapshot(db, NewEventChannel(events), len(events), []byte{})
	assert.Nil(t, err)
	assert.Nil(t, snapshot.Delete(db))
	assert.Nil(t, snapshot.Delete(db)) // idempotent deletes

	events, err = snapshot.Scan(db, 0, 100)
	assert.Equal(t, DeletedError, errors.Cause(err))
	assert.Nil(t, events) // idempotent deletes
}

func TestDurableLog_CreateSegment_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)
	assert.Equal(t, -1, seg.prevIndex)
	assert.Equal(t, -1, seg.prevTerm)

	head, err := seg.Head(db)
	assert.Nil(t, err)
	assert.Equal(t, -1, head)

	batch, err := seg.Scan(db, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{}, batch)

	snapshot, err := seg.Snapshot(db)
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, snapshot.Config())

	events, err := snapshot.Scan(db, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []Event{}, events)
}

func TestDurableLog_Segment_Append_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	expected := Event{0, 1}
	head, err := seg.Append(db, []Event{expected}, 1)
	assert.Nil(t, err)
	assert.Equal(t, 0, head)

	batch, err := seg.Scan(db, 0, 100)
	assert.Nil(t, err)

	expectedItem := LogItem{Index: 0, term: 1, Event: expected}
	assert.Equal(t, []LogItem{expectedItem}, batch)
}

func TestDurableLog_Segment_Append_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	expected := Event{0, 1}
	head, err := seg.Append(db, []Event{expected}, 1)
	head, err = seg.Append(db, []Event{expected}, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, head)

	batch, err := seg.Scan(db, 0, 100)
	assert.Nil(t, err)

	expectedItem1 := LogItem{Index: 0, term: 1, Event: expected}
	expectedItem2 := LogItem{Index: 1, term: 1, Event: expected}
	assert.Equal(t, []LogItem{expectedItem1, expectedItem2}, batch)
}

func TestDurableLog_Segment_Insert_IllegalIndex(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	exp := LogItem{Index: 1, term: 1, Event: Event{0, 1}}

	_, err = seg.Insert(db, []LogItem{exp})
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))
}

func TestDurableLog_Segment_Insert_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	expected1 := LogItem{Index: 0, term: 1, Event: Event{0, 1}}
	expected2 := LogItem{Index: 1, term: 1, Event: Event{0, 1, 2}}

	_, err = seg.Insert(db, []LogItem{expected1})
	assert.Nil(t, err)
	_, err = seg.Insert(db, []LogItem{expected2})
	assert.Nil(t, err)

	batch, err := seg.Scan(db, 0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{expected1, expected2}, batch)
}

func TestDurableLog_Segment_Delete(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	expected1 := LogItem{Index: 0, term: 1, Event: Event{0, 1}}
	expected2 := LogItem{Index: 1, term: 1, Event: Event{0, 1, 2}}

	_, err = seg.Insert(db, []LogItem{expected1})
	assert.Nil(t, err)
	_, err = seg.Insert(db, []LogItem{expected2})
	assert.Nil(t, err)

	err = seg.Delete(db)
	assert.Nil(t, err)

	_, err = seg.Scan(db, 0, 100)
	assert.Equal(t, DeletedError, errors.Cause(err))
}

func TestDurableLog_Segment_Compact_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	_, err = seg.CopyAndCompact(db, 1, NewEventChannel([]Event{}), 0, []byte{})
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))

	cop, err := seg.CopyAndCompact(db, -1, NewEventChannel([]Event{}), 0, []byte{})
	assert.Nil(t, err)
	assert.NotEqual(t, cop.id, seg.id)
}

func TestDurableLog_Segment_Compact_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	prevHead, err := seg.Append(db, []Event{Event{0, 1}}, 1)
	assert.Nil(t, err)

	compacted, err := seg.CopyAndCompact(db, 0, NewEventChannel([]Event{Event{0}}), 0, []byte{})
	assert.Nil(t, err)

	newHead, err := compacted.Head(db)
	assert.Nil(t, err)
	assert.Equal(t, prevHead, newHead)
	assert.Equal(t, compacted.prevIndex, 0)
	assert.Equal(t, compacted.prevTerm, 1)
}

func TestDurableLog_Segment_Compact_Multiple(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	prevHead, err := seg.Append(db, []Event{Event{0, 1}, Event{0, 1}}, 1)
	assert.Nil(t, err)

	compacted, err := seg.CopyAndCompact(db, 1, NewEventChannel([]Event{Event{0}}), 0, []byte{})
	assert.Nil(t, err)

	newHead, err := compacted.Head(db)
	assert.Nil(t, err)
	assert.Equal(t, prevHead, newHead)
	assert.Equal(t, compacted.prevIndex, 1)
	assert.Equal(t, compacted.prevTerm, 1)

	_, err = compacted.Scan(db, 0, 0)
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))
}

func TestDurableLog_Segment_Compact_UntilLessThanHead(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	seg, err := initDurableSegment(db)
	assert.Nil(t, err)

	prevHead, err := seg.Append(db, []Event{Event{0, 1, 1}, Event{0, 1}}, 1)
	assert.Nil(t, err)

	compacted, err := seg.CopyAndCompact(db, 0, NewEventChannel([]Event{Event{0}}), 0, []byte{})
	assert.Nil(t, err)

	newHead, err := compacted.Head(db)
	assert.Nil(t, err)
	assert.Equal(t, prevHead, newHead)
	assert.Equal(t, compacted.prevIndex, 0)
	assert.Equal(t, compacted.prevTerm, 1)

	scanned, err := compacted.Scan(db, 1, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{LogItem{Index: 1, term: 1, Event: Event{0, 1}}}, scanned)
}

func TestDurableLog_OpenDurableLog_NoExist(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := openDurableLog(db, uuid.NewV1())
	assert.Nil(t, err)

	seg, err := log.Active(db)
	assert.Nil(t, err)
	assert.Equal(t, -1, seg.prevIndex)
	assert.Equal(t, -1, seg.prevTerm)

	snapshot, err := seg.Snapshot(db)
	assert.Nil(t, err)
	assert.Equal(t, 0, snapshot.size)
	assert.Equal(t, []byte{}, snapshot.config)
}
