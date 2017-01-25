package kayak

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func OpenTestLogStash(ctx common.Context) *bolt.DB {
	db := OpenTestStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		return initBoltBuckets(tx)
	})
	return db
}

func TestBoltLog_CreateSnapshot_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)

	s, err := createBoltSnapshot(db, -1, -1, NewEventChannel([]Event{}), 0, []byte{})
	assert.Nil(t, err)

	snapshot, err := openBoltSnapshot(db, s.Id())
	assert.Nil(t, err)
	assert.Equal(t, s.Id(), snapshot.Id())
	assert.Equal(t, s.Config(), snapshot.Config())

	events, err := snapshot.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []Event{}, events)
}

func TestBoltLog_CreateSnapshot_Config(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []byte{0, 1, 2}

	db := OpenTestLogStash(ctx)

	s, err := createBoltSnapshot(db, -1, -1, NewEventChannel([]Event{}), 0, expected)
	assert.Nil(t, err)

	snapshot, err := openBoltSnapshot(db, s.Id())
	assert.Nil(t, err)
	assert.Equal(t, s.Id(), snapshot.Id())
	assert.Equal(t, expected, snapshot.Config())
}

func TestBoltLog_CreateSnapshot_Events(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []Event{[]byte{0, 1}, []byte{0, 1}}

	db := OpenTestLogStash(ctx)

	s, err := createBoltSnapshot(db, 1, 1, NewEventChannel(expected), 2, []byte{})
	assert.Nil(t, err)

	events, err := s.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, expected, events)
}

func TestBoltLog_CreateSnapshot_MultipleWithEvents(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected1 := []Event{[]byte{0, 1}, []byte{2, 3}}
	expected2 := []Event{[]byte{0, 1, 2}, []byte{3}, []byte{4, 5}}

	db := OpenTestLogStash(ctx)

	snapshot1, err := createBoltSnapshot(db, 1, 1, NewEventChannel(expected1), len(expected1), []byte{})
	assert.Nil(t, err)

	snapshot2, err := createBoltSnapshot(db, 2, 2, NewEventChannel(expected2), len(expected2), []byte{})
	assert.Nil(t, err)

	events1, err := snapshot1.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, expected1, events1)

	events2, err := snapshot2.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, expected2, events2)
}

func TestBoltLog_DeleteSnapshot(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	events := []Event{[]byte{0, 1}, []byte{2, 3}}

	db := OpenTestLogStash(ctx)
	snapshot, err := createBoltSnapshot(db, 2, 1, NewEventChannel(events), len(events), []byte{})
	assert.Nil(t, err)
	assert.Nil(t, snapshot.Delete())
	assert.Nil(t, snapshot.Delete()) // idempotent deletes

	events, err = snapshot.Scan(0, 100)
	assert.Equal(t, AccessError, errors.Cause(err))
	assert.Nil(t, events) // idempotent deletes
}

// func TestBoltStore_New_WithConfig(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// s := NewBoltStore(OpenTestLogStash(ctx))
// assert.NotNil(t, s)
//
// id := uuid.NewV1()
//
// log, err := s.New(id, []byte{0, 1})
// assert.Nil(t, err)
// assert.NotNil(t, log)
//
// seg, err := log.Active()
// assert.Nil(t, err)
//
// assert.Equal(t, -1, seg.PrevIndex())
// assert.Equal(t, -1, seg.PrevTerm())
// }
//
// func TestBoltStore_Get_NoExist(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// s := NewBoltStore(OpenTestLogStash(ctx))
// assert.NotNil(t, s)
//
// log, err := s.Get(uuid.NewV1())
// assert.Nil(t, err)
// assert.Nil(t, log)
// }

// func TestBoltLog_CreateSegment_Empty(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
// assert.Equal(t, -1, seg.PrevIndex())
// assert.Equal(t, -1, seg.PrevTerm())
//
// head, err := seg.Head()
// assert.Nil(t, err)
// assert.Equal(t, -1, head)
//
// batch, err := seg.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{}, batch)
//
// snapshot, err := seg.Snapshot()
// assert.Nil(t, err)
// assert.Equal(t, []byte{}, snapshot.Config())
//
// events, err := snapshot.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []Event{}, events)
// }
//
// func TestBoltLog_Segment_Append_Single(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
// assert.Equal(t, -1, seg.PrevIndex())
// assert.Equal(t, -1, seg.PrevTerm())
//
// exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
// head, err := seg.Append(exp.Event, exp.Term, exp.Source, exp.Seq, exp.Kind)
// assert.Nil(t, err)
// assert.Equal(t, 0, head)
//
// batch, err := seg.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{exp}, batch)
// }
//
// func TestBoltLog_Segment_Append_Multi(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// exp1 := LogItem{Index: 0, Term: 1, Event: Event{0}}
// exp2 := LogItem{Index: 1, Term: 2, Event: Event{0, 1}}
//
// head1, err := seg.Append(exp1.Event, exp1.Term, exp1.Source, exp1.Seq, exp1.Kind)
// assert.Nil(t, err)
// head2, err := seg.Append(exp2.Event, exp2.Term, exp2.Source, exp2.Seq, exp2.Kind)
// assert.Nil(t, err)
//
// assert.Equal(t, 0, head1)
// assert.Equal(t, 1, head2)
//
// batch, err := seg.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{exp1, exp2}, batch)
// }
//
// func TestBoltLog_Segment_Insert_IllegalIndex(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// exp := LogItem{Index: 1, Term: 1, Event: Event{0}}
//
// _, err = seg.Insert([]LogItem{exp})
// assert.Equal(t, OutOfBoundsError, errors.Cause(err))
// }
//
// func TestBoltLog_Segment_Insert_Multi(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// exp1 := LogItem{Index: 0, Term: 1, Event: Event{0, 1}}
// exp2 := LogItem{Index: 1, Term: 1, Event: Event{0, 1, 2}}
//
// head, err := seg.Insert([]LogItem{exp1, exp2})
// assert.Nil(t, err)
// assert.Equal(t, 1, head)
//
// batch, err := seg.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{exp1, exp2}, batch)
// }
//
// // This will move Head/Max backwards
// func TestBoltLog_Segment_Insert_Middle(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// exp1 := LogItem{Index: 0, Term: 1, Event: Event{0, 1}}
// exp2 := LogItem{Index: 1, Term: 1, Event: Event{0, 1, 2}}
//
// head, err := seg.Insert([]LogItem{exp1, exp2})
// assert.Nil(t, err)
// assert.Equal(t, 1, head)
//
// exp3 := LogItem{Index: 0, Term: 1, Event: Event{0}}
// head, err = seg.Insert([]LogItem{exp3})
// assert.Equal(t, OutOfBoundsError, errors.Cause(err))
//
// batch, err := seg.Scan(0, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{exp1, exp2}, batch)
// }
//
// //
// func TestBoltLog_Segment_Delete(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// expected1 := LogItem{Index: 0, Term: 1, Event: Event{0, 1}}
// expected2 := LogItem{Index: 1, Term: 1, Event: Event{0, 1, 2}}
//
// _, err = seg.Insert([]LogItem{expected1})
// assert.Nil(t, err)
// _, err = seg.Insert([]LogItem{expected2})
// assert.Nil(t, err)
//
// err = seg.Delete()
// assert.Nil(t, err)
//
// _, err = seg.Scan(0, 100)
// assert.Equal(t, AccessError, errors.Cause(err))
// }
//
// func TestBoltLog_Segment_Compact_Empty(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// _, err = seg.Compact(1, NewEventChannel([]Event{}), 0, []byte{})
// assert.Equal(t, OutOfBoundsError, errors.Cause(err))
//
// cop, err := seg.Compact(-1, NewEventChannel([]Event{}), 0, []byte{})
// assert.Nil(t, err)
// assert.NotEqual(t, cop, seg)
// }
//
// func TestBoltLog_Segment_Compact_SingleItem(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// item1 := LogItem{Index: 0, Term: 1, Event: Event{0, 1}}
// item2 := LogItem{Index: 1, Term: 2, Event: Event{0, 1, 2}}
//
// prevHead, err := seg.Insert([]LogItem{item1, item2})
// assert.Nil(t, err)
// assert.Equal(t, 1, prevHead)
//
// compacted, err := seg.Compact(0, NewEventChannel([]Event{Event{0}}), 1, []byte{})
// assert.Nil(t, err)
//
// newHead, err := compacted.Head()
// assert.Nil(t, err)
// assert.Equal(t, prevHead, newHead)
// assert.Equal(t, compacted.PrevIndex(), 0)
// assert.Equal(t, compacted.PrevTerm(), 1)
//
// batch, err := seg.Scan(1, 100)
// assert.Nil(t, err)
// assert.Equal(t, []LogItem{item2}, batch)
//
// }
//
// func TestBoltLog_Segment_Compact_All(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// db := OpenTestLogStash(ctx)
// seg, err := createEmptyBoltSegment(db, []byte{})
// assert.Nil(t, err)
//
// item1 := LogItem{Index: 0, Term: 1, Event: Event{0, 1}}
// item2 := LogItem{Index: 1, Term: 2, Event: Event{0, 1, 2}}
// item3 := LogItem{Index: 2, Term: 3, Event: Event{0, 1, 2, 3}}
//
// prevHead, err := seg.Insert([]LogItem{item1, item2, item3})
// assert.Nil(t, err)
// assert.Equal(t, 2, prevHead)
//
// compacted, err := seg.Compact(2, NewEventChannel([]Event{Event{0}}), 1, []byte{})
// assert.Nil(t, err)
//
// newHead, err := compacted.Head()
// assert.Nil(t, err)
// assert.Equal(t, prevHead, newHead)
// assert.Equal(t, compacted.PrevIndex(), 2)
// assert.Equal(t, compacted.PrevTerm(), 3)
//
// _, err = compacted.Scan(2, 100)
// assert.Equal(t, OutOfBoundsError, errors.Cause(err))
// }
