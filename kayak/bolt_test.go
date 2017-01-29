package kayak

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
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
	assert.Equal(t, InvariantError, errors.Cause(err))
	assert.Nil(t, events) // idempotent deletes
}

func TestBoltStore_New_WithConfig(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	s := NewBoltStore(OpenTestLogStash(ctx))
	assert.NotNil(t, s)

	id := uuid.NewV1()

	log, err := s.New(id, []byte{0, 1})
	assert.Nil(t, err)
	assert.NotNil(t, log)

	raw := log.(*boltLog)

	min, err := raw.Min()
	assert.Nil(t, err)
	assert.Equal(t, -1, min)
}

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

func TestBoltLog_Create_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)

	id := uuid.NewV1()
	log, err := createBoltLog(db, id, []byte{})
	assert.Nil(t, err)
	assert.Equal(t, id, log.id)

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, -1, min)
	assert.Equal(t, -1, max)

	lastIndex, lastTerm, _ := log.Last()
	assert.Equal(t, -1, lastIndex)
	assert.Equal(t, -1, lastTerm)

	batch, _ := log.Scan(0, 0)
	assert.Empty(t, batch)

	_, err = log.Scan(-2, 0)
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))

	snapshot, err := log.Snapshot()
	assert.Equal(t, -1, snapshot.LastIndex())
	assert.Equal(t, -1, snapshot.LastTerm())
	assert.Equal(t, 0, snapshot.Size())
	assert.Equal(t, []byte{}, snapshot.Config())
}

func TestBoltLog_Append_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
	item, err := log.Append(exp.Event, exp.Term, exp.Source, exp.Seq, exp.Kind)
	assert.Nil(t, err)
	assert.Equal(t, exp, item)

	batch, err := log.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp}, batch)
}

func TestBoltLog_Append_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	exp1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	exp2 := LogItem{Index: 1, Term: 1, Event: Event{1}}

	item1, err := log.Append(exp1.Event, exp1.Term, exp1.Source, exp1.Seq, exp1.Kind)
	assert.Nil(t, err)
	item2, err := log.Append(exp2.Event, exp2.Term, exp2.Source, exp2.Seq, exp2.Kind)
	assert.Nil(t, err)

	assert.Equal(t, exp1, item1)
	assert.Equal(t, exp2, item2)

	batch, err := log.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp1, exp2}, batch)
}

func TestBoltLog_Insert_OutOfBounds(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	err = log.Insert([]LogItem{LogItem{Index: 1, Term: 0, Event: Event{0}}})
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))
}

func TestBoltLog_Insert_Multi(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	exp1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	exp2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	assert.Nil(t, log.Insert([]LogItem{exp1, exp2}))

	batch, err := log.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp1, exp2}, batch)
}

func TestBoltLog_Truncate_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)
	assert.Equal(t, OutOfBoundsError, errors.Cause(log.Truncate(1)))
}

func TestBoltLog_Truncate_GreaterThanMax(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2}))
	assert.Equal(t, OutOfBoundsError, errors.Cause(log.Truncate(2)))
}

func TestBoltLog_Truncate_EqualToMax(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))
	assert.Nil(t, log.Truncate(2))

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, 0, min)
	assert.Equal(t, 1, max)

	batch, err := log.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{item1, item2}, batch)
}

func TestBoltLog_Truncate_EqualToMin(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))
	assert.Nil(t, log.Truncate(0))

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, -1, min)
	assert.Equal(t, -1, max)

	batch, err := log.Scan(-1, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{}, batch)
}

func TestBoltLog_Prune_EqualToMin(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))
	assert.Nil(t, log.Prune(0))

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, 1, min)
	assert.Equal(t, 2, max)

	batch, err := log.Scan(1, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{item2, item3}, batch)
}

func TestBoltLog_Prune_EqualToMax(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))
	assert.Nil(t, log.Prune(2))

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, -1, min)
	assert.Equal(t, -1, max)
}

func TestBoltLog_Prune_MultiBatch(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	for i := 0; i < 1024; i++ {
		item := LogItem{Index: i, Term: 0, Event: Event{0}}
		assert.Nil(t, log.Insert([]LogItem{item}))
	}

	assert.Nil(t, log.Prune(1021))

	min, _ := log.Min()
	max, _ := log.Max()

	assert.Equal(t, 1022, min)
	assert.Equal(t, 1023, max)
}

func TestBoltLog_InstallSnapshot_Empty_LessThanPrev(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	snapshot, err := store.NewSnapshot(-2, -2, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Equal(t, CompactionError, errors.Cause(err))
	assert.False(t, success)

	maxIndex, maxTerm, err := log.Last()
	assert.Nil(t, err)
	assert.Equal(t, -1, -1, maxIndex, maxTerm)
}

func TestBoltLog_InstallSnapshot_Empty_EqualToPrev(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	snapshot, err := store.NewSnapshot(-1, -1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	maxIndex, maxTerm, err := log.Last()
	assert.Nil(t, err)
	assert.Equal(t, -1, -1, maxIndex, maxTerm)
}

func TestBoltLog_InstallSnapshot_EqualToMin(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))

	snapshot, err := store.NewSnapshot(0, 0, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	batch, err := log.Scan(1, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{item2, item3}, batch)

	snapshot, err = store.NewSnapshot(1, 1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err = log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	batch, err = log.Scan(2, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{item3}, batch)

	maxIndex, maxTerm, err := log.Last()
	assert.Nil(t, err)
	assert.Equal(t, 2, 2, maxIndex, maxTerm)
}

func TestBoltLog_InstallSnapshot_Middle(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))

	snapshot, err := store.NewSnapshot(1, 1, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	batch, err := log.Scan(2, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{item3}, batch)
}

func TestBoltLog_InstallSnapshot_EqualToMax(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))

	snapshot, err := store.NewSnapshot(2, 2, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	index, term, err := log.Last()
	assert.Nil(t, err)
	assert.Equal(t, index, 2)
	assert.Equal(t, term, 2)
}

func TestBoltLog_InstallSnapshot_GreaterThanMax(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	log, err := createBoltLog(db, uuid.NewV1(), []byte{})
	assert.Nil(t, err)

	store, err := log.Store()
	assert.Nil(t, err)

	item1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	item2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	item3 := LogItem{Index: 2, Term: 2, Event: Event{2}}
	assert.Nil(t, log.Insert([]LogItem{item1, item2, item3}))

	snapshot, err := store.NewSnapshot(5, 5, NewEventChannel([]Event{Event{0}}), 1, []byte{})
	assert.Nil(t, err)

	success, err := log.Install(snapshot)
	assert.Nil(t, err)
	assert.True(t, success)

	index, term, err := log.Last()
	assert.Nil(t, err)
	assert.Equal(t, index, 5)
	assert.Equal(t, term, 5)
}
