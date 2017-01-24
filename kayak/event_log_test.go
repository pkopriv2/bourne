package kayak

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func OpenTestBoltEventLog(ctx common.Context) *eventLog {
	store := NewBoltStore(OpenTestLogStash(ctx))
	raw, err := store.New(uuid.NewV1(), []byte{})
	if err != nil {
		panic(err)
	}

	log, err := openEventLog(ctx.Logger(), raw)
	if err != nil {
		panic(err)
	}
	return log
}

func TestEventLog_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)
	assert.Equal(t, -1, log.Committed())
	assert.Equal(t, -1, log.Head())

	snapshot, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 0, snapshot.Size())
	assert.Equal(t, []byte{}, snapshot.Config())
}

func TestEventLog_Append_SingleBatch_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
	head, err := log.Append(exp.Event, exp.Term, exp.Source, exp.Seq, exp.Kind)
	assert.Nil(t, err)
	assert.Equal(t, 0, head)

	batch, err := log.Scan(0, 100)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp}, batch)
	assert.Equal(t, -1, log.Committed())
}

func TestEventLog_Insert_GreaterThanHead(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp := LogItem{Index: 1, Term: 1, Event: Event{0}}
	_, err := log.Insert([]LogItem{exp})
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))
}


func TestEventLog_Insert_SingleBatch_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
	head, err := log.Insert([]LogItem{exp})
	assert.Nil(t, err)
	assert.Equal(t, 0, head)
	assert.Equal(t, 0, log.Head())
	assert.Equal(t, -1, log.Committed())
}

func TestEventLog_Get_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	_, found, err := log.Get(1)
	assert.Nil(t, err)
	assert.False(t, found)
}

func TestEventLog_Get_NotFound(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
	_, err := log.Insert([]LogItem{exp})
	assert.Nil(t, err)

	_, found, err := log.Get(1)
	assert.Nil(t, err)
	assert.False(t, found)
}

func TestEventLog_Get(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)
	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}
	_, err := log.Insert([]LogItem{exp})
	assert.Nil(t, err)

	item, found, err := log.Get(0)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, exp, item)
}

func TestEventLog_Scan_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)
	items, err := log.Scan(0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{}, items)
}

func TestEventLog_Scan_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp := LogItem{Index: 0, Term: 1, Event: Event{0}}

	_, err := log.Insert([]LogItem{exp})
	assert.Nil(t, err)

	items, err := log.Scan(0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp}, items)
}

func TestEventLog_Scan_Middle(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log := OpenTestBoltEventLog(ctx)

	exp1 := LogItem{Index: 0, Term: 0, Event: Event{0}}
	exp2 := LogItem{Index: 1, Term: 1, Event: Event{1}}
	exp3 := LogItem{Index: 2, Term: 2, Event: Event{2}}

	_, err := log.Insert([]LogItem{exp1,exp2,exp3})
	assert.Nil(t, err)

	items, err := log.Scan(1, 1)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{exp2}, items)
}
//
// func TestEventLog_Listen_LogClosed(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// log, err := openEventLog(ctx.Logger(), OpenTestLogStash(ctx), uuid.NewV1())
// assert.Nil(t, err)
//
// l := log.ListenCommits(0, 1)
// assert.Nil(t, log.Close())
//
// _, e := l.Next()
// assert.Equal(t, ClosedError, e)
// }
//
// func TestEventLog_Listen_Close(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// log, err := openEventLog(ctx.Logger(), OpenTestLogStash(ctx), uuid.NewV1())
// assert.Nil(t, err)
//
// l := log.ListenCommits(0, 1)
// assert.Nil(t, l.Close())
//
// _, e := l.Next()
// assert.Equal(t, ClosedError, e)
// }
//
// func TestEventLog_Listen_Historical(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// log, err := openEventLog(ctx.Logger(), OpenTestLogStash(ctx), uuid.NewV1())
// assert.Nil(t, err)
//
// log.Append([]Event{Event{0}}, 1)
// log.Append([]Event{Event{1}}, 1)
// log.Commit(1)
//
// l := log.ListenCommits(0, 0)
// defer l.Close()
//
// for i := 0; i < 2; i++ {
// item, err := l.Next()
// assert.Nil(t, err)
// assert.Equal(t, i, item.Index)
// }
// }
//
// func TestEventLog_Listen_Realtime(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// log, err := openEventLog(ctx.Logger(), OpenTestLogStash(ctx), uuid.NewV1())
// assert.Nil(t, err)
//
// commits := log.ListenCommits(0, 10)
// defer commits.Close()
//
// var item LogItem
// log.Append([]Event{Event{0}}, 1)
// log.Commit(0)
//
// item, _ = commits.Next()
// assert.Equal(t, LogItem{Index: 0, term: 1, Event: Event{0}}, item)
//
// log.Append([]Event{Event{1}}, 2)
// log.Commit(1)
//
// item, _ = commits.Next()
// assert.Equal(t, LogItem{Index: 1, term: 2, Event: Event{1}}, item)
// }
