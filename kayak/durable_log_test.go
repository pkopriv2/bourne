package kayak

import (
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
	"github.com/stretchr/testify/assert"
)

func OpenTestLogStash(ctx common.Context) stash.Stash {
	db := OpenTestStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		return initBuckets(tx)
	})
	return db
}

func OpenTestDurableLog(ctx common.Context) durableLog {
	return durableLog{}
}

func TestDurableLog_CreateSnapshot_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		s, err := createDurableSnapshot(tx, []Event{}, []byte{})
		if err != nil {
			panic(err)
		}

		snapshot, ok, err := openDurableSnapshot(tx, s.id)
		if err != nil {
			panic(err)
		}

		assert.True(t, ok)
		assert.Equal(t, s.id, snapshot.id)
		assert.Equal(t, s.Config(), snapshot.Config())

		events, err := snapshot.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, []Event{}, events)
		return nil
	})
}

func TestDurableLog_CreateSnapshot_Config(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []byte{0,1,2}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot, err := createDurableSnapshot(tx, []Event{}, expected)
		if err != nil {
			panic(err)
		}

		assert.NotNil(t, snapshot.id)
		assert.Equal(t, expected, snapshot.Config())
		return nil
	})
}

func TestDurableLog_CreateSnapshot_Events(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected := []Event{[]byte{0,1},[]byte{0,1}}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot, err := createDurableSnapshot(tx, expected, []byte{})
		if err != nil {
			panic(err)
		}

		assert.NotNil(t, snapshot.id)
		assert.Equal(t, []byte{}, snapshot.Config())

		events, err := snapshot.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, expected, events)
		return nil
	})
}

func TestDurableLog_CreateSnapshot_MultipleWithEvents(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	expected1 := []Event{[]byte{0,1},[]byte{2,3}}
	expected2 := []Event{[]byte{0,1,2},[]byte{3},[]byte{4,5}}

	db := OpenTestLogStash(ctx)
	db.Update(func(tx *bolt.Tx) error {
		snapshot1, err := createDurableSnapshot(tx, expected1, []byte{})
		if err != nil {
			panic(err)
		}

		snapshot2, err := createDurableSnapshot(tx, expected2, []byte{})
		if err != nil {
			panic(err)
		}

		events1, err := snapshot1.Events(tx)
		if err != nil {
			panic(err)
		}

		events2, err := snapshot2.Events(tx)
		if err != nil {
			panic(err)
		}

		assert.Equal(t, expected1, events1)
		assert.Equal(t, expected2, events2)
		return nil
	})
}

//
// func TestEventLog_Head_Empty(t *testing.T) {
// log := NewTestEventLog()
// assert.Equal(t, -1, log.Head())
// }
//
// func TestEventLog_Snapshot_Empty(t *testing.T) {
// log := NewTestEventLog()
// head, term, commit := log.Snapshot()
// assert.Equal(t, -1, head)
// assert.Equal(t, -1, term)
// assert.Equal(t, -1, commit)
// }
//
// func TestEventLog_Append_SingleBatch_SingleItem(t *testing.T) {
// log := NewTestEventLog()
// idx := log.Append([]Event{&testEvent{}}, 1)
// assert.Equal(t, 0, idx)
//
// head, term, commit := log.Snapshot()
// assert.Equal(t, 0, head)
// assert.Equal(t, 1, term)
// assert.Equal(t, -1, commit)
// }
//
// func TestEventLog_Append_SingleBatch_MultiItem(t *testing.T) {
// log := NewTestEventLog()
// idx := log.Append([]Event{&testEvent{}, &testEvent{}}, 1)
// assert.Equal(t, 1, idx)
//
// head, term, commit := log.Snapshot()
// assert.Equal(t, 1, head)
// assert.Equal(t, 1, term)
// assert.Equal(t, -1, commit)
// }
//
// func TestEventLog_Append_MultiBatch_MultiItem(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}, &testEvent{}}, 1)
// idx := log.Append([]Event{&testEvent{}, &testEvent{}}, 1)
// assert.Equal(t, 3, idx)
//
// head, term, commit := log.Snapshot()
// assert.Equal(t, 3, head)
// assert.Equal(t, 1, term)
// assert.Equal(t, -1, commit)
// }
//
// func TestEventLog_Insert_SingleBatch_SingleItem(t *testing.T) {
// log := NewTestEventLog()
// log.Insert([]LogItem{LogItem{Index: 1, term: 1}})
//
// head, term, commit := log.Snapshot()
// assert.Equal(t, 1, head)
// assert.Equal(t, 1, term)
// assert.Equal(t, -1, commit)
// }
//
// func TestEventLog_Get_Empty(t *testing.T) {
// log := NewTestEventLog()
// _, found := log.Get(1)
// assert.False(t, found)
// }
//
// func TestEventLog_Get_NotFound(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// _, found := log.Get(1)
// assert.False(t, found)
// }
//
// func TestEventLog_Get_Single(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// item, found := log.Get(0)
// assert.True(t, found)
// assert.Equal(t, 0, item.Index)
// assert.Equal(t, 1, item.term)
// }
//
// func TestEventLog_Scan_Empty(t *testing.T) {
// log := NewTestEventLog()
// items := log.Scan(0, 1)
// assert.Equal(t, 0, len(items))
// }
//
// func TestEventLog_Scan_Single(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// evts := log.Scan(0, 1)
// assert.Equal(t, 1, len(evts))
// }
//
// func TestEventLog_Scan_Middle(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// log.Append([]Event{&testEvent{}}, 1)
// log.Append([]Event{&testEvent{}}, 1)
// evts := log.Scan(1, 2)
// assert.Equal(t, 2, len(evts))
// }
//
// func TestEventLog_Listen_LogClosed(t *testing.T) {
// log := NewTestEventLog()
// l := log.ListenCommits(0, 1)
// assert.Nil(t, log.Close())
//
// time.Sleep(10 * time.Millisecond)
// select {
// case <-l.Closed():
// default:
// assert.Fail(t, "Not closed")
// }
// }
//
// func TestEventLog_Listen_Close(t *testing.T) {
// log := NewTestEventLog()
// l := log.ListenCommits(0, 1)
// assert.Nil(t, l.Close())
//
// select {
// case <-l.Closed():
// default:
// assert.Fail(t, "Not closed")
// }
// }
//
// func TestEventLog_Listen_Historical(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// log.Append([]Event{&testEvent{}}, 1)
// log.Commit(1)
//
// l := log.ListenCommits(0, 1)
// defer l.Close()
//
// for i := 0; i < 2; i++ {
// time.Sleep(10 * time.Millisecond)
// select {
// default:
// assert.FailNow(t, fmt.Sprintf("Missing item: %v", i))
// case item := <-l.Items():
// assert.Equal(t, i, item.Index)
// }
// }
// }
//
// func TestEventLog_Listen_Realtime(t *testing.T) {
// log := NewTestEventLog()
//
// commits := log.ListenCommits(0, 10)
// defer commits.Close()
//
// var item LogItem
// assert.Equal(t, 0, log.Append([]Event{&testEvent{}}, 1))
// log.Commit(0)
// item = <-commits.Items()
// assert.Equal(t, 0, item.Index)
//
// assert.Equal(t, 1, log.Append([]Event{&testEvent{}}, 1))
// log.Commit(1)
// item = <-commits.Items()
// assert.Equal(t, 1, item.Index)
//
// time.Sleep(100 * time.Millisecond)
// select {
// default:
// case <-commits.Items():
// assert.FailNow(t, fmt.Sprintf("Missing item: %v"))
// }
// }
//
// func TestEventLog_Listen_Compact(t *testing.T) {
// log := NewTestEventLog()
// log.Append([]Event{&testEvent{}}, 1)
// log.Append([]Event{&testEvent{}}, 1)
// log.Commit(1)
//
// l := log.ListenCommits(0, 1)
// defer l.Close()
//
// log.Append([]Event{&testEvent{}}, 1)
// log.Commit(2)
//
// for i := 0; i < 3; i++ {
// time.Sleep(10 * time.Millisecond)
// select {
// default:
// assert.FailNow(t, fmt.Sprintf("Missing item: %v", i))
// case item := <-l.Items():
// assert.Equal(t, i, item.Index)
// }
// }
// }
//
// func NewTestEventLog() *eventLog {
// return newEventLog(common.NewContext(common.NewEmptyConfig()))
// }

// type testEvent struct {
// }
//
// func newTestEvent() *testEvent {
// return &testEvent{}
// }
//
// func (e *testEvent) Write(w scribe.Writer) {
// w.WriteString("type", "testevent")
// }
//
// func (e *testEvent) String() string {
// return "TestEvent"
// }
//
// func testEventParser(r scribe.Reader) (Event, error) {
// return &testEvent{},nil
// }
