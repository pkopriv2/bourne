package kayak

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/stretchr/testify/assert"
)

func TestEventLog_Committed_Empty(t *testing.T) {
	log := NewTestEventLog()
	assert.Equal(t, -1, log.Committed())
}

func TestEventLog_Head_Empty(t *testing.T) {
	log := NewTestEventLog()
	assert.Equal(t, -1, log.Head())
}

func TestEventLog_Commit_Greater_Than_Head(t *testing.T) {
	log := NewTestEventLog()
	assert.Panics(t, func() { log.Commit(1) })
}

func TestEventLog_Snapshot_Empty(t *testing.T) {
	log := NewTestEventLog()
	head, term, commit := log.Snapshot()
	assert.Equal(t, -1, head)
	assert.Equal(t, -1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_SingleBatch_SingleItem(t *testing.T) {
	log := NewTestEventLog()
	idx := log.Append([]event{&testEvent{}}, 1)
	assert.Equal(t, 0, idx)

	head, term, commit := log.Snapshot()
	assert.Equal(t, 0, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_SingleBatch_MultiItem(t *testing.T) {
	log := NewTestEventLog()
	idx := log.Append([]event{&testEvent{}, &testEvent{}}, 1)
	assert.Equal(t, 1, idx)

	head, term, commit := log.Snapshot()
	assert.Equal(t, 1, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_MultiBatch_MultiItem(t *testing.T) {
	log := NewTestEventLog()
	log.Append([]event{&testEvent{}, &testEvent{}}, 1)
	idx := log.Append([]event{&testEvent{}, &testEvent{}}, 1)
	assert.Equal(t, 3, idx)

	head, term, commit := log.Snapshot()
	assert.Equal(t, 3, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Insert_SingleBatch_SingleItem(t *testing.T) {
	log := NewTestEventLog()
	log.Insert([]event{&testEvent{}}, 1, 1)

	head, term, commit := log.Snapshot()
	assert.Equal(t, 1, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Get_Empty(t *testing.T) {
	log := NewTestEventLog()
	_, found := log.Get(1)
	assert.False(t, found)
}

func TestEventLog_Get_NotFound(t *testing.T) {
	log := NewTestEventLog()
	log.Append([]event{&testEvent{}}, 1)
	_, found := log.Get(1)
	assert.False(t, found)
}

func TestEventLog_Get_Single(t *testing.T) {
	log := NewTestEventLog()
	log.Append([]event{&testEvent{}}, 1)
	item, found := log.Get(0)
	assert.True(t, found)
	assert.Equal(t, 0, item.index)
	assert.Equal(t, 1, item.term)
}

func TestEventLog_Scan_Empty(t *testing.T) {
	log := NewTestEventLog()
	evts := log.Scan(0, 1)
	assert.Equal(t, 0, len(evts))
}

func TestEventLog_Scan_Single(t *testing.T) {
	log := NewTestEventLog()
	log.Append([]event{&testEvent{}}, 1)
	evts := log.Scan(0, 1)
	assert.Equal(t, 1, len(evts))
}

func TestEventLog_Scan_Middle(t *testing.T) {
	log := NewTestEventLog()
	log.Append([]event{&testEvent{}}, 1)
	log.Append([]event{&testEvent{}}, 1)
	log.Append([]event{&testEvent{}}, 1)
	evts := log.Scan(1, 1)
	assert.Equal(t, 1, len(evts))
}

func NewTestEventLog() *eventLog {
	return newEventLog(common.NewContext(common.NewEmptyConfig()))
}

var i int = 0
var lock sync.Mutex

type testEvent struct {
	i int
}

func newTestEvent() *testEvent {
	lock.Lock()
	defer lock.Unlock()
	i++

	return &testEvent{i}
}

func (e *testEvent) Write(w scribe.Writer) {
	w.WriteString("type", "testevent")
	w.WriteInt("i", e.i)
}

func (e *testEvent) String() string {
	return fmt.Sprintf("Event(%v)", e.i)
}

func testEventParser(r scribe.Reader) (event, error) {
	evt := &testEvent{}
	err := r.ReadInt("i", &evt.i)
	return evt, err
}
