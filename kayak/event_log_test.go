package kayak

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestEventLog_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)
	assert.Equal(t, -1, log.Committed())
	assert.Equal(t, -1, log.Head())
}

func TestEventLog_Snapshot_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	head, term, commit, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, -1, head)
	assert.Equal(t, -1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_SingleBatch_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	idx, err := log.Append([]Event{Event{0}}, 1)
	assert.Nil(t, err)
	assert.Equal(t, 0, idx)

	head, term, commit, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 0, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_SingleBatch_MultiItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	idx, err := log.Append([]Event{Event{0}, Event{1}}, 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, idx)

	head, term, commit, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 1, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Append_MultiBatch_MultiItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Append([]Event{Event{0}, Event{1}}, 1)
	assert.Nil(t, err)

	idx, err := log.Append([]Event{Event{2}, Event{3}}, 1)
	assert.Nil(t, err)
	assert.Equal(t, 3, idx)

	head, term, commit, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 3, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Insert_Batch_GreaterThanHead(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Insert([]LogItem{LogItem{Index: 1, term: 1}})
	assert.Equal(t, OutOfBoundsError, errors.Cause(err))
}

func TestEventLog_Insert_SingleBatch_SingleItem(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	idx, err := log.Insert([]LogItem{LogItem{Index: 0, term: 1, Event: Event{0}}})
	assert.Nil(t, err)
	assert.Equal(t, 0, idx)

	head, term, commit, err := log.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 0, head)
	assert.Equal(t, 1, term)
	assert.Equal(t, -1, commit)
}

func TestEventLog_Get_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, found, err := log.Get(1)
	assert.Nil(t, err)
	assert.False(t, found)
}

func TestEventLog_Get_NotFound(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Append([]Event{Event{0}}, 1)
	assert.Nil(t, err)

	_, found, err := log.Get(1)
	assert.Nil(t, err)
	assert.False(t, found)
}

func TestEventLog_Get_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Append([]Event{Event{0}}, 1)
	assert.Nil(t, err)

	item, found, err := log.Get(0)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, 0, item.Index)
	assert.Equal(t, 1, item.term)
}

func TestEventLog_Scan_Empty(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	items, err := log.Scan(0, 1)
	assert.Equal(t, 0, len(items))
}

func TestEventLog_Scan_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Append([]Event{Event{0}}, 1)
	assert.Nil(t, err)

	items, err := log.Scan(0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{LogItem{Index: 0, term: 1, Event: Event{0}}}, items)
}

func TestEventLog_Scan_Middle(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	_, err = log.Append([]Event{Event{0}}, 1)
	assert.Nil(t, err)
	_, err = log.Append([]Event{Event{1}}, 1)
	assert.Nil(t, err)
	_, err = log.Append([]Event{Event{2}}, 1)
	assert.Nil(t, err)

	items, err := log.Scan(1, 1)
	assert.Nil(t, err)
	assert.Equal(t, []LogItem{LogItem{Index: 1, term: 1, Event: Event{1}}}, items)
}

func TestEventLog_Listen_LogClosed(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	l := log.ListenCommits(0, 1)
	assert.Nil(t, log.Close())

	_, e := l.Next()
	assert.Equal(t, ClosedError, e)
}

func TestEventLog_Listen_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	l := log.ListenCommits(0, 1)
	assert.Nil(t, l.Close())

	_, e := l.Next()
	assert.Equal(t, ClosedError, e)
}


func TestEventLog_Listen_Historical(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	log.Append([]Event{Event{0}}, 1)
	log.Append([]Event{Event{1}}, 1)
	log.Commit(1)

	l := log.ListenCommits(0, 0)
	defer l.Close()

	for i := 0; i < 2; i++ {
		item, err := l.Next()
		assert.Nil(t, err)
		assert.Equal(t, i, item.Index)
	}
}

func TestEventLog_Listen_Realtime(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	log, err := openEventLog(OpenTestLogStash(ctx), uuid.NewV1())
	assert.Nil(t, err)

	commits := log.ListenCommits(0, 10)
	defer commits.Close()

	var item LogItem
	log.Append([]Event{Event{0}}, 1)
	log.Commit(0)

	item, _ = commits.Next()
	assert.Equal(t, LogItem{Index: 0, term: 1, Event: Event{0}}, item)

	log.Append([]Event{Event{1}}, 2)
	log.Commit(1)

	item, _ = commits.Next()
	assert.Equal(t, LogItem{Index: 1, term: 2, Event: Event{1}}, item)
}
