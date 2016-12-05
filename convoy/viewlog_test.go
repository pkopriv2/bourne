package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/stretchr/testify/assert"
)

func TestViewLog_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newViewLog(ctx)
	assert.Nil(t, el.Close())
}

func TestViewLog_Push_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newViewLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push([]event{evt}, 1)

	batch := el.Peek(10)
	assert.Equal(t, 1, len(batch))
	assert.Equal(t, evt, batch[0])
}

type testEvent struct{}

func (e *testEvent) Write(w scribe.Writer) {
}

func (e *testEvent) Apply(tx *dirUpdate) bool {
	return true
}

func (e *testEvent) String() string {
	return "Test event"
}
