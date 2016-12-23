package kayak

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

func TestEventLog_Push_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)

	evt := &testEvent{}
	el.Append([]event{evt}, 1)

	// // batch := el.Peek(10)
	// assert.Equal(t, 1, len(batch))
	// assert.Equal(t, evt, batch[0])
}

// func TestEventLog_Push_Single(t *testing.T) {
// ctx := common.NewContext(common.NewEmptyConfig())
// defer ctx.Close()
//
// el := newEventLog(ctx)
//
// evt := &testEvent{}
// el.Push([]event{evt}, 1)
//
// batch := el.Peek(10)
// assert.Equal(t, 1, len(batch))
// assert.Equal(t, evt, batch[0])
// }

type testEvent struct{}

func (e *testEvent) Write(w scribe.Writer) {
}

func (e *testEvent) String() string {
	return "Test event"
}
