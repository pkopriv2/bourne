package convoy

import (
	"testing"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
	"github.com/stretchr/testify/assert"
)

func TestEventLog_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)
	assert.Nil(t, el.Close())
}

func TestEventLog_Push_Process_SingleSuccess(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push(evt, 1)

	el.Process(func(b []event) error {
		assert.Equal(t, 1, len(b))
		assert.Equal(t, evt, b[0])
		return nil
	})

	el.Process(func(b []event) error {
		assert.Equal(t, []event{}, b)
		return nil
	})
}

func TestEventLog_Push_Process_MultipleSuccess(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push(evt, 2)

	el.Process(func(b []event) error {
		assert.Equal(t, 1, len(b))
		assert.Equal(t, evt, b[0])
		return nil
	})

	el.Process(func(b []event) error {
		assert.Equal(t, 1, len(b))
		assert.Equal(t, evt, b[0])
		return nil
	})

	el.Process(func(b []event) error {
		assert.Equal(t, []event{}, b)
		return nil
	})
}

func TestEventLog_Push_Process_MultipleItems(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push(evt, 1)
	el.Push(evt, 1)

	el.Process(func(b []event) error {
		assert.Equal(t, 2, len(b))
		assert.Equal(t, evt, b[0])
		assert.Equal(t, evt, b[1])
		return nil
	})

	el.Process(func(b []event) error {
		assert.Equal(t, []event{}, b)
		return nil
	})
}

func TestEventLog_Push_Process_ConcurrentProcesses(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newEventLog(ctx)
	defer el.Close()

	start := make(chan struct{}, 1024)
	go func() {
		for i := 0; i < 1024; i++ {
			start <- struct{}{}
			el.Push(&testEvent{}, 8)
		}
	}()

	<-start

	count := 0
	for cont := true; cont; {
		el.Process(func(b []event) error {
			count = count + len(b)
			cont = count < 1024*8
			return nil
		})
	}

	// not really necessary.  this test won't finish if we don't process them all.
	assert.Equal(t, 1024*8, count)
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

