package convoy

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/stretchr/testify/assert"
)

func TestTimeLog_Close(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newTimeLog(ctx)
	assert.Nil(t, el.Close())
}

func TestTimeLog_Push_Single(t *testing.T) {
	ctx := common.NewContext(common.NewEmptyConfig())
	defer ctx.Close()

	el := newTimeLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push([]event{evt})

	batch := el.Peek(time.Now().Add(-100 * time.Second))
	assert.Equal(t, 1, len(batch))
	assert.Equal(t, evt, batch[0])
}

func TestTimeLog_Push_OlderThanHorizon(t *testing.T) {
	ctx := common.NewContext(common.NewConfig(map[string]interface{}{
		ConfTimeLogHorizon: 100 * time.Millisecond,
	}))

	el := newTimeLog(ctx)
	defer el.Close()

	evt := &testEvent{}
	el.Push([]event{evt})

	time.Sleep(200* time.Millisecond)
	batch := el.Peek(time.Now().Add(-100 * time.Second))
	assert.Equal(t, 0, len(batch))
}
