package tunnel

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/message/wire"
	"github.com/pkopriv2/bourne/utils"
)

const (
	confTunnelAssemblerLimit = "bourne.msg.client.tunnel.assembler.limit"
)

const (
	defaultTunnelAssemblerLimit = 1024
)

type AssemblerSocket struct {
	SegmentRx <-chan wire.SegmentMessage
	SegmentTx chan<- []byte
	VerifyTx  chan<- wire.NumMessage
}

func NewRecvAssembler(ctx common.Context, socket *AssemblerSocket) func(utils.WorkerController, []interface{}) {
	logger := ctx.Logger()
	config := ctx.Config()

	return func(state utils.WorkerController, args []interface{}) {
		logger.Debug("RecvAssembler Opening")
		defer logger.Debug("RecvAssembler Opening")

		pending := NewPendingSegments(config.OptionalInt(confTunnelAssemblerLimit, defaultTunnelAssemblerLimit))

		var segmentTx chan<- []byte
		var verifyTx chan<- wire.NumMessage

		var outSegment []byte
		var outVerify wire.NumMessage
		for {
			if outSegment == nil && outVerify == nil {
				if outSegment = pending.Take(); outSegment != nil {
					outVerify = wire.NewNumMessage(pending.Offset())
				}
			}

			if outSegment != nil {
				segmentTx = socket.SegmentTx
			} else {
				segmentTx = nil
			}

			if outVerify != nil {
				verifyTx = socket.VerifyTx
			} else {
				verifyTx = nil
			}

			select {
			case <-state.Close():
				return
			case segmentTx <- outSegment:
				outSegment = nil
			case verifyTx <- outVerify:
				outVerify = nil
			case curIn := <-socket.SegmentRx:
				pending.Add(curIn.Offset(), curIn.Data())
			}
		}
	}
}

func OffsetComparator(a, b interface{}) int {
	offsetA := a.(uint64)
	offsetB := b.(uint64)

	if offsetA > offsetB {
		return 1
	}

	if offsetA == offsetB {
		return 0
	}

	return -1
}

type PendingSegments struct {
	sorted *treemap.Map
	limit  int
	offset uint64
}

func NewPendingSegments(limit int) *PendingSegments {
	return &PendingSegments{
		sorted: treemap.NewWith(OffsetComparator),
		limit:  limit}
}

func (a *PendingSegments) Offset() uint64 {
	return a.offset
}

func (a *PendingSegments) Take() []byte {
	tmp := make([]byte, 0, 4096)
	for cur := a.Next(); cur != nil; cur = a.Next() {
		tmp = append(tmp, cur...)
	}

	if len(tmp) == 0 {
		return nil
	}

	return tmp
}

func (a *PendingSegments) Next() []byte {
	for {
		k, v := a.sorted.Min()
		if k == nil || v == nil {
			return nil
		}

		// Handle: Future segment
		offset, data := k.(uint64), v.([]byte)
		if offset > a.offset {
			return nil
		}

		// Handle: Past segment
		a.sorted.Remove(offset)
		if a.offset-offset > uint64(len(data)) {
			continue
		}

		data = data[a.offset-offset:]
		if len(data) == 0 {
			return nil
		}

		a.offset += uint64(len(data))
		return data
	}

	return nil
}

func (a *PendingSegments) Add(offset uint64, data []byte) {

	// if this puts us over the limit, prune
	if a.sorted.Size()+1 > a.limit {
		k, _ := a.sorted.Max()
		a.sorted.Remove(k)
	}

	a.sorted.Put(offset, data)
}
