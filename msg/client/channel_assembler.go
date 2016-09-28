package client

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

const (
	confChannelRecvAssemblerSize = "bourne.msg.channel.recv.assembler.size"
)

const (
	defaultChannelRecvAssemblerSize = 1 << 20 // 1 MB
)

func NewAssemblerWorker(config utils.Config, in chan wire.SegmentMessage, out chan []byte) func(utils.StateController, []interface{}) {
	return func(state utils.StateController, args []interface{}) {
		assembler := NewAssembler(config.OptionalInt(confChannelRecvAssemblerSize, defaultChannelRecvAssemblerSize))

		chanIn := in
		chanOut := out

		var curOut []byte
		for {
			if curOut == nil {
				chanOut = nil
			}

			select {
			case <-state.Done():
				return
			case chanOut <- curOut:
			case curIn := <-chanIn:
				assembler.Add(curIn.Offset(), curIn.Data())
			}

			curOut = assembler.Next()
		}
	}
}

type Assembler struct {
	sorted *treemap.Map
	limit  int
	offset uint64
}

func NewAssembler(limit int) *Assembler {
	return &Assembler{
		sorted: treemap.NewWith(OffsetComparator),
		limit:  limit}
}

func (a *Assembler) Next() []byte {
	out := make([]byte, 0, 4096)
	for {
		k, v := a.sorted.Min()
		if k == nil || v == nil {
			panic("Cannot have nil keys or values")
		}

		// Handle: Future offset
		offset, data := k.(uint64), v.([]byte)
		if offset > a.offset {
			break
		}

		// Handle: Past exclusive offset
		a.sorted.Remove(offset)
		if a.offset > offset+uint64(len(data)) {
			continue
		}

		data = data[a.offset-offset:]
		a.offset += uint64(len(data))
		out = append(out, data...)
	}

	if len(out) == 0 {
		return nil
	} else {
		return out
	}
}

func (a *Assembler) Add(offset uint64, data []byte) {

	// if this puts us over the limit, prune
	if a.sorted.Size()+1 > a.limit {
		k, _ := a.sorted.Max()
		a.sorted.Remove(k)
	}

	a.sorted.Put(offset, data)
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
