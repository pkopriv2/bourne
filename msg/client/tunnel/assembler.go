package tunnel

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pkopriv2/bourne/msg/wire"
	"github.com/pkopriv2/bourne/utils"
)

func NewAssembler(env *Env, in chan wire.SegmentMessage, out chan []byte) func(utils.StateController, []interface{}) {

	return func(state utils.StateController, args []interface{}) {
		pending := NewPendingSegments(env.conf.AssemblerMax)

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
				pending.Add(curIn.Offset(), curIn.Data())
			}

			curOut = pending.Next()
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

func (a *PendingSegments) Next() []byte {
	for {
		k, v := a.sorted.Min()
		if k == nil || v == nil {
			panic("Cannot have nil keys or values")
		}

		// Handle: Future segment
		offset, data := k.(uint64), v.([]byte)
		if offset > a.offset {
			break
		}

		// Handle: Past segment
		a.sorted.Remove(offset)
		if a.offset > offset+uint64(len(data)) {
			continue
		}

		data = data[a.offset-offset:]
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
