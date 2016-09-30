package tunnel

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pkopriv2/bourne/utils"
)

func NewRecvAssembler(env *tunnelEnv, channels *tunnelChannels) func(utils.Controller, []interface{}) {
	return func(state utils.Controller, args []interface{}) {
		defer env.logger.Info("Assembler closing")

		pending := NewPendingSegments(env.config.AssemblerLimit)

		var outChan chan<- []byte
		var outSegment []byte
		for {
			outSegment = pending.Next()
			if outSegment == nil {
				outChan = nil
			} else {
				outChan = channels.bufferer
			}

			select {
			case <-state.Close():
				return
			case outChan <- outSegment:
			case curIn := <-channels.assembler:
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
