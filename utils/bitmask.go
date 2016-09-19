package utils

import "fmt"

// A simple bit masking utility.
type BitMask uint64

const (
	EmptyBitMask BitMask = 0
)

func (b BitMask) Matches(flag BitMask) bool {
	return b&flag > 0
}

func (b BitMask) Equals(flag BitMask) bool {
	return b == flag
}

func SingleValueMask(val uint64) BitMask {
	mask := BitMask(val)
	if len(SplitMask(mask)) > 1 {
		panic(fmt.Sprintf("Invalid single value bit mask [%v].  Must have at most one bit set", val))
	}

	return mask
}

func SplitMask(val BitMask) []BitMask {
	if val == 0 {
		return []BitMask{}
	}

	var max uint
	switch {
	default:
		max = 64
	case val <= 1<<(8*1):
		max = 8
	case val <= 1<<(8*2):
		max = 8 * 2
	case val <= 1<<(8*3):
		max = 8 * 3
	case val <= 1<<(8*4):
		max = 8 * 4
	case val <= 1<<(8*5):
		max = 8 * 5
	case val <= 1<<(8*6):
		max = 8 * 6
	case val <= 1<<(8*7):
		max = 8 * 7
	}

	ret := make([]BitMask, 0, max)
	for i := uint(0); i < max; i, val = i+1, val>>1 {
		if val&0x01 == 0 {
			continue
		}

		ret = append(ret, BitMask(1<<i))
	}

	return ret
}
