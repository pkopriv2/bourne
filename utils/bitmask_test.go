package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitMask_Empty(t *testing.T) {
	mask := EmptyBitMask
	assert.False(t, mask.Matches(0))
}

func TestBitMask_SingleBit(t *testing.T) {
	mask := BitMask(1)
	assert.False(t, mask.Matches(2))
	assert.True(t, mask.Matches(1))
}

func TestBitMask_MultiBit(t *testing.T) {
	mask := BitMask(0x11)

	assert.False(t, mask.Matches(BitMask(^uint8(0x11))))
	assert.True(t, mask.Matches(0x01))
	assert.True(t, mask.Matches(0x10))
	assert.True(t, mask.Matches(0x11))
}

func TestSplitMask_Zero(t *testing.T) {
	assert.Equal(t, []BitMask{}, SplitMask(0))
}

func TestSplitMask_One(t *testing.T) {
	assert.Equal(t, []BitMask{1}, SplitMask(1))
}

func TestSplitMask_Two(t *testing.T) {
	assert.Equal(t, []BitMask{2}, SplitMask(2))
}

func TestSplitMask_Multi(t *testing.T) {
	assert.Equal(t, []BitMask{1,2}, SplitMask(3))
}

func TestSplitMask_Multi_Spread(t *testing.T) {
	assert.Equal(t, []BitMask{1,256}, SplitMask(257))
}
