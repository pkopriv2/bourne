package elmer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Equals_DiffElem(t *testing.T) {
	seg1 := segment{[]byte("one"), 0}
	seg2 := segment{[]byte("two"), 0}
	assert.False(t, seg1.Equals(seg2))
}

func TestSegment_Equals_DiffVersion(t *testing.T) {
	seg1 := segment{[]byte("one"), 0}
	seg2 := segment{[]byte("one"), 1}
	assert.False(t, seg1.Equals(seg2))
}

func TestSegment_Equals(t *testing.T) {
	seg1 := segment{[]byte("one"), 1}
	seg2 := segment{[]byte("one"), 1}
	assert.True(t, seg1.Equals(seg2))
}

func TestPath_Equals_DiffLength(t *testing.T) {
	path1 := emptyPath.Child([]byte("one"), 1).Child([]byte("two"), 2)
	path2 := emptyPath.Child([]byte("one"), 2)
	assert.False(t, path1.Equals(path2))
}

func TestPath_Equals_DiffTail(t *testing.T) {
	path1 := emptyPath.Child([]byte("one"), 1).Child([]byte("two"), 2)
	path2 := emptyPath.Child([]byte("one"), 1).Child([]byte("two"), 3)
	assert.False(t, path1.Equals(path2))
}

func TestPath_Equals(t *testing.T) {
	path1 := emptyPath.Child([]byte("one"), 1).Child([]byte("two"), 2)
	path2 := emptyPath.Child([]byte("one"), 1).Child([]byte("two"), 2)
	assert.True(t, path1.Equals(path2))
}
