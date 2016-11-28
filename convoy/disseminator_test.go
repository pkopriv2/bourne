package convoy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDisseminator_NumTransmissions_One(t *testing.T) {
	assert.Equal(t, 0, dissemFanout(1))
}

func TestDisseminator_NumTransmissions_Two(t *testing.T) {
	assert.Equal(t, 1, dissemFanout(2))
}

func TestDisseminator_NumTransmissions_100(t *testing.T) {
	assert.Equal(t, 7, dissemFanout(100))
}
