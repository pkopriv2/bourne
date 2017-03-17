package warden

import (
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLine00_Height(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(0)}
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
}

func TestLine0x_Height(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(1024)}

	random1,_ := randomBigInt(rand.New(rand.NewSource(1)), 1)
	random2,_ := randomBigInt(rand.New(rand.NewSource(2)), 1)
	random3,_ := randomBigInt(rand.New(rand.NewSource(3)), 1)
	assert.Equal(t, line.Intercept, line.Height(random1))
	assert.Equal(t, line.Intercept, line.Height(random2))
	assert.Equal(t, line.Intercept, line.Height(random3))
}

func TestLine10_Height(t *testing.T) {
	line := line{big.NewInt(1), big.NewInt(0)}
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, big.NewInt(1), line.Height(big.NewInt(1)))
	assert.Equal(t, big.NewInt(2), line.Height(big.NewInt(2)))
}

func TestPoint00_Derive01(t *testing.T) {
	point1 := point{big.NewInt(0), big.NewInt(0)}
	point2 := point{big.NewInt(0), big.NewInt(1)}

}
