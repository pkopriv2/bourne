package warden

import (
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestLine00(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(0)}
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, point{big.NewInt(0), big.NewInt(0)}, line.Point(big.NewInt(0)))
}

func TestLine10(t *testing.T) {
	line := line{big.NewInt(1), big.NewInt(0)}

	// x = 0
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, point{big.NewInt(0), big.NewInt(0)}, line.Point(big.NewInt(0)))

	// x = 1
	assert.Equal(t, big.NewInt(1), line.Height(big.NewInt(1)))
	assert.Equal(t, point{big.NewInt(1), big.NewInt(1)}, line.Point(big.NewInt(1)))
}

func TestLine0x(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(1024)}

	// x = 0
	assert.Equal(t, big.NewInt(1024), line.Height(big.NewInt(0)))
	assert.Equal(t, point{big.NewInt(0), big.NewInt(1024)}, line.Point(big.NewInt(0)))

	// x = 1
	assert.Equal(t, big.NewInt(1024), line.Height(big.NewInt(1)))
	assert.Equal(t, point{big.NewInt(1), big.NewInt(1024)}, line.Point(big.NewInt(1)))
}

func TestPoint00_Derive00(t *testing.T) {
	point := point{big.NewInt(0), big.NewInt(0)}
	_, err := point.Derive(point)
	assert.NotNil(t, err)
}

func TestPoint00_Derive01(t *testing.T) {
	point1 := point{big.NewInt(0), big.NewInt(0)}
	point2 := point{big.NewInt(1), big.NewInt(1)}

	expected := line{big.NewInt(1), big.NewInt(0)}
	derived1, err1 := point1.Derive(point2)
	assert.Nil(t, err1)
	assert.True(t, expected.Equals(derived1))

	derived2, err2 := point2.Derive(point1)
	assert.Nil(t, err2)
	assert.True(t, expected.Equals(derived2))
}

func TestLineRandRand(t *testing.T) {
	source := rand.New(rand.NewSource(1))

	line, err := generateLine(source, 16)
	assert.Nil(t, err)

	randomX1, _ := generateBigInt(source, 16)
	randomX2, _ := generateBigInt(source, 16)

	point1 := line.Point(randomX1)
	point2 := line.Point(randomX2)

	derived, err := point1.Derive(point2)
	assert.Nil(t, err)
	assert.Equal(t, line, derived)
}
