package secret

import (
	"math/big"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestLine00(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(0)}
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, Point{big.NewInt(0), big.NewInt(0)}, line.Point(big.NewInt(0)))
}

func TestLine10(t *testing.T) {
	line := line{big.NewInt(1), big.NewInt(0)}

	// x = 0
	assert.Equal(t, big.NewInt(0), line.Height(big.NewInt(0)))
	assert.Equal(t, Point{big.NewInt(0), big.NewInt(0)}, line.Point(big.NewInt(0)))

	// x = 1
	assert.Equal(t, big.NewInt(1), line.Height(big.NewInt(1)))
	assert.Equal(t, Point{big.NewInt(1), big.NewInt(1)}, line.Point(big.NewInt(1)))
}

func TestLine0x(t *testing.T) {
	line := line{big.NewInt(0), big.NewInt(1024)}

	// x = 0
	assert.Equal(t, big.NewInt(1024), line.Height(big.NewInt(0)))
	assert.Equal(t, Point{big.NewInt(0), big.NewInt(1024)}, line.Point(big.NewInt(0)))

	// x = 1
	assert.Equal(t, big.NewInt(1024), line.Height(big.NewInt(1)))
	assert.Equal(t, Point{big.NewInt(1), big.NewInt(1024)}, line.Point(big.NewInt(1)))
}

func TestPoint00_Derive00(t *testing.T) {
	Point := Point{big.NewInt(0), big.NewInt(0)}
	_, err := Point.Derive(Point)
	assert.NotNil(t, err)
}

func TestPoint00_Derive01(t *testing.T) {
	Point1 := Point{big.NewInt(0), big.NewInt(0)}
	Point2 := Point{big.NewInt(1), big.NewInt(1)}

	expected := line{big.NewInt(1), big.NewInt(0)}
	derived1, err1 := Point1.Derive(Point2)
	assert.Nil(t, err1)
	assert.True(t, expected.Equals(derived1))

	derived2, err2 := Point2.Derive(Point1)
	assert.Nil(t, err2)
	assert.True(t, expected.Equals(derived2))
}

func TestLineRandRand(t *testing.T) {
	source := rand.New(rand.NewSource(1))

	line, err := generateLine(source, 16)
	assert.Nil(t, err)

	randomX1, _ := generateBigInt(source, 16)
	randomX2, _ := generateBigInt(source, 16)

	Point1 := line.Point(randomX1)
	Point2 := line.Point(randomX2)

	derived, err := Point1.Derive(Point2)
	assert.Nil(t, err)
	assert.Equal(t, line, derived)
}
