package warden

import (
	"io"
	"math/big"

	"github.com/pkg/errors"
)

// TODO: Generalize to

type curvedKeyRing struct {
}

type line struct {
	Slope     *big.Int
	Intercept *big.Int
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Point(x *big.Int) point {
	return point{x, l.Height(x)}
}

func (l line) Bytes() []byte {
	return []byte{}
}

func randomLine(rand io.Reader, domain int) (line, error) {
	slope, err := randomBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	intercept, err := randomBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	return line{slope, intercept}, nil
}

func randomBigInt(rand io.Reader, num int) (*big.Int, error) {
	buf := make([]byte, num)
	if _, err := io.ReadFull(rand, buf); err != nil {
		return nil, errors.Wrapf(err, "Unable to allocate [%v] random bytes", num)
	}
	return new(big.Int).SetBytes(buf), nil
}

// A vector representation of a point in n-dimensional space
type point struct {
	X *big.Int
	Y *big.Int
}

func (p point) Derive(o point) line {
	slope := deriveSlope(p, o)
	return line{slope, deriveIntercept(p, slope)}
}

func deriveSlope(p1, p2 point) *big.Int {
	delX := big.NewInt(0)
	delX.Sub(p2.X, p1.X)

	delY := big.NewInt(0)
	delY.Sub(p2.Y, p1.Y)

	return delY.Div(delY, delX)
}

func deriveIntercept(p point, slope *big.Int) *big.Int {
	delY := big.NewInt(0)
	delY.Mul(p.X, slope)

	ret := big.NewInt(0)
	return ret.Sub(p.Y, delY)
}
