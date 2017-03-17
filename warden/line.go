package warden

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/big"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/scribe"
)

// func randomBigInt(io io.Reader, size int) (*big.Int,error) {
// raw, err := generateRandomBytes(size)
// if err != nil {
// return nil, errors.Wrapf("Error generating random int of %v bytes", size)
// }
// }

type line struct {
	Slope     *big.Int
	Intercept *big.Int
}

func (l line) Destroy() {
	l.Slope.SetBytes(big.NewInt(0).Bytes())
	l.Intercept.SetBytes(big.NewInt(0).Bytes())
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Point(x *big.Int) point {
	return point{x, l.Height(x)}
}

// Cannon use typical field oriented approaches here.  Must be totally deterministic impl
func (l line) Bytes() []byte {
	buf := &bytes.Buffer{}
	writer := scribe.NewStreamWriter(bufio.NewWriter(buf))
	writer.PutBytes(l.Slope.Bytes())
	writer.PutBytes(l.Intercept.Bytes())
	writer.Flush()
	return buf.Bytes()
}

func parseLineBytes(raw []byte) (line, error) {
	reader := scribe.NewStreamReader(bufio.NewReader(bytes.NewBuffer(raw)))
	sBytes := reader.ReadBytes()
	iBytes := reader.ReadBytes()
	if err := reader.Err(); err != nil {
		return line{}, errors.WithStack(err)
	}

	slope, intercept := new(big.Int), new(big.Int)
	return line{slope.SetBytes(sBytes), intercept.SetBytes(iBytes)}, nil
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

func (p point) Derive(o point) (line, error) {
	if p.Equals(o) {
		return line{}, errors.Errorf("Cannot derive a line from the same points [%v,%v]", o, p)
	}
	slope := deriveSlope(p, o)
	return line{slope, deriveIntercept(p, slope)}, nil
}

func (p point) Equals(o point) bool {
	return (p.X.Cmp(o.X) == 0) && (p.Y.Cmp(o.Y) == 0)
}

func (p point) String() string {
	return fmt.Sprintf("Point(%v,%v)", p.X, p.Y)
}

func (p point) Bytes() []byte {
	buf := &bytes.Buffer{}
	writer := scribe.NewStreamWriter(bufio.NewWriter(buf))
	writer.PutBytes(p.X.Bytes())
	writer.PutBytes(p.Y.Bytes())
	writer.Flush()
	return buf.Bytes()
}

func parsePointBytes(raw []byte) (point, error) {
	reader := scribe.NewStreamReader(bufio.NewReader(bytes.NewBuffer(raw)))
	xBytes := reader.ReadBytes()
	yBytes := reader.ReadBytes()
	if err := reader.Err(); err != nil {
		return point{}, errors.WithStack(err)
	}

	x, y := new(big.Int), new(big.Int)
	return point{x.SetBytes(xBytes), y.SetBytes(yBytes)}, nil
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
