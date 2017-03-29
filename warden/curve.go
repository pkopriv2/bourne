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

// TODO: Generalize this for curves of n-degrees. (ie: lines, parabolas, cubics, quartics, etc...)

// Generates a random line.  The domain is used to determine the number of bytes to use when generating
// the properties of the curve.
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

// Generates a random point on the line.  The domain is used to bound the size of the resulting point.
func randomPoint(rand io.Reader, line line, domain int) (point, error) {
	x, err := randomBigInt(rand, domain)
	if err != nil {
		return point{}, errors.Wrapf(err, "Unable to generate point on line [%v] for domain [%v]", line, domain)
	}
	return line.Point(x), nil
}

// TODO: Is generating a random byte array consistent with generating a random integer?
//
// Generates a random integer using the size to determine the number of bytes to use when generating the
// random value.
func randomBigInt(rand io.Reader, size int) (*big.Int, error) {
	buf, err := generateRandomBytes(rand, size)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to generate random *big.Int of size [%v]", size)
	}

	return new(big.Int).SetBytes(buf), nil
}

type line struct {
	Slope     *big.Int
	Intercept *big.Int
}

func (l line) Destroy() {
	sBytes := l.Slope.Bytes()
	iBytes := l.Intercept.Bytes()

	Bytes(sBytes).Destroy()
	Bytes(iBytes).Destroy()

	l.Slope.SetBytes(sBytes)
	l.Slope.SetBytes(iBytes)
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Point(x *big.Int) point {
	return point{x, l.Height(x)}
}

func (l line) Equals(o line) bool {
	return (l.Slope.Cmp(o.Slope) == 0) && (l.Intercept.Cmp(o.Intercept) == 0)
}

func (l line) String() string {
	return fmt.Sprintf("Line(m=%v,y-intercept=%v)", l.Slope, l.Intercept)
}

// Cannot use typical field oriented approaches here.  Must be totally deterministic impl
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

// A vector representation of a point in 2-dimensional space
type point struct {
	X *big.Int
	Y *big.Int
}

func (p point) Destroy() {
	xBytes := p.X.Bytes()
	yBytes := p.Y.Bytes()

	Bytes(xBytes).Destroy()
	Bytes(yBytes).Destroy()

	p.X.SetBytes(xBytes)
	p.Y.SetBytes(yBytes)
}

func (p point) Derive(o point) (line, error) {
	if p.X.Cmp(o.X) == 0 {
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
