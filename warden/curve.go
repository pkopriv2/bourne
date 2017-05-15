package warden

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"math/big"

	"github.com/pkg/errors"
)

// FIXME: This IS NOT shamir's algorithm - but should be enough to get going.
type shamirSecret struct {
	line line
	opts SecretOptions
}

func generateShamirSecret(rand io.Reader, opts SecretOptions) (Secret, error) {
	line, err := generateLine(rand, opts.ShardStrength)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &shamirSecret{line, opts}, nil
}

func (s *shamirSecret) Opts() SecretOptions {
	return s.opts
}

func (s *shamirSecret) Shard(rand io.Reader) (Shard, error) {
	pt, err := generatePoint(rand, s.line, s.opts.ShardStrength)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &shamirShard{pt, s.opts}, nil
}

func (s *shamirSecret) Hash(h Hash) ([]byte, error) {
	ret, err := s.line.Hash(h)
	return ret, errors.WithStack(err)
}

func (s *shamirSecret) Destroy() {
	s.line.Destroy()
}

func (s *shamirSecret) String() string {
	return fmt.Sprintf("Line: %v", s.line)
}

type shamirShard struct {
	Pt      point
	RawOpts SecretOptions
}

func parseShamirShard(raw []byte) (s *shamirShard, e error) {
	_, e = parseGobBytes(raw, &s)
	return
}

func (s *shamirShard) Opts() SecretOptions {
	return s.RawOpts
}

func (s *shamirShard) Derive(raw Shard) (Secret, error) {
	sh, ok := raw.(*shamirShard)
	if !ok {
		return nil, errors.Wrap(TrustError, "Incompatible shards.")
	}

	if s.Opts() != sh.Opts() {
		return nil, errors.Wrap(TrustError, "Incompatible shards.")
	}

	line, err := s.Pt.Derive(sh.Pt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &shamirSecret{line, s.Opts()}, nil
}

func (s *shamirShard) Format() ([]byte, error) {
	return gobBytes(s)
}

func (s *shamirShard) Destroy() {
	s.Pt.Destroy()
}

// Generates a random line.  The domain is used to determine the number of bytes to use when generating
// the properties of the curve.
func generateLine(rand io.Reader, domain int) (line, error) {
	slope, err := generateBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	intercept, err := generateBigInt(rand, domain)
	if err != nil {
		return line{}, errors.WithStack(err)
	}

	return line{slope, intercept}, nil
}

// Generates a random point on the line.  The domain is used to bound the size of the resulting point.
func generatePoint(rand io.Reader, line line, domain int) (point, error) {
	x, err := generateBigInt(rand, domain)
	if err != nil {
		return point{}, errors.Wrapf(err, "Unable to generate point on line [%v] for domain [%v]", line, domain)
	}
	return line.Point(x), nil
}

// TODO: Is generating a random byte array consistent with generating a random integer?
//
// Generates a random integer using the size to determine the number of bytes to use when generating the
// random value.
func generateBigInt(rand io.Reader, size int) (*big.Int, error) {
	buf, err := genRandomBytes(rand, size)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to generate random *big.Int of size [%v]", size)
	}

	return new(big.Int).SetBytes(buf), nil
}

// A simple y=mx+b line.  Considered a parametric form, but when moving to a
// canonical form, it reduces to this anyway.
type line struct {
	Slope     *big.Int
	Intercept *big.Int
}

func (l line) Destroy() {
	sBytes := l.Slope.Bytes()
	iBytes := l.Intercept.Bytes()

	cryptoBytes(sBytes).Destroy()
	cryptoBytes(iBytes).Destroy()

	l.Slope.SetBytes(sBytes)
	l.Slope.SetBytes(iBytes)
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Contains(o point) bool {
	return l.Point(o.X).Equals(o)
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


// consistent byte representation of line
func (l line) Hash(hash Hash) ([]byte, error) {
	raw, err :=  gobBytes(l)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ret, err := hash.Hash(raw)
	return ret, errors.WithStack(err)
}

func parseLineBytes(raw []byte) (l line, e error) {
	e = gob.NewDecoder(bytes.NewBuffer(raw)).Decode(&l)
	return
}

// A vector representation of a point in 2-dimensional space
type point struct {
	X *big.Int
	Y *big.Int
}

func (p point) Destroy() {
	xBytes := p.X.Bytes()
	yBytes := p.Y.Bytes()

	cryptoBytes(xBytes).Destroy()
	cryptoBytes(yBytes).Destroy()

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

// consistent byte representation of line
func (p point) Bytes() []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&p)
	return buf.Bytes()
}

func parsePointBytes(raw []byte) (p point, e error) {
	e = gob.NewDecoder(bytes.NewBuffer(raw)).Decode(&p)
	return
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
