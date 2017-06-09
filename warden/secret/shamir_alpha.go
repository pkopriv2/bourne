package secret

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"math/big"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/warden/krypto"
)

// Register all the gob types.
func init() {
	gob.Register(&shamirShard{})
	gob.Register(&Point{})
}

// FIXME: This IS NOT shamir's algorithm - but should be enough to get going.
type shamirSecret struct {
	line line
	opts SecretOptions
}

func generateShamirSecret(rand io.Reader, opts SecretOptions) (Secret, error) {
	line, err := generateLine(rand, opts.Entropy)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &shamirSecret{line, opts}, nil
}

func (s *shamirSecret) Opts() SecretOptions {
	return s.opts
}

func (s *shamirSecret) Shard(rand io.Reader) (Shard, error) {
	pt, err := generatePoint(rand, s.line, s.opts.Entropy)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &shamirShard{pt, s.opts}, nil
}

func (s *shamirSecret) Hash(h krypto.Hash) ([]byte, error) {
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
	Pt      Point
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
		return nil, errors.Wrap(ErrIncompatibleShard, "Incompatible shards.")
	}

	if s.Opts() != sh.Opts() {
		return nil, errors.Wrap(ErrIncompatibleShard, "Incompatible shards.")
	}

	line, err := s.Pt.Derive(sh.Pt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &shamirSecret{line, s.Opts()}, nil
}

func (s *shamirShard) SigningFormat() ([]byte, error) {
	fmt, err := gobBytes(s)
	return fmt, errors.WithStack(err)
}

func (s *shamirShard) UnmarshalBinary(data []byte) error {
	_, err := parseGobBytes(data, &s)
	return errors.WithStack(err)
}

func (s *shamirShard) MarshalBinary() (data []byte, err error) {
	fmt, err := gobBytes(s)
	return fmt, errors.WithStack(err)
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

// Generates a random Point on the line.  The domain is used to bound the size of the resulting Point.
func generatePoint(rand io.Reader, line line, domain int) (Point, error) {
	x, err := generateBigInt(rand, domain)
	if err != nil {
		return Point{}, errors.Wrapf(err, "Unable to generate Point on line [%v] for domain [%v]", line, domain)
	}
	return line.Point(x), nil
}

// TODO: Is generating a random byte array consistent with generating a random integer?
//
// Generates a random integer using the size to determine the number of bytes to use when generating the
// random value.
func generateBigInt(rand io.Reader, size int) (*big.Int, error) {
	buf, err := krypto.GenNonce(rand, size)
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

	krypto.NewBytes(sBytes).Destroy()
	krypto.NewBytes(iBytes).Destroy()

	l.Slope.SetBytes(sBytes)
	l.Slope.SetBytes(iBytes)
}

func (l line) Height(x *big.Int) *big.Int {
	ret := big.NewInt(0)
	ret.Mul(x, l.Slope).Add(ret, l.Intercept)
	return ret
}

func (l line) Contains(o Point) bool {
	return l.Point(o.X).Equals(o)
}

func (l line) Point(x *big.Int) Point {
	return Point{x, l.Height(x)}
}

func (l line) Equals(o line) bool {
	return (l.Slope.Cmp(o.Slope) == 0) && (l.Intercept.Cmp(o.Intercept) == 0)
}

func (l line) String() string {
	return fmt.Sprintf("Line(m=%v,y-intercept=%v)", l.Slope, l.Intercept)
}

// consistent byte representation of line
func (l line) Hash(hash krypto.Hash) ([]byte, error) {
	raw, err := gobBytes(l)
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

// A vector representation of a Point in 2-dimensional space
type Point struct {

	X *big.Int
	Y *big.Int
}

func (p Point) Destroy() {
	xBytes := p.X.Bytes()
	yBytes := p.Y.Bytes()

	krypto.NewBytes(xBytes).Destroy()
	krypto.NewBytes(yBytes).Destroy()

	p.X.SetBytes(xBytes)
	p.Y.SetBytes(yBytes)
}

func (p Point) Derive(o Point) (line, error) {
	if p.X.Cmp(o.X) == 0 {
		return line{}, errors.Errorf("Cannot derive a line from the same Points [%v,%v]", o, p)
	}
	slope := deriveSlope(p, o)
	return line{slope, deriveIntercept(p, slope)}, nil
}

func (p Point) Equals(o Point) bool {
	return (p.X.Cmp(o.X) == 0) && (p.Y.Cmp(o.Y) == 0)
}

func (p Point) String() string {
	return fmt.Sprintf("Point(%v,%v)", p.X, p.Y)
}

func (p *Point) UnmarshalBinary(data []byte) error {
	panic("not implemented")
}


func (p *Point) MarshalBinary() (data []byte, err error) {
	panic("not implemented")
}


// consistent byte representation of line
func (p Point) Bytes() []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&p)
	return buf.Bytes()
}

func parsePointBytes(raw []byte) (p Point, e error) {
	e = gob.NewDecoder(bytes.NewBuffer(raw)).Decode(&p)
	return
}

func deriveSlope(p1, p2 Point) *big.Int {
	delX := big.NewInt(0)
	delX.Sub(p2.X, p1.X)

	delY := big.NewInt(0)
	delY.Sub(p2.Y, p1.Y)

	return delY.Div(delY, delX)
}

func deriveIntercept(p Point, slope *big.Int) *big.Int {
	delY := big.NewInt(0)
	delY.Mul(p.X, slope)

	ret := big.NewInt(0)
	return ret.Sub(p.Y, delY)
}
