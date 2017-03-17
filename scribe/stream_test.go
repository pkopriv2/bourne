package scribe

// import (
	// "bufio"
	// "bytes"
	// "testing"
	// // "fmt"
//
	// "github.com/stretchr/testify/assert"
//
	// uuid "github.com/satori/go.uuid"
// )
//
// func TestPutUUID_simple(t *testing.T) {
	// val := uuid.NewV4()
//
	// in := new(bytes.Buffer)
	// wBuf := bufio.NewWriter(in)
	// writer := NewMessageWriter(wBuf)
	// writer.PutUUID(val)
	// writer.Flush()
//
	// out := bytes.NewBuffer(in.Bytes())
	// rBuf := bufio.NewReader(out)
	// reader := NewMessageReader(rBuf)
	// dec := reader.ReadUUID()
//
	// assert.Equal(t, val, dec)
// }
//
// // func TestPutUint64_zero(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint64(in, 0)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint64(out)
// //
	// // assert.Equal(t, uint64(0), dec)
// // }
// //
// // func TestPutUint64_one(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint64(in, 1)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint64(out)
// //
	// // assert.Equal(t, uint64(1), dec)
// // }
// //
// // func TestPutUint64_large(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint64(in, 103245678)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint64(out)
// //
	// // assert.Equal(t, uint64(103245678), dec)
// // }
// //
// // func TestPutUint32(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint32(in, 103001)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint32(out)
// //
	// // assert.Equal(t, uint32(103001), dec)
// // }
// //
// // func TestPutUint16(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint16(in, 1025)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint16(out)
// //
	// // assert.Equal(t, uint16(1025), dec)
// // }
// //
// // func TestPutUint8(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutUint8(in, 103)
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadUint8(out)
// //
	// // assert.Equal(t, uint8(103), dec)
// // }
// //
// // func TestPutBytes(t *testing.T) {
	// // in := new(bytes.Buffer)
// //
	// // PutBytes(in, []byte{1, 2, 3, 4})
// //
	// // out := bytes.NewBuffer(in.Bytes())
	// // dec, _ := ReadBytes(out)
// //
	// // assert.Equal(t, []byte{1, 2, 3, 4}, dec)
// // }
