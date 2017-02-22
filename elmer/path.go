package elmer

import (
	"bytes"
	"fmt"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/scribe"
)

var emptyPath = path([]segment{})

type partialPath struct {
	Parent path
	Child  []byte
}

func (p partialPath) Path(ver int) path {
	return p.Parent.Sub(p.Child, ver)
}

func (p partialPath) Write(w scribe.Writer) {
	w.WriteMessage("parent", p.Parent)
	w.WriteBytes("child", p.Child)
}

func readPartialPath(r scribe.Reader) (p partialPath, e error) {
	e = r.ParseMessage("parent", &p.Parent, pathParser)
	e = common.Or(e, r.ReadBytes("child", &p.Child))
	return
}

type path []segment

func (p path) String() string {
	if len(p) == 0 {
		return ""
	}

	return fmt.Sprintf("%v:%v/%v", p[0].Elem, p[0].Ver, path(p[1:]))
}

func (p path) Equals(o path) bool {
	if len(p) != len(o) {
		return false
	}

	for i := len(p) - 1; i > 0; i-- {
		if ! p[i].Equals(o[i]) {
			return false
		}
	}
	return true
}

func (p path) Parent() path {
	return p[:len(p)-1]
}

func (p path) Tail() ([]byte, int) {
	last := p.Last()
	return last.Elem, last.Ver
}

func (p path) Last() segment {
	if len(p) == 0 {
		return segment{}
	}

	return p[len(p)-1]
}

func (p path) Sub(elem []byte, ver int) path {
	return append(p, segment{elem, ver})
}

func (p path) Partial(elem []byte) partialPath {
	return partialPath{p, elem}
}

func (p path) Write(w scribe.Writer) {
	w.WriteMessages("raw", []segment(p))
}

func readPath(r scribe.Reader) (p path, e error) {
	e = r.ParseMessages("raw", (*[]segment)(&p), segmentParser)
	return
}

func pathParser(r scribe.Reader) (interface{}, error) {
	return readPath(r)
}

type segment struct {
	Elem []byte
	Ver  int
}

func (s segment) Equals(o segment) bool {
	return s.Ver == o.Ver && bytes.Equal(s.Elem, o.Elem)
}

func (s segment) Write(w scribe.Writer) {
	w.WriteBytes("elem", s.Elem)
	w.WriteInt("ver", s.Ver)
}

func readSegment(r scribe.Reader) (s segment, e error) {
	e = r.ReadBytes("elem", &s.Elem)
	e = common.Or(e, r.ReadInt("ver", &s.Ver))
	return
}

func segmentParser(r scribe.Reader) (interface{}, error) {
	return readSegment(r)
}
