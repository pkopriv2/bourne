package convoy

import "github.com/pkopriv2/bourne/enc"


type change struct {
	version int
	deleted bool
	key     string
	val     string
}

func newChange(version int, deleted bool, key string, val string) *change {
	return &change{version, deleted, key, val}
}

func readChange(r enc.Reader) Change {
	c := &change{}
	r.Read("version", &c.version)
	r.Read("deleted", &c.deleted)
	r.Read("key", &c.key)
	r.Read("val", &c.val)
	return c
}

func (c *change) Write(w enc.Writer) {
	w.Write("version", c.version)
	w.Write("deleted", c.deleted)
	w.Write("key", c.key)
	w.Write("val", c.val)
}

func (c *change) Version() int {
	return c.version
}

func (c *change) Deleted() bool {
	return c.deleted
}

func (c *change) Key() string {
	return c.key
}

func (c *change) Val() string {
	return c.val
}
