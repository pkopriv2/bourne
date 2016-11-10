package convoy

import (
	"testing"
	"time"

	"github.com/pkopriv2/bourne/common"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func NewTestDirectory() *directory {
	return newDirectory(
		common.NewContext(
			common.NewConfig(map[string]interface{}{
				"convoy.directory.gc.data.expiration": 100 * time.Millisecond,
				"convoy.directory.gc.cycle.time":      10 * time.Millisecond,
			})))
}

func TestIncrementBytes_Zero(t *testing.T) {
	buf := []byte{0}
	inc := incBytes(buf)
	assert.Equal(t, []byte{1}, inc)
}

func TestIncrementBytes_One(t *testing.T) {
	buf := []byte{1}
	inc := incBytes(buf)
	assert.Equal(t, []byte{2}, inc)
}

func TestIncrementBytes_255(t *testing.T) {
	buf := []byte{255}
	inc := incBytes(buf)
	assert.Equal(t, []byte{1, 0}, inc)
}

func TestKiv_Compare_Equal(t *testing.T) {
	key1 := kiv{uuid.NewV4(), "key", "val"}
	key2 := kiv{key1.Id, "key", "val"}
	assert.True(t, key1.Compare(key2) == 0)
}

func TestKiv_Compare_DiffId(t *testing.T) {
	key1 := kiv{uuid.NewV4(), "key", "val"}
	key2 := kiv{Key: "key", Val: "val"} // zero
	assert.True(t, key1.Compare(key2) > 0)
}

func TestKiv_Compare_DiffKey(t *testing.T) {
	key1 := kiv{uuid.NewV4(), "key", "val"}
	key2 := kiv{key1.Id, "key2", "val"}
	assert.True(t, key1.Compare(key2) < 0)
}

// TODO: need to add some tests to show that mutual exclusivity holds..
func TestDirectory_Read_Write(t *testing.T) {
	dir := newDirectory(common.NewContext(common.NewEmptyConfig()))
	defer dir.Close()

	member := newMember(uuid.NewV4(), "host", 1)

	assert.Nil(t, dir.write(func(tx *tx) error {
		tx.Primary[member.id] = datum{Member: member}
		return nil
	}))

	assert.Nil(t, dir.read(func(tx *tx) error {
		assert.Equal(t, 1, len(tx.Primary))
		assert.Equal(t, member, tx.Primary[member.id].Member)
		return nil
	}))
}

func TestDirectory_Gc_DeletedDatum(t *testing.T) {
	dir := NewTestDirectory()
	defer dir.Close()

	id := uuid.NewV4()
	assert.Nil(t, dir.write(func(tx *tx) error {
		tx.Primary[id] = datum{Deleted: true, Time: time.Now()}
		return nil
	}))

	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, dir.read(func(tx *tx) error {
		assert.Equal(t, 0, len(tx.Primary))
		return nil
	}))
}

func TestDirectory_Gc_HangingRef(t *testing.T) {
	dir := NewTestDirectory()
	defer dir.Close()

	id := uuid.NewV4()
	assert.Nil(t, dir.write(func(tx *tx) error {
		tx.Kiv.Put(kiv{id, "key", "val"}, ref{})
		return nil
	}))

	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, dir.read(func(tx *tx) error {
		assert.Equal(t, 0, tx.Kiv.Size())
		return nil
	}))
}

func TestDirectory_Gc_DeletedDatumWithRefs(t *testing.T) {
	dir := NewTestDirectory()
	defer dir.Close()

	id := uuid.NewV4()
	assert.Nil(t, dir.write(func(tx *tx) error {
		tx.Primary[id] = datum{Deleted: true, Time: time.Now()}
		tx.Kiv.Put(kiv{id, "key", "val"}, ref{Id: id}) // not deleted
		return nil
	}))

	time.Sleep(200 * time.Millisecond)
	assert.Nil(t, dir.read(func(tx *tx) error {
		assert.Equal(t, 0, len(tx.Primary))
		return nil
	}))
}
