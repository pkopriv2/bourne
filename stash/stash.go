package stash

import (
	"io"
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/afero"
)

const (
	StashLocationKey     = "bourne.stash.path"
	StashLocationDefault = "/var/bourne/stash.data"
)

// A stash is nothing but a shared instance of a bolt
// database.  They can be
type Stash interface {
	io.Closer

	// Returns the os filesystem path to the stash
	Path() string

	// Update the shared bolt instance.
	Update(func(*bolt.Tx) error) error

	// View the shared bolt instance.
	View(func(*bolt.Tx) error) error
}

// Opens a stash instance at random temporary location.
func OpenRandom(ctx common.Context) (Stash, error) {
	return Open(ctx, path.Join(afero.GetTempDir(afero.NewOsFs(), path.Join("bourne", uuid.NewV1().String())), "stash.db"))
}

// Opens the stash instance at the default location and binds it to the context.
func OpenConfigured(ctx common.Context) (stash Stash, err error) {
	return Open(ctx, ctx.Config().Optional(StashLocationKey, StashLocationDefault))
}

// Opens a transient stash instance that will be deleted on ctx#close().
func OpenTransient(ctx common.Context) (Stash, error) {
	stash, err := OpenRandom(ctx)
	if err != nil {
		return nil, err
	}

	deleteOnClose(ctx, stash)
	return stash, nil
}

// Opens the stash instance at the given location and binds it to the context.
func Open(ctx common.Context, path string) (stash Stash, err error) {
	env := ctx.Env()
	env.Data().Update(func(data concurrent.Map) {
		ctx.Logger().Debug("Opening stash instance [%v]", path)

		// See if a stash has already been opened.
		val := data.Get(path)
		if val != nil {
			stash = val.(Stash)
			return
		}

		// Go ahead and open stash instance.
		stash, err = getStore(path)
		if err != nil {
			return
		}

		// Store it on the context
		data.Put(path, stash)

		// bind the stash's cleanup to the context
		removeOnClose(ctx, stash)
		closeOnClose(ctx, stash)
	})
	return
}

func closeOnClose(ctx common.Context, stash Stash) {
	ctx.Control().Defer(func(error) {
		ctx.Logger().Debug("Closing stash [%v]", stash.Path())
		stash.Close()
	})
}

func removeOnClose(ctx common.Context, stash Stash) {
	path := stash.Path()
	ctx.Control().Defer(func(error) {
		ctx.Logger().Debug("Removing context entry [%v]", path)
		ctx.Env().Data().Remove(path)
	})
}

func deleteOnClose(ctx common.Context, stash Stash) {
	path := stash.Path()
	ctx.Control().Defer(func(error) {
		ctx.Logger().Debug("Deleting stash instance [%v]", path)
		afero.NewOsFs().RemoveAll(path)
	})
}

func getStore(loc string) (*bolt.DB, error) {
	return bolt.Open(loc, 0666, &bolt.Options{Timeout: 10 * time.Second})
}
