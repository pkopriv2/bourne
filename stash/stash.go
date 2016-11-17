package stash

import (
	"path"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
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
// database.
type Stash interface {

	// Update the shared bolt instance.
	Update(func(*bolt.Tx) error) error

	// View the shared bolt instance.
	View(func(*bolt.Tx) error) error
}

// Opens a stash instance at random temporary location.
func OpenRandom(ctx common.Context) (Stash, error) {
	return Open(ctx, path.Join(afero.GetTempDir(afero.NewOsFs(), uuid.NewV4().String()), "stash.db"))
}

// Opens the stash instance at the default location and binds it to the context.
func OpenConfigured(ctx common.Context) (stash Stash, err error) {
	return Open(ctx, ctx.Config().Optional(StashLocationKey, StashLocationDefault))
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
		db, dbErr := getStore(path)
		if dbErr != nil {
			err = errors.Wrap(dbErr, "Error opening store")
			return
		}

		// Store it on the context
		data.Put(path, db)

		// Finally, add a close handler
		env.OnClose(func() {
			defer db.Close()
			defer env.Data().Remove(path)
			ctx.Logger().Debug("Closing stash instance [%v]", path)
		})

		stash = db
	})
	return
}

func getStore(loc string) (*bolt.DB, error) {
	return bolt.Open(loc, 0666, &bolt.Options{Timeout: 10 * time.Second})
}
