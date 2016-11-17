package stash

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/concurrent"
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

// Opens the stash instance for the given context.
// Only a single instance per db location is allowed.
func Open(ctx common.Context) (stash Stash, err error) {
	loc := ctx.Config().Optional(
		StashLocationKey, StashLocationDefault)

	env := ctx.Env()
	env.Data().Update(func(data concurrent.Map) {
		ctx.Logger().Debug("Opening stash instance [%v]", loc)

		// See if a stash has already been opened.
		val := data.Get(loc)
		if val != nil {
			stash = val.(Stash)
			return
		}

		// Go ahead and open stash instance.
		db, dbErr := getStore(loc)
		if dbErr != nil {
			err = errors.Wrap(dbErr, "Error opening store")
			return
		}

		// Store it on the context
		data.Put(loc, db)

		// Finally, add a close handler
		env.OnClose(func() {
			defer db.Close()
			defer env.Data().Remove(loc)
			ctx.Logger().Debug("Closing stash instance [%v]", loc)
		})

		stash = db
	})
	return
}

func getStore(loc string) (*bolt.DB, error) {
	return bolt.Open(loc, 0666, &bolt.Options{Timeout: 10 * time.Second})
}
