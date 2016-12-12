package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
)

// The database is the local version of a publiseh
type Database interface {
	// Store

	// Returns the log of the database.  These are required to be durable
	// in the event of failures.  In order to rejoin a cluster,  a failed
	// replica is required to retransmit any incomplete state along with
	// its versioning information.
	//
	// See the changelog documentation for more info on what state is required
	// to qualify as a database.
	Log() ChangeLog
}

type database struct {
	ctx    common.Context
	data   amoeba.Index
	chgLog ChangeLog
}

// Opens the database using the path to the given db file.
func OpenDatabase(ctx common.Context, path string) (Database, error) {
	stash, err := stash.Open(ctx, path)
	if err != nil {
		return nil, err
	}

	return initDatabase(ctx, openChangeLog(stash))
}

// Opens the database using the given changelog
func initDatabase(ctx common.Context, log ChangeLog) (Database, error) {
	db := &database{
		ctx:    ctx,
		data:   amoeba.NewBTreeIndex(8),
		chgLog: log,
	}

	return db, db.init()
}

func (d *database) Close() error {
	return nil
}

func (d *database) init() error {
	chgs, err := d.chgLog.All()
	if err != nil {
		return err
	}

	d.data.Update(func(u amoeba.Update) {
		for _, chg := range chgs {
			if chg.Del {
				u.Del(amoeba.StringKey(chg.Key))
			} else {
				u.Put(amoeba.StringKey(chg.Key), chg.Val)
			}
		}
	})
	return nil
}

func (d *database) Get(key string) (ret string, ok bool, err error) {
	d.data.Read(func(v amoeba.View) {
		ret, ok = dbGetVal(v, key)
	})
	return
}

func (d *database) Put(key string, val string) (err error) {
	d.data.Update(func(u amoeba.Update) {
		err = dbPutVal(d.chgLog, u, key, val)
	})
	return
}

func (d *database) Del(key string) (err error) {
	d.data.Update(func(u amoeba.Update) {
		err = dbDelVal(d.chgLog, u, key)
	})
	return
}

func (d *database) Log() ChangeLog {
	return d.chgLog
}

// Helper functions.
func dbGetVal(data amoeba.View, key string) (str string, ok bool) {
	if raw := data.Get(amoeba.StringKey(key)); raw != nil {
		return raw.(string), true
	}
	return
}

func dbDelVal(log ChangeLog, data amoeba.Update, key string) error {
	_, err := log.Append(key, "", true)
	if err != nil {
		return err
	}

	data.Del(amoeba.StringKey(key))
	return nil
}

func dbPutVal(log ChangeLog, data amoeba.Update, key string, val string) error {
	_, err := log.Append(key, val, false)
	if err != nil {
		return err
	}

	data.Put(amoeba.StringKey(key), val)
	return nil
}
