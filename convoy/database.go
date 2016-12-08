package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
)

type database struct {
	Ctx    common.Context
	Data   amoeba.Index
	ChgLog ChangeLog
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
		Ctx:    ctx,
		Data:   amoeba.NewIndexer(ctx),
		ChgLog: log,
	}

	return db, db.init()
}

func (d *database) Close() error {
	return d.Data.Close()
}

func (d *database) init() error {
	chgs, err := d.ChgLog.All()
	if err != nil {
		return err
	}

	d.Data.Update(func(u amoeba.Update) {
		for _, chg := range chgs {
			if chg.Del {
				u.Del(amoeba.StringKey(chg.Key), chg.Ver)
			} else {
				u.Put(amoeba.StringKey(chg.Key), chg.Val, chg.Ver)
			}
		}
	})
	return nil
}

func (d *database) Get(key string) (ret string, ok bool, err error) {
	d.Data.Read(func(v amoeba.View) {
		ret, ok = dbGetVal(v, key)
	})
	return
}

func (d *database) Put(key string, val string) (err error) {
	d.Data.Update(func(u amoeba.Update) {
		err = dbPutVal(d.ChgLog, u, key, val)
	})
	return
}

func (d *database) Del(key string) (err error) {
	d.Data.Update(func(u amoeba.Update) {
		err = dbDelVal(d.ChgLog, u, key)
	})
	return
}

func (d *database) Log() ChangeLog {
	return d.ChgLog
}

// Helper functions.
func dbUnpackAmoebaItem(item amoeba.Item) (val string, ver int, ok bool) {
	if item == nil {
		return
	}

	raw := item.Val()
	if raw == nil {
		return
	}

	return raw.(string), item.Ver(), true
}

func dbGetVal(data amoeba.View, key string) (str string, ok bool) {
	val, _, ok := dbUnpackAmoebaItem(data.Get(amoeba.StringKey(key)))
	return val, ok
}

func dbDelVal(log ChangeLog, data amoeba.Update, key string) error {
	chg, err := log.Append(key, "", true)
	if err != nil {
		return err
	}

	data.Del(amoeba.StringKey(key), chg.Ver)
	return nil
}

func dbPutVal(log ChangeLog, data amoeba.Update, key string, val string) error {
	chg, err := log.Append(key, val, false)
	if err != nil {
		return err
	}

	data.Put(amoeba.StringKey(key), val, chg.Ver)
	return nil
}
