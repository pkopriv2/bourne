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
		Data:   amoeba.NewBTreeIndex(8),
		ChgLog: log,
	}

	return db, db.init()
}

func (d *database) Close() error {
	return nil
}

func (d *database) init() error {
	chgs, err := d.ChgLog.All()
	if err != nil {
		return err
	}

	d.Data.Update(func(u amoeba.Update) {
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
