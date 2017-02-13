package convoy

import (
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
)

type database struct {
	ctx    common.Context
	ctrl   common.Control
	data   amoeba.Index
	chgLog *changeLog
}

// Opens the database using the given changelog
func openDatabase(ctx common.Context, log *changeLog) (*database, error) {
	ctx = ctx.Sub("Db")
	db := &database{
		ctx:    ctx,
		ctrl:   ctx.Control(),
		data:   amoeba.NewBTreeIndex(8),
		chgLog: log,
	}

	return db, db.init()
}

func (d *database) init() error {
	chgs, err := d.Log().All()
	if err != nil {
		return errors.WithStack(err)
	}

	d.data.Update(func(u amoeba.Update) {
		for _, chg := range chgs {
			if chg.Del {
				u.Put(amoeba.StringKey(chg.Key), Item{chg.Val, chg.Ver, true})
			} else {
				u.Put(amoeba.StringKey(chg.Key), Item{chg.Val, chg.Ver, false})
			}
		}
	})
	return nil
}

func (d *database) Close() error {
	return d.ctrl.Close()
}

func (d *database) Get(key string) (found bool, item Item, err error) {
	if d.ctrl.IsClosed() {
		return false, Item{}, errors.WithStack(ClosedError)
	}

	d.data.Read(func(v amoeba.View) {
		item, found = dbGetItem(v, key)
	})
	return
}

func (d *database) Put(key string, val string, expected int) (ok bool, new Item, err error) {
	if d.ctrl.IsClosed() {
		return false, Item{}, errors.WithStack(ClosedError)
	}

	d.data.Update(func(u amoeba.Update) {
		ok, new, err = dbPutItem(d.chgLog, u, key, val, expected)
	})
	return
}

func (d *database) Del(key string, expected int) (ok bool, new Item, err error) {
	if d.ctrl.IsClosed() {
		return false, Item{}, errors.WithStack(ClosedError)
	}

	d.data.Update(func(u amoeba.Update) {
		ok, new, err = dbDelItem(d.chgLog, u, key, expected)
	})
	return
}

func (d *database) Log() *changeLog {
	return d.chgLog
}

// Helper functions.

func dbGetItem(data amoeba.View, key string) (item Item, ok bool) {
	if raw := data.Get(amoeba.StringKey(key)); raw != nil {
		return raw.(Item), true
	}
	return
}

func dbDelItem(log *changeLog, data amoeba.Update, key string, ver int) (bool, Item, error) {
	exp, _ := dbGetItem(data, key)
	if exp.Ver != ver {
		return false, Item{}, nil
	}

	chg, err := log.Append(key, "", true)
	if err != nil {
		return false, Item{}, err
	}

	new := Item{"", chg.Ver, true}
	data.Put(amoeba.StringKey(key), new)
	return true, new, nil
}

func dbPutItem(log *changeLog, data amoeba.Update, key string, val string, expVer int) (bool, Item, error) {
	cur, _ := dbGetItem(data, key)
	if expVer != cur.Ver {
		return false, Item{}, nil
	}

	chg, err := log.Append(key, val, false)
	if err != nil {
		return false, Item{}, err
	}

	new := Item{val, chg.Ver, false}
	data.Put(amoeba.StringKey(key), new)
	return true, new, nil
}
