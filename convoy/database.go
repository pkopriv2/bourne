package convoy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
)

var (
	databaseClosedError = errors.New("DB:CLOSED")
)

type database struct {
	ctx    common.Context
	data   amoeba.Index
	chgLog *changeLog
	lock   sync.RWMutex
	closed bool
}

// Opens the database using the path to the given db file.
func openDatabase(ctx common.Context, path string) (*database, error) {
	stash, err := stash.Open(ctx, path)
	if err != nil {
		return nil, err
	}

	return initDatabase(ctx, openChangeLog(stash))
}

// Opens the database using the given changelog
func initDatabase(ctx common.Context, log *changeLog) (*database, error) {
	db := &database{
		ctx:    ctx,
		data:   amoeba.NewBTreeIndex(8),
		chgLog: log,
	}

	return db, db.init()
}

func (d *database) init() error {
	chgs, err := d.chgLog.All()
	if err != nil {
		return err
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
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.closed {
		return databaseClosedError
	}

	d.Log().Close()
	return nil
}

func (d *database) Get(key string) (item *Item, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.closed {
		return nil, databaseClosedError
	}

	d.data.Read(func(v amoeba.View) {
		i, found := dbGetItem(v, key)
		if found {
			item = &i
		}
	})
	return
}

func (d *database) Put(key string, val string, expected int) (ok bool, new Item, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.closed {
		return false, Item{}, databaseClosedError
	}

	d.data.Update(func(u amoeba.Update) {
		ok, new, err = dbPutItem(d.chgLog, u, key, val, expected)
	})
	return
}

func (d *database) Del(key string, expected int) (ok bool, new Item, err error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if d.closed {
		return false, Item{}, databaseClosedError
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
