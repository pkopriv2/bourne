package convoy

import (
	"github.com/pkopriv2/bourne/amoeba"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/stash"
)

type database struct {
	ctx    common.Context
	data   amoeba.Index
	chgLog ChangeLog
}

// Opens the localStore using the path to the given ls file.
func openDatabase(ctx common.Context, path string) (*database, error) {
	stash, err := stash.Open(ctx, path)
	if err != nil {
		return nil, err
	}

	return initDatabase(ctx, openChangeLog(stash))
}

// Opens the localStore using the given changelog
func initDatabase(ctx common.Context, log ChangeLog) (*database, error) {
	ls := &database{
		ctx:    ctx,
		data:   amoeba.NewBTreeIndex(8),
		chgLog: log,
	}

	return ls, ls.init()
}

func (d *database) Close() error {
	d.Log().Close()
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
				u.Put(amoeba.StringKey(chg.Key), Item{chg.Val, chg.Ver, true})
			} else {
				u.Put(amoeba.StringKey(chg.Key), Item{chg.Val, chg.Ver, false})
			}
		}
	})
	return nil
}

func (l *database) Get(key string) (item Item, ok bool) {
	l.data.Read(func(v amoeba.View) {
		item, ok = lsGetItem(v, key)
	})
	return
}

func (l *database) Put(key string, val string, expected int) (ok bool, new Item, err error) {
	l.data.Update(func(u amoeba.Update) {
		ok, new, err = lsPutItem(l.chgLog, u, key, val, expected)
	})
	return
}

func (l *database) Del(key string, expected int) (ok bool, new Item, err error) {
	l.data.Update(func(u amoeba.Update) {
		ok, new, err = lsDelItem(l.chgLog, u, key, expected)
	})
	return
}

func (d *database) Log() ChangeLog {
	return d.chgLog
}

// Helper functions.

func lsGetItem(data amoeba.View, key string) (item Item, ok bool) {
	if raw := data.Get(amoeba.StringKey(key)); raw != nil {
		return raw.(Item), true
	}
	return
}

func lsDelItem(log ChangeLog, data amoeba.Update, key string, ver int) (bool, Item, error) {
	exp, _ := lsGetItem(data, key)
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

func lsPutItem(log ChangeLog, data amoeba.Update, key string, val string, expVer int) (bool, Item, error) {
	cur, _ := lsGetItem(data, key)
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
