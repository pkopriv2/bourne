package kayak

import (
	"github.com/boltdb/bolt"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
)

type Dependencies struct {
	Network  net.Network
	LogStore LogStore
	Storage  *bolt.DB
}

func NewDefaultDependencies(ctx common.Context) (Dependencies, error) {
	db, err := stash.Open(ctx, ctx.Config().Optional(Config.StoragePath, Config.StoragePathDefault))
	if err != nil {
		return Dependencies{}, err
	}

	store, err := NewBoltStore(db)
	if err != nil {
		return Dependencies{}, err
	}

	return Dependencies{
		Network:  net.NewTcpNetwork(),
		LogStore: store,
		Storage:  db,
	}, nil
}

func NewTransientDependencies(ctx common.Context) (Dependencies, error) {
	db, err := stash.OpenTransient(ctx)
	if err != nil {
		return Dependencies{}, err
	}

	store, err := NewBoltStore(db)
	if err != nil {
		return Dependencies{}, err
	}

	return Dependencies{
		Network:  net.NewTcpNetwork(),
		LogStore: store,
		Storage:  db,
	}, nil
}
