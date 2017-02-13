package kayak

import (
	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
	"github.com/pkopriv2/bourne/stash"
)

type Options struct {
	net      net.Network
	storage  *bolt.DB
	logStore LogStore
}

func (o *Options) WithNetwork(n net.Network) *Options {
	o.net = n
	return o
}

func (o *Options) WithStorage(s *bolt.DB) *Options {
	o.storage = s
	return o
}

func (o *Options) WithLogStore(l LogStore) *Options {
	o.logStore = l
	return o
}

func defaultStoragePath(ctx common.Context) string {
	return ctx.Config().Optional(Config.StoragePath, Config.StoragePathDefault)
}

func defaultStorage(ctx common.Context, path string) (*bolt.DB, error) {
	return stash.Open(ctx, path)
}

func defaultLogStore(ctx common.Context, db *bolt.DB) (LogStore, error) {
	return NewBoltStore(db)
}

func defaultNetwork(ctx common.Context) net.Network {
	return net.NewTcpNetwork()
}

func buildOptions(ctx common.Context, fns []func(*Options)) (*Options, error) {
	opts := &Options{}

	for _, fn := range fns {
		fn(opts)
	}

	if opts.net == nil {
		opts.WithNetwork(net.NewTcpNetwork())
	}

	if opts.storage == nil {
		db, err := defaultStorage(ctx, defaultStoragePath(ctx))
		if err != nil {
			return nil, errors.Wrap(err, "Error opening default storage")
		}
		ctx.Control().Defer(func(error) {
			db.Close()
		})
		opts.WithStorage(db)
	}

	if opts.logStore == nil {
		store, err := defaultLogStore(ctx, opts.storage)
		if err != nil {
			return nil, errors.Wrap(err, "Error opening default log store")
		}
		opts.WithLogStore(store)
	}

	return opts, nil
}
