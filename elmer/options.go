package elmer

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type Options struct {
	Net           net.Network
	ConnTimeout   time.Duration
	ConnPool      int
	RosterTimeout time.Duration
}

func (o *Options) WithNetwork(n net.Network) *Options {
	o.Net = n
	return o
}

func (o *Options) WithConnTimeout(timeout time.Duration) *Options {
	o.ConnTimeout = timeout
	return o
}

func (o *Options) WithConnPool(size int) *Options {
	o.ConnPool = size
	return o
}

func (o *Options) WithRosterTimeout(timeout time.Duration) *Options {
	o.RosterTimeout = timeout
	return o
}

func buildOptions(ctx common.Context, fns []func(*Options)) (*Options, error) {
	opts := &Options{
		ConnPool:      20,
		ConnTimeout:   30 * time.Second,
		RosterTimeout: 30 * time.Second,
	}

	for _, fn := range fns {
		fn(opts)
	}

	if opts.Net == nil {
		opts.WithNetwork(net.NewTcpNetwork())
	}

	return opts, nil
}
