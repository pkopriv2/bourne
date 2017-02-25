package elmer

import (
	"time"

	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)


type Options struct {
	net              net.Network
	rpcTimeout       time.Duration
	rpcClientPool    int
	rpcServerPool    int
	rpcRosterRefresh time.Duration
}

func initOptions(ctx common.Context, fns []func(*Options)) (*Options, error) {
	opts := &Options{
		rpcClientPool:    5,
		rpcTimeout:       30 * time.Second,
		rpcRosterRefresh: 30 * time.Second,
		rpcServerPool:    100,
	}

	for _, fn := range fns {
		fn(opts)
	}

	if opts.net == nil {
		opts.WithNetwork(net.NewTcpNetwork())
	}

	return opts, nil
}

func (o *Options) WithNetwork(n net.Network) *Options {
	o.net = n
	return o
}

func (o *Options) WithRpcTimeout(t time.Duration) *Options {
	o.rpcTimeout = t
	return o
}

func (o *Options) WithRpcClientPool(n int) *Options {
	o.rpcClientPool = n
	return o
}

func (o *Options) WithRpcServerPool(n int) *Options {
	o.rpcClientPool = n
	return o
}

func (o *Options) WithRpcRosterRefresh(timeout time.Duration) *Options {
	o.rpcRosterRefresh = timeout
	return o
}
