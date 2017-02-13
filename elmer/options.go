package elmer

import (
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/net"
)

type Options struct {
	net net.Network
}

func (o *Options) WithNetwork(n net.Network) *Options {
	o.net = n
	return o
}

func buildOptions(ctx common.Context, fns []func(*Options)) (*Options, error) {
	opts := &Options{}

	for _, fn := range fns {
		fn(opts)
	}

	if opts.net == nil {
		opts.WithNetwork(net.NewTcpNetwork())
	}

	return opts, nil
}
