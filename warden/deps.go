package warden

import (
	"crypto/rand"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/pkopriv2/bourne/common"
	"github.com/pkopriv2/bourne/micro"
	"github.com/pkopriv2/bourne/net"
)

// Core dependencies
type Dependencies struct {
	Net  Transport
	Rand io.Reader
}

func buildDependencies(ctx common.Context, addr string, timeout time.Duration, fns ...func(*Dependencies)) (Dependencies, error) {
	ret := Dependencies{}
	for _, fn := range fns {
		fn(&ret)
	}

	if ret.Rand == nil {
		ret.Rand = rand.Reader
	}

	if ret.Net == nil {
		conn, err := net.NewTcpNetwork().Dial(timeout, addr)
		if err != nil {
			return Dependencies{}, errors.WithStack(err)
		}

		cl, err := micro.NewClient(ctx, conn, micro.Gob)
		if err != nil {
			return Dependencies{}, errors.WithStack(err)
		}
		ret.Net = newClient(cl)
	}

	return ret, nil
}
