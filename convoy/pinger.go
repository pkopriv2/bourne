package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

type Pinger interface {
	Ping(Member, time.Duration) (bool, error)
	PingViaProxies([]Member, uuid.UUID, time.Duration) (bool, error)
}

type pinger struct {
	pool concurrent.WorkPool
}

func newPinger(pool concurrent.WorkPool) Pinger {
	return &pinger{pool}
}

func (p *pinger) Ping(m Member, timeout time.Duration) (bool, error) {
	ret := make(chan interface{})
	if err := p.pool.Submit(Ping(m, timeout), ret); err != nil {
		return false, err
	}

	return (<-ret).(bool), nil
}

func (p *pinger) PingViaProxies(proxies []Member, id uuid.UUID, timeout time.Duration) (bool, error) {
	ret := make(chan interface{}, len(proxies))

	for _, m := range proxies {
		if err := p.pool.Submit(PingViaProxy(m, id, timeout), ret); err != nil {
			return false, err
		}
	}

	return (<-ret).(bool), nil
}

func Ping(m Member, timeout time.Duration) func() interface{} {
	return func() interface{} {
		return m.service().Ping(timeout)
	}
}

func PingViaProxy(proxy Member, target uuid.UUID, timeout time.Duration) func() interface{} {
	return func() interface{} {
		return proxy.service().ProxyPing(target, timeout)
	}
}
