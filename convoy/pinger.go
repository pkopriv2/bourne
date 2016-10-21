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
	if err := p.pool.Submit(ret, Ping(m, timeout)); err != nil {
		return false, err
	}

	val := (<-ret).(struct{val bool; err error})
	return val.val, val.err
}

func (p *pinger) PingViaProxies(proxies []Member, id uuid.UUID, timeout time.Duration) (bool, error) {
	ret := make(chan interface{}, len(proxies))
	for _, m := range proxies {
		if err := p.pool.Submit(ret, PingViaProxy(m, id, timeout)); err != nil {
			return false, err
		}
	}

	val := (<-ret).(struct{val bool; err error})
	return val.val, val.err
}

func Ping(m Member, timeout time.Duration) func() interface{} {
	return func() interface{} {
		client, err := m.Client()
		if err != nil {
			return false
		}

		success, err := client.Ping(timeout)
		return struct {val bool; err error}{success, err}
	}
}

func PingViaProxy(proxy Member, target uuid.UUID, timeout time.Duration) func() interface{} {
	return func() interface{} {
		client, err := proxy.Client()
		if err != nil {
			return false
		}

		success, err := client.ProxyPing(target, timeout)
		return struct {val bool; err error}{success, err}
	}
}
