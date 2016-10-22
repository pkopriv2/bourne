package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

type pinger interface {
	Ping(Member, time.Duration) (bool, error)
	ProxyPing([]Member, uuid.UUID, time.Duration) (bool, error)
}

type pingerImpl struct {
	pool concurrent.WorkPool
}

func newPinger(pool concurrent.WorkPool) pinger {
	return &pingerImpl{pool}
}

func (p *pingerImpl) Ping(m Member, t time.Duration) (bool, error) {
	client, err := m.client()
	if err != nil {
		return false, err
	}

	defer client.Close()
	return client.Ping(t)
}

func (p *pingerImpl) ProxyPing(proxies []Member, memberId uuid.UUID, timeout time.Duration) (bool, error) {
	val := make(chan interface{}, len(proxies))
	for _, m := range proxies {
		if err := p.pool.Submit(proxyPing(val, m, memberId, timeout)); err != nil {
			return false, err
		}
	}

	var cur interface{}
	for {
		cur := <-val
		if _, ok := cur.(bool); ok {
			return true, nil
		}
	}

	return false, cur.(error)
}

func proxyPing(resp chan<-interface{}, proxy Member, memberId uuid.UUID, timeout time.Duration) func() {
	return func() {
		client, err := proxy.client()
		if err != nil {
			resp <- err
			return
		}

		defer client.Close()
		success, err := client.PingProxy(memberId, timeout)
		if err != nil {
			resp <- err
			return
		}

		resp <- success
	}
}
