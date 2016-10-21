package convoy

import (
	"time"

	"github.com/pkopriv2/bourne/concurrent"
	uuid "github.com/satori/go.uuid"
)

type Pinger interface {
	Ping(Member, time.Duration) (bool, error)
	ProxyPing([]Member, uuid.UUID, time.Duration) (bool, error)
}

type pinger struct {
	pool concurrent.WorkPool
}

func newPinger(pool concurrent.WorkPool) Pinger {
	return &pinger{pool}
}

func (p *pinger) Ping(m Member, t time.Duration) (bool, error) {
	val := make(chan interface{})
	if err := p.pool.Submit(val, ping(m, t)); err != nil {
		return false, err
	}

	switch t := (<-val).(type) {
	default:
		panic("Unknown type")
	case bool:
		return t, nil
	case error:
		return false, t
	}
}

func (p *pinger) ProxyPing(proxies []Member, memberId uuid.UUID, timeout time.Duration) (bool, error) {
	val := make(chan interface{}, len(proxies))
	for _, m := range proxies {
		if err := p.pool.Submit(val, proxyPing(m, memberId, timeout)); err != nil {
			return false, err
		}
	}

	switch t := (<-val).(type) {
	default:
		panic("Unknown type")
	case bool:
		return t, nil
	case error:
		return false, t
	}
}


func ping(m Member, timeout time.Duration) concurrent.Work {
	return func(resp concurrent.Response) {
		client, err := m.Client()
		if err != nil {
			resp<-err
			return
		}

		success, err := client.Ping(timeout)
		if err != nil {
			resp<-err
			return
		}

		resp<-success
	}
}


func proxyPing(proxy Member, memberId uuid.UUID, timeout time.Duration) concurrent.Work {
	return func(resp concurrent.Response) {
		client, err := proxy.Client()
		if err != nil {
			resp<-err
			return
		}

		success, err := client.ProxyPing(memberId, timeout)
		if err != nil {
			resp<-err
			return
		}

		resp<-success
	}
}
