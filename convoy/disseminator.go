package convoy

import "github.com/pkopriv2/bourne/concurrent"

type sendResponse struct {
	Accepted bool
	Error    error
}

// Sends an update to a randomly chosen recipient.
type Disseminator interface {
	Send(Update) (bool, error)
}

type disseminator struct {
	generator Generator
	pool      concurrent.WorkPool
}

// func (d *disseminator) Send(u Update, timeout time.Duration) (bool, error) {
// ret := make(chan interface{})
// if err := d.pool.Submit(Ping(m, timeout), ret); err != nil {
// return false, err
// }
//
// return (<-ret).(bool), nil
// m := <-d.generator.Members()
// return m.service().Send(u)
// }
//
// func SendUpdate(m Member, timeout time.Duration) func() interface{} {
// return func() interface{}
//
// }
