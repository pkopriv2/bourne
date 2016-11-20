package convoy

import (
	"sync"
	"time"

	"github.com/pkopriv2/bourne/concurrent"
)

// The indexer simply reads from a changelog and writes to the directory.
func indexEvents(ch <-chan event, dir *directory) {
	go func() {
		for e := range ch {
			done, timeout := concurrent.NewBreaker(365*24*time.Hour, func() interface{} {
				dir.Apply(e)
				return nil
			})

			select {
			case <-done:
				continue
			case <-timeout:
				return
			}
		}
	}()
}

func mergeEventChannels(chs ...<-chan event) <-chan event {
	ret := make(chan event)

	var wait sync.WaitGroup
	wait.Add(len(chs))
	for _, c := range chs {
		go func(c <-chan event) {
			for e := range c {
				ret <- e
			}
			wait.Done()
		}(c)
	}

	go func() {
		wait.Wait()
		close(ret)
	}()

	return ret
}
