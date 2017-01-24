package kayak

import "github.com/pkg/errors"

func NewEventChannel(arr []Event) <-chan Event {
	ch := make(chan Event)
	go func() {
		defer close(ch)
		for _, i := range arr {
			ch <- i
		}
	}()
	return ch
}

func CollectEvents(ch <-chan Event, exp int) ([]Event, error) {
	ret := make([]Event, 0, exp)
	for i := 1; i <= exp; i++ {
		e, ok := <-ch
		if !ok {
			return nil, errors.Wrapf(EndOfStreamError, "Expected [%v] events.  Only received [%v]", exp, i)
		}

		ret = append(ret, e)
	}
	return ret, nil
}
