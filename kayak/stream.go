package kayak

type ArrayEventStream struct {
	events []Event
	cur int
}

func NewArrayEventStream(events []Event) *ArrayEventStream {
	return &ArrayEventStream{events: events}
}

func (a *ArrayEventStream) Close() error {
	return nil
}

func (a *ArrayEventStream) Next() (Event, error) {
	if a.cur > len(a.events)-1 {
		return nil, nil
	}

	defer func() {a.cur++}()
	return a.events[a.cur], nil
}

func NewEventChannel(arr []Event) <-chan Event {
	ch := make(chan Event)
	go func() {
		defer close(ch)
		for _, i := range arr {
			ch<-i
		}
	}()
	return ch
}
