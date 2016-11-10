package concurrent

func NewFuture(fn func() interface{}) <-chan interface{} {
	done := make(chan interface{}, 1)

	go func() {
		done<-fn()
	}()

	return done
}
