package convoy

// func Generate(r Roster, done <-chan struct{}) <-chan Member {
// out:= make(chan Member)
// go func() {
// iter := r.Iterator()
// for {
// var next Member
// for {
// next = iter.Next()
// if next == nil {
// iter = r.Iterator()
// select {
// default:
// continue
// case <-done:
// return
// }
// }
// }
//
// select {
// case <-done:
// return
// case out <- next:
// }
// }
// }()
//
// return out
// }
//
//
// // Implements a round robin, random permutation over a roster.
// type generator interface {
// Members() <-chan Member // not thread safe!
// Close() error
// }
//
// type generatorImpl struct {
// roster  Roster
// members chan Member
// wait    sync.WaitGroup
// close   chan struct{}
// closed  chan struct{}
// }
//
// func NewGenerator(r Roster) generator {
// d := &generatorImpl{
// roster:  r,
// members: make(chan Member),
// close:   make(chan struct{})}
//
// d.wait.Add(1)
// go generate(d)
// return d
// }
//
// func (d *generatorImpl) Members() <-chan Member {
// return d.members
// }
//
// func (d *generatorImpl) Close() error {
// select {
// case <-d.closed:
// return fmt.Errorf("Already closed")
// case d.close <- struct{}{}:
// d.wait.Wait()
// close(d.members)
// }
// return nil
// }
//
// func generate(d *generatorImpl) {
// defer d.wait.Done()
//
// iter := d.roster.Iterator()
// for {
// select {
// case <-d.close:
// return
// }
//
// var next Member
// for {
// next = iter.Next()
// if next == nil {
// iter = d.roster.Iterator()
// continue
// }
// }
//
// select {
// case d.members <- next:
// case <-d.close:
// return
// }
// }
// }
