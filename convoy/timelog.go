package convoy

// const (
// ConfTimeLogHorizon = "convoy.timelog.horizon"
// )
//
// const (
// DefaultTimeLogHorizon = 30 * time.Minute
// )
//
// // Adds a listener to the change log and returns a buffered channel of changes.
// // the channel is closed when the log is closed.
// func timeLogListen(tl *timeLog) <-chan []event {
// ret := make(chan []event, 1024)
// tl.Listen(func(batch []event) {
// if batch == nil {
// close(ret)
// return
// }
//
// ret <- batch
// })
// return ret
// }
//
// // Maintains a time sorted list of events up to some specified maximum ttl.
// type timeLogKey struct {
// Id      uuid.UUID
// Created time.Time // assumed to be unique (time must be taken inside lock)
// }
//
// func (e timeLogKey) Compare(other amoeba.Sortable) int {
// o := other.(timeLogKey)
// if e.Created.Before(o.Created) {
// return 1
// }
//
// if e.Created.After(o.Created) {
// return -1
// }
//
// return amoeba.CompareUUIDs(e.Id, o.Id)
// }
//
//
// func timeLogScanFrom(start timeLogKey, data amoeba.RawView, fn func(amoeba.Scan, timeLogKey, event)) {
// data.ScanFrom(start, func(s amoeba.Scan, k amoeba.Key, i amoeba.RawItem) {
// evt := timeLogUnpackAmoebaItem(i)
// if evt == nil {
// return // shouldn't be possible...but guarding anyway.
// }
//
// fn(s, timeLogUnpackAmoebaKey(k), evt)
// })
// }
//
// func timeLogScan(data amoeba.RawView, fn func(amoeba.Scan, timeLogKey, event)) {
// data.Scan(func(s amoeba.Scan, k amoeba.Key, i amoeba.RawItem) {
// evt := timeLogUnpackAmoebaItem(i)
// if evt == nil {
// return
// }
//
// fn(s, timeLogUnpackAmoebaKey(k), evt)
// })
// }
//
// // timeLog implementation.
// type timeLog struct {
// Ctx      common.Context
// Data     amoeba.RawIndex
// Dead     time.Duration
// Handlers []func([]event)
// Lock     sync.RWMutex
// }
//
// func newTimeLog(ctx common.Context) *timeLog {
// return &timeLog{
// Ctx:  ctx,
// Data: amoeba.NewIndexer(ctx),
// Dead: ctx.Config().OptionalDuration(ConfTimeLogHorizon, DefaultTimeLogHorizon)}
// }
//
// func (t *timeLog) Close() (ret error) {
// ret = t.Data.Close()
// t.broadcast(nil)
// return
// }
//
// func (t *timeLog) broadcast(batch []event) {
// for _, fn := range t.Listeners() {
// fn(batch)
// }
// }
//
// func (t *timeLog) Listeners() []func([]event) {
// t.Lock.RLock()
// defer t.Lock.RUnlock()
// ret := make([]func([]event), 0, len(t.Handlers))
// for _, fn := range t.Handlers {
// ret = append(ret, fn)
// }
// return ret
// }
//
// func (e *timeLog) Listen(fn func([]event)) {
// e.Lock.Lock()
// defer e.Lock.Unlock()
// e.Handlers = append(e.Handlers, fn)
// }
//
// func (t *timeLog) Peek(after time.Time) (ret []event) {
// ret = []event{}
// t.Data.Read(func(data amoeba.RawView) {
// horizon := data.Time().Add(-t.Dead)
//
// ret = make([]event, 0, 256)
// timeLogScan(data, func(s amoeba.Scan, key timeLogKey, e event) {
// if key.Created.Before(after) || key.Created.Equal(after) || key.Created.Before(horizon) {
// defer s.Stop()
// return
// }
//
// ret = append(ret, e)
// })
// })
// return
// }
//
// func (t *timeLog) Push(batch []event) {
// t.Data.Update(func(data amoeba.RawUpdate) {
// for _, e := range batch {
// data.Put(timeLogKey{uuid.NewV1(), data.Time()}, e, 0)
// }
//
// t.gc(data)
// })
//
// t.broadcast(batch)
// }
//
// func (t *timeLog) gc(data amoeba.RawUpdate) {
// horizon := data.Time().Add(-t.Dead)
//
// dead := make([]timeLogKey, 0, 128)
// timeLogScanFrom(timeLogKey{amoeba.ZeroUUID, horizon}, data, func(s amoeba.Scan, key timeLogKey, e event) {
// dead = append(dead, key)
// })
//
// for _, t := range dead {
// data.DelNow(t)
// }
// }
