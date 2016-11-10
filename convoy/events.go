package convoy

// type joinEvent struct {
// id      uuid.UUID
// version int
// host    string
// port    int
// data    []Change
// }
//
// func newJoinEvent(id uuid.UUID, version int, host string, port int, data []Change) *joinEvent {
// return &joinEvent{id, version, host, port, data}
// }
//
// func (j *joinEvent) Write(w enc.Writer) {
// w.Write("id", j.id.String())
// w.Write("version", j.version)
// w.Write("host", j.host)
// w.Write("port", j.port)
// w.Write("data", j.data)
// }
//
// func (j *joinEvent) Apply(c *directory) error {
// c.dataLock.Lock()
// defer c.dataLock.Unlock()
//
// now := time.Now()
// if !j.updateIdIndex(c, now) {
// return nil
// }
//
// j.updateRefIndex(c, now)
// return nil
// }
//
// func (j *joinEvent) updateIdIndex(c *directory, txTime time.Time) bool {
// new := &datum{
// Time:    txTime,
// Deleted: false,
// Version: j.version,
// Member:  &member{j.id, j.host, j.port}}
//
// existing := c.dataPrimary[j.id]
// if existing == nil {
// c.dataPrimary[j.id] = new
// return true
// }
//
// if existing.Version < new.Version {
// c.dataPrimary[j.id] = new
// return true
// }
//
// return false
// }
//
// func (j *joinEvent) updateRefIndex(c *directory, txTime time.Time) {
// for _, chg := range j.data {
// ref := &ref{j.id, chg.Key(), chg.Val(), chg.Deleted(), chg.Version(), txTime}
// c.dataIndex.Put(ref.RefKey(), ref)
// }
// }
//
// type leaveEvent struct {
// id      uuid.UUID
// version int
// }
//
// func newLeaveEvent(id uuid.UUID, version int) *leaveEvent {
// return &leaveEvent{id, version}
// }
//
// func (j *leaveEvent) Write(w enc.Writer) {
// w.Write("id", j.id.String())
// w.Write("version", j.version)
// }
//
// func (j *leaveEvent) Apply(c *directory) error {
// c.dataLock.Lock()
// defer c.dataLock.Unlock()
//
// // update all the indexes
// if !j.updateIdIndex(c) {
// return nil
// }
//
// j.updateKeyIndex(c)
// j.updateValIndex(c)
// return nil
// }
//
// func (j *leaveEvent) updateIdIndex(c *directory) bool {
// new := &datum{
// Time:    time.Now(),
// Deleted: true,
// Version: j.version}
//
// existing := c.dataPrimary[j.id]
// if existing == nil {
// c.dataPrimary[j.id] = new
// return true
// }
//
// if existing.Version < new.Version {
// c.dataPrimary[j.id] = new
// return true
// }
//
// return false
// }
//
// func (j *leaveEvent) updateKeyIndex(c *directory) {
// return // will be gc'ed
// }
//
// func (j *leaveEvent) updateValIndex(c *directory) {
// return // will be gc'ed
// }
//
// type dataEvent struct {
// id     uuid.UUID
// change Change
// }
//
// func newDataEvent(id uuid.UUID, chg Change) *dataEvent {
// return &dataEvent{id, chg}
// }
//
// func (j *dataEvent) Write(w enc.Writer) {
// w.Write("id", j.id.String())
// w.Write("change", j.change)
// }
//
// func (j *dataEvent) Apply(c *directory) error {
// c.dataLock.Lock()
// defer c.dataLock.Unlock()
//
// now := time.Now()
// j.updateIdIndex(c, now)
// j.updateRefIndex(c, now)
// return nil
// }
//
// func (j *dataEvent) updateIdIndex(c *directory, now time.Time) {
// return // no update to primary index
// }
//
// func (j *dataEvent) updateRefIndex(c *directory, now time.Time) {
// newRef := &ref{
// Id:      j.id,
// Key:     j.change.Key(),
// Val:     j.change.Val(),
// Deleted: j.change.Deleted(),
// Version: j.change.Version(),
// Time:    now}
//
// start := refKey{key: newRef.Key, id: newRef.Id}
//
// var existingRef *ref
// c.dataIndex.ScanRange(start, start.IncrementId(), func(r *ref) bool {
// existingRef = r
// return false
// })
//
// if existingRef == nil {
// c.dataIndex.Put(newRef.RefKey(), newRef)
// return
// }
//
// if newRef.Version > existingRef.Version {
// c.dataIndex.Remove(existingRef.RefKey())
// c.dataIndex.Put(newRef.RefKey(), newRef)
// return
// }
// }
