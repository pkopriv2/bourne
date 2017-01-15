package kayak

var (
	latestBucket   = []byte("kayak.latest.segment")
	segmentsBucket = []byte("kayak.segments")
	itemsBucket    = []byte("kayak.items")
	headsBucket    = []byte("kayak.heads")
	snapshotBucket = []byte("kayak.snapshots")
	eventsBucket   = []byte("kayak.events")
)

// func parseItem(bytes []byte) (LogItem, error) {
// msg, err := scribe.Parse(bytes)
// if err != nil {
// return LogItem{}, err
// }
//
// raw, err := readLogItem(msg)
// if err != nil {
// return LogItem{}, err
// }
//
// return raw.(LogItem), nil
// }
//
// // func parseDurableSegment(bytes []byte) (*durableSegment, error) {
// // msg, err := scribe.Parse(bytes)
// // if err != nil {
// // return nil, err
// // }
// //
// // return readDurableSegment(msg)
// // }
// //
// // func parseDurableItem(bytes []byte) (*durableItem, error) {
// // msg, err := scribe.Parse(bytes)
// // if err != nil {
// // return nil, err
// // }
// //
// // return readDurableItem(msg)
// // }
//
// type durableItem struct {
// segmentId uuid.UUID
// raw LogItem
// }
//
// func (d *durableItem) Write(w scribe.Writer) {
// w.WriteUUID("segmentId", d.segmentId)
// w.WriteMessage("raw", d.raw)
// }
//
// func durableItemParser(r scribe.Reader) (interface{}, error) {
// item := &durableItem{}
//
// err = r.ReadUUID("segmentId", &item.segmentId)
// err = common.Or(err, r.ReadMessage("raw", &msg))
// if err != nil {
// return nil, err
// }
//
// item, err := readLogItem(r)
// if err != nil {
// return nil, err
// }
//
// return &durableItem{segmentId, item}, nil
// }
//
// type durableSegment struct {
// id uuid.UUID
//
// prevSnapshot uuid.UUID
// prevIndex    int
// prevTerm     int
// }
//
// func readDurableSegment(r scribe.Reader) (seg *durableSegment, err error) {
// seg = &durableSegment{}
// err = r.ReadUUID("id", &seg.id)
// err = common.Or(err, r.ReadUUID("prevSnapshot", &seg.prevSnapshot))
// err = common.Or(err, r.ReadInt("prevIndex", &seg.prevIndex))
// err = common.Or(err, r.ReadInt("prevTerm", &seg.prevTerm))
// return
// }
//
// func createDurableSegment(tx bolt.Tx, snapshotEvents []Event, snapshotConfig []byte) *durableSegment {
// return nil
// }
//
// func (d *durableSegment) Write(w scribe.Writer) {
// w.WriteUUID("id", d.id)
// w.WriteUUID("prevSnapshot", d.prevSnapshot)
// w.WriteInt("prevIndex", d.prevIndex)
// w.WriteInt("PrevTerm", d.prevTerm)
// }
//
// func (d *durableSegment) Bytes() []byte {
// return scribe.Write(d).Bytes()
// }
//
// func (d *durableSegment) Key() stash.Key {
// return stash.NewUUIDKey(d.id)
// }
//
// func (d *durableSegment) Compact(tx *bolt.Tx) *durableSegment {
// return nil
// }
//
// func (d *durableSegment) Head(tx *bolt.Tx) (int, error) {
// raw := tx.Bucket(headsBucket).Get(d.Key())
// if raw == nil {
// return 0, nil
// }
//
// return stash.ParseInt(raw)
// }
//
// func (d *durableSegment) Get(tx *bolt.Tx, index int) (LogItem, bool, error) {
// val := tx.Bucket(itemsBucket).Get(d.Key().Raw())
// if val == nil {
// return LogItem{}, false, nil
// }
// item, err := parseItem(val)
// if err != nil {
// return LogItem{}, false, err
// }
// return item, true, nil
// }
//
// // Scan inclusive of start and end
// func (d *durableSegment) Scan(tx *bolt.Tx, start int, end int) (batch []LogItem, err error) {
// cursor := tx.Bucket(itemsBucket).Cursor()
//
// batch = make([]LogItem, 0, end-start)
//
// for _, v := cursor.Seek(d.Key().ChildInt(0).Raw()); v != nil; _,v = cursor.Next() {
// i, e := parseDurableItem(v)
// if e != nil {
// return []LogItem{}, e
// }
//
// if i.segmentId == d.id {
// batch = append(batch, i.raw)
// }
// }
//
// return batch, nil
//
// }
//
// func (d *durableSegment) Append(tx *bolt.Tx, batch []Event, term int) (int, error) {
// index, err := d.Head(tx)
// if err != nil {
// return 0, nil
// }
//
// bucket := tx.Bucket(itemsBucket)
// for _, e := range batch {
// index++
// bucket.Put(d.Key().ChildInt(index).Raw(), durableItem{d.id, LogItem{index, e, term}})
// }
// return cur
// }
//
// func (d *durableSegment) Insert(tx *bolt.Tx, batch []LogItem) (head int) {
// return
// }
//
// type durableSnapshot struct {
// replicaId  uuid.UUID
// snapshotId uuid.UUID
//
// len    int
// config string
// }
//
// type durableLog struct {
// bucket []byte
// logId  []byte
//
// logMin int
// logMax int
// }
