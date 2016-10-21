package convoy

// func TestPendingUpdates_Empty(t *testing.T) {
// updates := newPendingUpdates()
// u, cnt := updates.Pop()
// assert.Nil(t, u)
// assert.Zero(t, cnt)
// }
//
// func TestPendingUpdates_Single(t *testing.T) {
// updates := newPendingUpdates()
//
// update := newDelete(uuid.NewV4(), 0)
// updates.Push(update, 0)
//
// u, cnt := updates.Pop()
// assert.Equal(t, update, u)
// assert.Equal(t, 0, cnt)
//
// // make sure the count is irrelevant.
//
// update = newDelete(uuid.NewV4(), 0)
// updates.Push(update, 1)
//
// u, cnt = updates.Pop()
// assert.Equal(t, update, u)
// assert.Equal(t, 1, cnt)
//
// }
//
// func TestPendingUpdates_Multi(t *testing.T) {
// updates := newPendingUpdates()
//
// update1 := newDelete(uuid.NewV4(), 0)
// update2 := newDelete(uuid.NewV4(), 0)
//
// // push them out of order
// updates.Push(update1, 0)
// updates.Push(update2, 1)
//
// u, cnt := updates.Pop()
// assert.Equal(t, update1, u)
// assert.Equal(t, 0, cnt)
// }
