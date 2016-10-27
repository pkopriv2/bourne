package net

// func TestConnector_initError(t *testing.T) {
// factory := func() (Connection, error) {
// return nil, errors.New("error")
// }
//
// conn := NewConnector(factory, utils.NewEmptyConfig())
// assert.NotNil(t, conn)
// }
//
// func TestConnector_initTimeout(t *testing.T) {
// factory := func() (Connection, error) {
// time.Sleep(2 * time.Second)
// return nil, nil
// }
//
// conn := NewConnector(factory, utils.NewEmptyConfig())
// assert.NotNil(t, conn)
// }
