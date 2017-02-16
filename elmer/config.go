package elmer

var (
	configStore = []byte("Elmer.Conf")
)

// type config struct {
// ctx     common.Context
// ctrl    common.Control
// logger  common.Logger
// indexer *indexer
// }
//
// func (c *config) Update(cancel <-chan struct{}, key string, func(val string) string) (string, error) {
// if err := c.indexer.StoreEnsure(cancel, configStore); err != nil {
// return "", errors.Wrapf(err, "Unable to retrieve config key [%v]", key)
// }
//
// c.logger.Info("Retrieving config key [%v]", key)
// item, ok, err := c.indexer.StoreReadItem(cancel, configStore, []byte(key))
// if err != nil {
// return "", errors.WithStack(err)
// }
//
// if ! ok {
// return def, nil
// } else {
// return string(item.Val), nil
// }
// }
//
// func (c *config) Optional(cancel <-chan struct{}, key string, def string) (string, error) {
// if err := c.indexer.StoreEnsure(cancel, configStore); err != nil {
// return "", errors.Wrapf(err, "Unable to retrieve config key [%v]", key)
// }
//
// c.logger.Info("Retrieving config key [%v]", key)
// item, ok, err := c.indexer.StoreReadItem(cancel, configStore, []byte(key))
// if err != nil {
// return "", errors.WithStack(err)
// }
//
// if ! ok {
// return def, nil
// } else {
// return string(item.Val), nil
// }
// }
//
// func (c *config) OptionalInt(key string, def int) int {
// panic("not implemented")
// }
//
// func (c *config) OptionalBool(key string, def bool) bool {
// panic("not implemented")
// }
//
// func (c *config) OptionalDuration(key string, def time.Duration) time.Duration {
// panic("not implemented")
// }
