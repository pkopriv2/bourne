package elmer

import "time"

type Env interface {
	Config() *config
	Roster() *roster
}

type config struct {

}

func (c *config) Optional(key string, def string) string {
	panic("not implemented")
}

func (c *config) OptionalInt(key string, def int) int {
	panic("not implemented")
}

func (c *config) OptionalBool(key string, def bool) bool {
	panic("not implemented")
}

func (c *config) OptionalDuration(key string, def time.Duration) time.Duration {
	panic("not implemented")
}
