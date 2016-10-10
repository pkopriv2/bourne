package common

type Context interface {
	Config() Config
	Logger() Logger
}

type ctx struct {
	config Config
	logger Logger
}

func NewContext(config Config) Context {
	return &ctx{config, NewStandardLogger(config)}
}

func (c *ctx) Config() Config {
	return c.config
}

func (c *ctx) Logger() Logger {
	return c.logger
}
