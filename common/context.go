package common

import "io"

type Context interface {
	io.Closer

	Env() Env
	Config() Config
	Logger() Logger
}

type ctx struct {
	config Config
	logger Logger
	env *env
}

func NewContext(config Config) Context {
	return &ctx{config: config, logger: NewStandardLogger(config), env: NewEnv()}
}

func (c *ctx) Close() error {
	return c.env.Close()
}

func (c *ctx) Env() Env {
	return c.env
}

func (c *ctx) Config() Config {
	return c.config
}

func (c *ctx) Logger() Logger {
	return c.logger
}
