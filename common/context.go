package common

import (
	"io"
	"time"
)

type Context interface {
	io.Closer

	Env() Env
	Config() Config
	Logger() Logger
	Control() Control
	Timer(time.Duration) Control
	Sub(fmt string, args ...interface{}) Context
}

type ctx struct {
	config  Config
	logger  Logger
	control Control
	env     *env
}

func NewContext(config Config) Context {
	return &ctx{config: config, logger: NewStandardLogger(config), env: NewEnv(), control: NewControl(nil)}
}

func NewEmptyContext() Context {
	return NewContext(NewEmptyConfig())
}

func (c *ctx) Close() error {
	return c.control.Close()
}

func (c *ctx) Env() Env {
	return c.env
}

func (c *ctx) Config() Config {
	return c.config
}

func (c *ctx) Control() Control {
	return c.control
}

func (c *ctx) Logger() Logger {
	return c.logger
}

func (c *ctx) Timer(dur time.Duration) Control {
	return NewTimer(c.Control(), dur)
}

func (c *ctx) Sub(fmt string, args ...interface{}) Context {
	return &ctx{env: c.env, config: c.config, logger: c.logger.Fmt(fmt, args...), control: c.control.Sub()}
}

func (c *ctx) FormatLogger(fmt string, args ...interface{}) Logger {
	return formatLogger(c.logger, fmt, args...)
}
