package core

import "github.com/pkopriv2/bourne/utils"

type Context interface {
	Config() utils.Config
	Logger() utils.Logger
}

type ctx struct {
	config utils.Config
	logger utils.Logger
}

func NewContext(config utils.Config) Context {
	return &ctx{config, utils.NewStandardLogger(config)}
}

func (c *ctx) Config() utils.Config {
	return c.config
}

func (c *ctx) Logger() utils.Logger {
	return c.logger
}
