package common

import (
	"fmt"
	"time"
)

// The goal of this package is to move configuration to a mostly runtime
// consideration.  In many instances, functions which return errors, will
// have analogous "panicking" methods.  This is to (and rightfully so)
// terminate the program as soon as possible.


// In order to support a more robust configuration system, some config
// values will be encoded as different types than what is returned.
// For example, durations will not be stored in explicit time.Duration
// format, but instead will be stored as a normal integer (type: int)
// and interpreted as milliseconds.   This should allow for a nice balance
// between compile time guarantees and operational simplicity.
type ConfigType string

const (
	Bool     = "bool"
	Int      = "int"
	Duration = "int(milliseconds)"
)

type ConfigMissingError struct {
	key string
}

func (c ConfigMissingError) Error() string {
	return fmt.Sprintf("Config is missing key [%s]", c.key)
}

type ConfigParsingError struct {
	expected ConfigType
	key      string
	val      interface{}
}

func (c ConfigParsingError) Error() string {
	return fmt.Sprintf("Error parsing config key [%s].  Expected type [%s], which can't be converted from [%v]", c.key, c.expected, c.val)
}

func newConfigMissingError(key string) ConfigMissingError {
	return ConfigMissingError{key}
}

func newConfigParsingError(expected ConfigType, key string, val interface{}) ConfigParsingError {
	return ConfigParsingError{expected, key, val}
}

type Configured interface {
	Config() Config
}

type Config interface {
	OptionalInt(key string, def int) int
	OptionalBool(key string, def bool) bool
	OptionalDuration(key string, def time.Duration) time.Duration
}

func NewEmptyConfig() Config {
	return NewConfig(nil)
}

func NewConfig(internal map[string]interface{}) Config {
	if internal == nil {
		internal = make(map[string]interface{})
	}

	return &config{internal}
}

type config struct {
	internal map[string]interface{}
}

func (c *config) OptionalInt(key string, def int) int {
	val, err := readInt(c.internal, key)
	if err == nil {
		return val
	}

	switch err.(type) {
	case ConfigMissingError:
		return def
	}

	panic(err)
}

func (c *config) OptionalBool(key string, def bool) bool {
	val, err := readBool(c.internal, key)
	if err == nil {
		return val
	}

	switch err.(type) {
	case ConfigMissingError:
		return def
	}

	panic(err)
}

func (c *config) OptionalDuration(key string, def time.Duration) time.Duration {
	val, err := readDuration(c.internal, key)
	if err == nil {
		return val
	}

	switch err.(type) {
	case ConfigMissingError:
		return def
	}

	panic(err)
}

func readInt(m map[string]interface{}, key string) (int, error) {
	val, ok := m[key]
	if ! ok {
		return 0, newConfigMissingError(key)
	}

	ret, ok := val.(int)
	if ! ok {
		return 0, newConfigParsingError(Int, key, val)
	}

	return ret, nil
}

func readBool(m map[string]interface{}, key string) (bool, error) {
	val, ok := m[key]
	if ! ok {
		return false, newConfigMissingError(key)
	}

	ret, ok := val.(bool)
	if ! ok {
		return false, newConfigParsingError(Bool, key, val)
	}

	return ret, nil
}

func readDuration(m map[string]interface{}, key string) (time.Duration, error) {
	val, ok := m[key]
	if ! ok {
		return 0, newConfigMissingError(key)
	}

	ret, ok := val.(int)
	if ! ok {
		return 0, newConfigParsingError(Duration, key, val)
	}

	return time.Duration(ret), nil
}
