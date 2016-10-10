package common

import (
	"fmt"
	"log"
)

const (
	confLoggerLevel = "bourne.log.level"
)

const (
	defaultLoggerLevel = Error
)

type Logger interface {
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Error(string, ...interface{})
}

type LoggerLevel int

const (
	Debug LoggerLevel = iota
	Info
	Error
)

type standardLogger struct {
	level LoggerLevel
}

func NewStandardLogger(c Config) Logger {
	return &standardLogger{LoggerLevel(c.OptionalInt(confLoggerLevel, int(Debug)))}
}

func (s *standardLogger) Debug(format string, vals ...interface{}) {
	if s.level >= Debug {
		s.Log(format, vals...)
	}
}

func (s *standardLogger) Info(format string, vals ...interface{}) {
	if s.level >= Info {
		s.Log(format, vals...)
	}
}

func (s *standardLogger) Error(format string, vals ...interface{}) {
	if s.level >= Error {
		s.Log(format, vals...)
	}
}

func (e *standardLogger) Log(format string, vals ...interface{}) {
	log.Println(fmt.Sprintf(format, vals...))
}
