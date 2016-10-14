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

func FormatLogger(logger Logger, format fmt.Stringer, args ...interface{}) Logger {
	return NewFormattedLogger(logger, format, args...)
}

func print(format string, vals ...interface{}) {
	log.Println(fmt.Sprintf(format, vals...))
}

type Logger interface {
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Error(string, ...interface{})
}

type LoggerLevel int

const (
	Error LoggerLevel = iota
	Info
	Debug
)

type standardLogger struct {
	level LoggerLevel
}

func NewStandardLogger(c Config) Logger {
	return &standardLogger{LoggerLevel(c.OptionalInt(confLoggerLevel, int(Debug)))}
}

func (s *standardLogger) Debug(format string, vals ...interface{}) {
	if s.level >= Debug {
		print(format, vals...)
	}
}

func (s *standardLogger) Info(format string, vals ...interface{}) {
	if s.level >= Info {
		print(format, vals...)
	}
}

func (s *standardLogger) Error(format string, vals ...interface{}) {
	if s.level >= Error {
		print(format, vals...)
	}
}

type formattedLogger struct {
	log Logger
	fmt string
}

func NewFormattedLogger(base Logger, format fmt.Stringer, vals ... interface{}) Logger {
	return &formattedLogger{base, fmt.Sprintf(format.String(), vals...)}
}

func (s *formattedLogger) Debug(format string, vals ...interface{}) {
	s.log.Debug(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
}

func (s *formattedLogger) Info(format string, vals ...interface{}) {
	s.log.Info(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
}

func (s *formattedLogger) Error(format string, vals ...interface{}) {
	s.log.Error(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
}
