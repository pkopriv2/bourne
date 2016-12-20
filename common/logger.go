package common

import (
	"fmt"
	"log"
)

// TODO: REPLACE ALL OF THIS WITH AN OPEN SOURCE VERSION!

const (
	confLoggerLevel = "bourne.log.level"
)

const (
	defaultLoggerLevel = Info
)

func formatLogger(logger Logger, format string, args ...interface{}) Logger {
	return NewFormattedLogger(logger, format, args...)
}

func print(format string, vals ...interface{}) {
	log.Println(fmt.Sprintf(format, vals...))
}

type LoggerLevel int

const (
	Off   LoggerLevel = iota
	Error
	Info
	Debug
)

func (l LoggerLevel) String() string {
	switch l {
	default:
		return "UNKNOWN"
	case Off:
		return "OFF"
	case Error:
		return "ERROR"
	case Info:
		return "INFO"
	case Debug:
		return "DEBUG"
	}
}

type Logger interface {
	Level() LoggerLevel
	Fmt(string, ...interface{}) Logger
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Error(string, ...interface{})
}

type standardLogger struct {
	level LoggerLevel
}

func NewStandardLogger(c Config) Logger {
	return &standardLogger{LoggerLevel(c.OptionalInt(confLoggerLevel, int(Debug)))}
}

func (s *standardLogger) Level() LoggerLevel {
	return s.level
}

func (s *standardLogger) Fmt(format string, vals ...interface{}) Logger {
	return formatLogger(s, format, vals...)
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

func NewFormattedLogger(base Logger, format string, vals ... interface{}) Logger {
	return &formattedLogger{base, fmt.Sprintf(format, vals...)}
}

func (s *formattedLogger) Level() LoggerLevel {
	return s.log.Level()
}

func (s *formattedLogger) Fmt(format string, vals ...interface{}) Logger {
	return formatLogger(s, format, vals...)
}

func (s *formattedLogger) Debug(format string, vals ...interface{}) {
	if s.Level() >= Debug {
		s.log.Debug(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
	}
}

func (s *formattedLogger) Info(format string, vals ...interface{}) {
	if s.Level() >= Info {
		s.log.Info(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
	}
}

func (s *formattedLogger) Error(format string, vals ...interface{}) {
	if s.Level() >= Error {
		s.log.Error(fmt.Sprintf("%v: %v", s.fmt, format), vals...)
	}
}
