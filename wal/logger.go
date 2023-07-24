package wal

import "log"

var (
	defaultLogger __loggerSpec = &stdLogger{}
)

var (
	_ __loggerSpec = (*nopLogger)(nil)
	_ __loggerSpec = (*stdLogger)(nil)
)

type __loggerSpec interface {
	Log(format string, args ...interface{})
}

type nopLogger struct{}

func (n *nopLogger) Log(format string, args ...interface{}) {}

type stdLogger struct{}

func (s *stdLogger) Log(format string, args ...interface{}) {
	if format[len(format)-1] != '\n' {
		format += "\n"
	}
	log.Printf("WAL: "+format, args...)
}
