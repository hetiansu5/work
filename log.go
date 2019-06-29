package work

type Logger interface {
	Trace(v ...interface{})
	Tracef(format string, args ...interface{})
	Debug(v ...interface{})
	Debugf(format string, args ...interface{})
	Info(v ...interface{})
	Infof(format string, args ...interface{})
	Warn(v ...interface{})
	Warnf(format string, args ...interface{})
	Error(v ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, args ...interface{})
}
