package logger

import (
	"errors"
	"sync/atomic"
)

type ILogger interface {
	All(v ...interface{})
	AllF(format string, v ...interface{})
	Debug(v ...interface{})
	DebugF(format string, v ...interface{})
	Info(v ...interface{})
	InfoF(format string, v ...interface{})
	Warning(v ...interface{})
	WarningF(format string, v ...interface{})
	Error(v ...interface{})
	ErrorF(format string, v ...interface{})
}

var (
	inst         ILogger = &exampleLogger{}
	loggerSetTag int32
)

func GetLoggerInstance() ILogger {
	return inst
}

func SetLoggerInstance(loggerInst ILogger) error {
	if loggerInst == nil {
		return errors.New("the parameter loggerInst is a nil point")
	}

	if !atomic.CompareAndSwapInt32(&loggerSetTag, 0, 1) {
		return errors.New("repeatedly setting log instances")
	}

	inst = loggerInst
	return nil
}
