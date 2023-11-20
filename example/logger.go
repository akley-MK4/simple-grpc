package example

import (
	"fmt"
	"log"
	"os"
)

func newExample(prefix string) *exampleLogger {
	eLogger := &exampleLogger{
		lg:        log.New(os.Stdout, prefix, log.Llongfile|log.Ldate|log.Ltime),
		calldepth: 2,
	}

	return eLogger
}

type exampleLogger struct {
	lg        *log.Logger
	calldepth int
}

func (t *exampleLogger) All(v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintln(v...))
}

func (t *exampleLogger) AllF(format string, v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintf(format, v...))
}

func (t *exampleLogger) Debug(v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintln(v...))
}

func (t *exampleLogger) DebugF(format string, v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintf(format, v...))
}

func (t *exampleLogger) Info(v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintln(v...))
}

func (t *exampleLogger) InfoF(format string, v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintf(format, v...))
}

func (t *exampleLogger) Warning(v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintln(v...))
}

func (t *exampleLogger) WarningF(format string, v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintf(format, v...))
}

func (t *exampleLogger) Error(v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintln(v...))
}

func (t *exampleLogger) ErrorF(format string, v ...interface{}) {
	t.lg.Output(t.calldepth, fmt.Sprintf(format, v...))
}
