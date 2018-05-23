package util

import (
	"os"

	"github.com/henrylee2cn/go-logging"
)


var Llog = logging.NewLogger("example")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var logFormat = logging.MustStringFormatter(
	`%{level:.4s} %{color:bold}%{module} %{time:15:04:05.000} %{longfile} ▶ %{id:03x}%{color:reset} %{message}`,
)

// Password is just an example type implementing the Redactor interface. Any
// time this is logged, the Redacted() function will be called.
type Password string

func (p Password) Redacted() interface{} {
	return logging.Redact(string(p))
}

func InitLog(logName string) {

	backend2 := logging.NewLogBackend(os.Stderr, "", 0)
	backend2.Color = true

	backend3, err := logging.NewDefaultFileBackend(logName + ".log")
	if err != nil {
		panic(err)
	}
	backend3Formatter := logging.NewBackendFormatter(backend3, logFormat)

	// For messages written to backend2 we want to add some additional
	// information to the output, including the used log level and the name of
	// the function.
	backend2Formatter := logging.NewBackendFormatter(backend2, logFormat)

	// Only errors and more severe messages should be sent to backend1

	// Set the backends to be used.
	logging.SetBackend(logging.MultiLogger(backend2Formatter, backend3Formatter))
}