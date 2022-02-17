package messaging

import (
	"log"
	"os"
)

// Logger defines a loggng interface for this module.

type Logger interface {
	Print(args ...interface{})
	Printf(format string, args ...interface{})
	Println(args ...interface{})
}

var (
	// Info level logger. Can be set by other packages. Defaults to writing to
	// os.Stdout.
	Info Logger = log.New(os.Stdout, "", log.Lshortfile)

	// Warn level logger. Can be set by other packages. Defaults to writing to
	// os.Stderr.
	Warn Logger = log.New(os.Stderr, "", log.Lshortfile)

	// Error level logger. Can be set by other packages. Default to writing to
	// os.Stderr.
	Error Logger = log.New(os.Stderr, "", log.Lshortfile)
)
