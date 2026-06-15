package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

var L zerolog.Logger

func Init(service string) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
		NoColor:    false,
	}

	L = zerolog.New(output).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Str("service", service).
		Logger()
}

func Get() zerolog.Logger {
	return L
}
