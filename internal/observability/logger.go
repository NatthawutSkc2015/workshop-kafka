// Package observability provides logging, tracing, and metrics functionality
package observability

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger creates a new structured logger
func NewLogger(env string) (*zap.Logger, error) {
	var config zap.Config
	if env == "production" {
		config = zap.NewProductionConfig()
		config.EncoderConfig.TimeKey = "timestamp"
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		config.DisableStacktrace = true
	} else {
		config = zap.NewDevelopmentConfig()
		// config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

		ec := &config.EncoderConfig
		ec.EncodeLevel = zapcore.CapitalColorLevelEncoder
		ec.EncodeCaller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
			const width = 40
			s := caller.TrimmedPath()
			if len(s) < width {
				s = s + strings.Repeat(" ", width-len(s))
			}
			enc.AppendString(s)
		}

		ec.ConsoleSeparator = "\x1b[36m | \x1b[0m"
		// ec.EncodeTime = zapcore.ISO8601TimeEncoder
		ec.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString("\x1b[36m" + t.Format("02/01/2006 15:04:05") + "\x1b[0m")
		}

		config.DisableStacktrace = true
	}

	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.PanicLevel),
		// zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, err
	}

	return logger, nil
}
