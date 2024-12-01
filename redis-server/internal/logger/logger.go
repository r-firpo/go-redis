// internal/logger/logger.go

package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go-redis/redis-server/internal/config"
)

// Setup initializes the logger based on configuration
func Setup(cfg *config.Config) (*zap.Logger, error) {
	// Configure log level
	level, err := zapcore.ParseLevel(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", cfg.LogLevel, err)
	}

	// Create encoder config
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// Configure output and encoder
	var encoder zapcore.Encoder
	if cfg.LogFormat == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Configure output writer
	var writer zapcore.WriteSyncer
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("cannot open log file: %w", err)
		}
		writer = zapcore.AddSync(f)
	} else {
		writer = zapcore.AddSync(os.Stdout)
	}

	// Create core
	core := zapcore.NewCore(encoder, writer, level)

	// Create logger
	logger := zap.New(core,
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, nil
}
