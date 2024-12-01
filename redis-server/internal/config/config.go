// internal/config/config.go

package config

import (
	"flag"
	"fmt"
	"os"
)

// Config holds all server configuration
type Config struct {
	// Server settings
	Host string
	Port int

	// Connection settings
	BacklogSize     int
	BufferLimit     int
	MonitorInterval int

	// RDB settings
	Dir        string
	DBFilename string

	// Replication settings
	Role            string // "master" or "slave"
	MasterHost      string
	MasterPort      int
	ReplBacklogSize int

	// Logging
	LogLevel  string
	LogFormat string
	LogFile   string
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:            "localhost",
		Port:            6379,
		BacklogSize:     100,
		BufferLimit:     65536,
		MonitorInterval: 5,
		Dir:             "/tmp/redis-files-rdb",
		DBFilename:      "dump.rdb",
		Role:            "master",
		ReplBacklogSize: 1048576,
		LogLevel:        "info",
		LogFormat:       "console", // or "json"
	}
}

// ParseFlags parses command line arguments into a Config
func ParseFlags() (*Config, error) {
	config := DefaultConfig()

	// Define flags
	flag.StringVar(&config.Host, "host", config.Host, "Server host")
	flag.IntVar(&config.Port, "port", config.Port, "Server port")
	flag.StringVar(&config.Dir, "dir", config.Dir, "RDB directory path")
	flag.StringVar(&config.DBFilename, "dbfilename", config.DBFilename, "RDB filename")
	flag.StringVar(&config.LogLevel, "loglevel", config.LogLevel, "Log level (debug, info, warn, error)")
	flag.StringVar(&config.LogFormat, "logformat", config.LogFormat, "Log format (console, json)")
	flag.StringVar(&config.LogFile, "logfile", "", "Log file path (empty for stdout)")

	// Replication flags
	replicaOf := flag.String("replicaof", "", "Master host and port (e.g., 'localhost 6379')")

	// Parse flags
	flag.Parse()

	// Handle replicaof flag
	if *replicaOf != "" {
		var host string
		var port int
		if _, err := fmt.Sscanf(*replicaOf, "%s %d", &host, &port); err != nil {
			return nil, fmt.Errorf("invalid replicaof format, expected 'host port', got %s", *replicaOf)
		}
		config.Role = "slave"
		config.MasterHost = host
		config.MasterPort = port
	}

	// Ensure RDB directory exists
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create RDB directory: %w", err)
	}

	return config, nil
}

// String returns a string representation of the config
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{Host:%s, Port:%d, Role:%s, Dir:%s, DBFilename:%s, LogLevel:%s}",
		c.Host, c.Port, c.Role, c.Dir, c.DBFilename, c.LogLevel,
	)
}
