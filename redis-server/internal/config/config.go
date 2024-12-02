// internal/config/config.go

package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Role represents server role (master/slave)
type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

// ReplicationConfig holds replication-specific configuration
type ReplicationConfig struct {
	// Master info
	MasterHost   string
	MasterPort   int
	MasterReplID string // Replication ID
	MasterOffset int64  // Replication offset

	// Backlog settings
	BacklogSize   int // Size of replication backlog buffer
	BacklogOffset int64

	// Connection settings
	ConnectedSlaves int
	SlaveReadOnly   bool
}

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

	// Logging
	LogLevel  string
	LogFormat string
	LogFile   string

	// Replication settings
	Role        Role
	Replication *ReplicationConfig
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
		LogLevel:        "info",
		LogFormat:       "console", // or "json"
		Role:            RoleMaster,
		Replication: &ReplicationConfig{
			MasterReplID:    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // Default replication ID
			BacklogSize:     1048576,                                    // 1MB default backlog
			SlaveReadOnly:   true,
			ConnectedSlaves: 0,
		},
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

	// Parse replication settings
	replicaOf := flag.String("replicaof", "", "Master host and port (e.g., 'localhost 6379')")
	flag.Parse()

	// Parse flags
	flag.Parse()

	// Handle replicaof flag
	if *replicaOf != "" {
		parts := strings.Fields(*replicaOf)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid replicaof format, expected 'host port', got %s", *replicaOf)
		}

		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid port in replicaof: %v", err)
		}

		config.Role = RoleSlave
		config.Replication.MasterHost = parts[0]
		config.Replication.MasterPort = port
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
