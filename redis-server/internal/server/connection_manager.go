// internal/server/connection_manager.go

package server

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ConnectionInfo holds information about a client connection
type ConnectionInfo struct {
	// RemoteAddr is the client's address
	RemoteAddr string
	// ConnectedAt is when the connection was established
	ConnectedAt time.Time
	// LastActivity is the timestamp of the last read/write
	LastActivity time.Time
	// BytesRead is the total bytes read from this connection
	BytesRead uint64
	// BytesWritten is the total bytes written to this connection
	BytesWritten uint64
}

// ConnectionManager handles tracking and managing client connections
type ConnectionManager struct {
	// connections maps connection IDs to their ConnectionInfo
	connections sync.Map
	// logger is used for logging connection events
	logger *zap.Logger
}

// NewConnectionManager creates a new ConnectionManager instance
func NewConnectionManager(logger *zap.Logger) *ConnectionManager {
	return &ConnectionManager{
		logger: logger,
	}
}

// AddConnection registers a new connection
func (cm *ConnectionManager) AddConnection(conn net.Conn) {
	info := &ConnectionInfo{
		RemoteAddr:   conn.RemoteAddr().String(),
		ConnectedAt:  time.Now(),
		LastActivity: time.Now(),
	}
	cm.connections.Store(conn.RemoteAddr().String(), info)
	cm.logger.Info("New connection added",
		zap.String("remote_addr", info.RemoteAddr),
		zap.Time("connected_at", info.ConnectedAt),
	)
}

// RemoveConnection unregisters a connection
func (cm *ConnectionManager) RemoveConnection(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	if info, ok := cm.connections.LoadAndDelete(addr); ok {
		connInfo := info.(*ConnectionInfo)
		cm.logger.Info("Connection removed",
			zap.String("remote_addr", addr),
			zap.Duration("connection_duration", time.Since(connInfo.ConnectedAt)),
		)
	}
}

// UpdateActivity updates the last activity timestamp and bytes counts for a connection
func (cm *ConnectionManager) UpdateActivity(conn net.Conn, bytesRead, bytesWritten uint64) {
	if info, ok := cm.connections.Load(conn.RemoteAddr().String()); ok {
		connInfo := info.(*ConnectionInfo)
		connInfo.LastActivity = time.Now()
		connInfo.BytesRead += bytesRead
		connInfo.BytesWritten += bytesWritten
	}
}

// GetConnectionInfo returns information about all active connections
func (cm *ConnectionManager) GetConnectionInfo() string {
	var info strings.Builder
	fmt.Fprintf(&info, "Active connections: ")

	count := 0
	cm.connections.Range(func(key, value interface{}) bool {
		count++
		connInfo := value.(*ConnectionInfo)
		fmt.Fprintf(&info, "\n  %s:", connInfo.RemoteAddr)
		fmt.Fprintf(&info, "\n    Connected: %s", time.Since(connInfo.ConnectedAt))
		fmt.Fprintf(&info, "\n    Last activity: %s", time.Since(connInfo.LastActivity))
		fmt.Fprintf(&info, "\n    Bytes read: %d", connInfo.BytesRead)
		fmt.Fprintf(&info, "\n    Bytes written: %d", connInfo.BytesWritten)
		return true
	})

	if count == 0 {
		info.WriteString("0")
	} else {
		fmt.Fprintf(&info, "\nTotal connections: %d", count)
	}

	return info.String()
}
