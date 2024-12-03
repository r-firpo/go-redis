// internal/replication/master.go

package replication

import (
	"fmt"
	"go-redis/redis-server/internal/protocol"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"go-redis/redis-server/internal/config"
	"go-redis/redis-server/internal/store"
)

type MasterHandler struct {
	config   *config.Config
	store    *store.DataStore
	logger   *zap.Logger
	replicas map[string]*ReplicaInfo // addr -> replica info
	backlog  []byte                  // Command backlog
	mu       sync.RWMutex
	Offset   int64 // Master's current Offset
}

type ReplicaInfo struct {
	Conn        net.Conn
	Writer      *protocol.RESPWriter
	Offset      int64 // This replica's current replication offset
	LastAckTime time.Time
}

func NewMasterHandler(cfg *config.Config, store *store.DataStore, logger *zap.Logger) *MasterHandler {
	return &MasterHandler{
		config:   cfg,
		store:    store,
		logger:   logger,
		replicas: make(map[string]*ReplicaInfo),
		backlog:  make([]byte, 0), // Initialize empty backlog
		Offset:   0,               // Start at offset 0
	}
}

func (m *MasterHandler) AddReplica(conn net.Conn, writer *protocol.RESPWriter) error {
	addr := conn.RemoteAddr().String()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if replica already exists
	existingReplica, exists := m.replicas[addr]
	if exists {
		existingReplica.Conn.Close()
		// Don't increment ConnectedSlaves since this is a reconnection
	} else {
		m.config.Replication.ConnectedSlaves++
	}

	// Start new replica at current offset
	m.replicas[addr] = &ReplicaInfo{
		Conn:        conn,
		Writer:      writer,
		Offset:      m.Offset, // Start at current master offset
		LastAckTime: time.Now(),
	}

	m.logger.Info("Replica connection handled",
		zap.String("addr", addr),
		zap.Int("total_replicas", m.config.Replication.ConnectedSlaves),
		zap.Bool("was_reconnection", exists))

	return nil
}

// RemoveReplica unregisters a replica
func (m *MasterHandler) RemoveReplica(conn net.Conn) {
	if conn == nil {
		return
	}

	addr := conn.RemoteAddr().String()

	m.mu.Lock()
	defer m.mu.Unlock()

	if replica, exists := m.replicas[addr]; exists {
		m.logger.Info("Removing replica",
			zap.String("addr", addr),
			zap.Int64("last_offset", replica.Offset),
			zap.Time("last_ack", replica.LastAckTime))

		delete(m.replicas, addr)
		m.config.Replication.ConnectedSlaves--

		m.logger.Info("Replica disconnected",
			zap.String("addr", addr),
			zap.Int("remaining_replicas", len(m.replicas)))
	}
}

// SendRDBToReplica sends an RDB snapshot to a replica
func (m *MasterHandler) SendRDBToReplica(writer *protocol.RESPWriter) error {
	// Force RDB save
	if err := m.store.SaveRDB(); err != nil {
		return fmt.Errorf("failed to create RDB snapshot: %w", err)
	}

	// Open and read RDB file
	rdbPath := filepath.Join(m.config.Dir, m.config.DBFilename)
	file, err := os.Open(rdbPath)
	if err != nil {
		return fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get RDB file size: %w", err)
	}

	// Send file size as bulk string header
	sizeHeader := fmt.Sprintf("$%d\r\n", stat.Size())
	if _, err := writer.Write([]byte(sizeHeader)); err != nil {
		return fmt.Errorf("failed to send size header: %w", err)
	}

	// Send file contents
	if _, err := io.Copy(writer.Writer(), file); err != nil {
		return fmt.Errorf("failed to send RDB file: %w", err)
	}

	m.logger.Info("Sent RDB file to replica",
		zap.Int64("size", stat.Size()),
	)
	return nil
}

func (m *MasterHandler) PropagateCommand(cmd []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add to backlog and update master offset
	m.backlog = append(m.backlog, cmd...)
	m.Offset += int64(len(cmd))

	// Trim backlog if needed
	if len(m.backlog) > m.config.Replication.BacklogSize {
		excess := len(m.backlog) - m.config.Replication.BacklogSize
		m.backlog = m.backlog[excess:]
		m.config.Replication.BacklogOffset += int64(excess)
	}

	// Send to all replicas
	for addr, replica := range m.replicas {
		if _, err := replica.Writer.Write(cmd); err != nil {
			m.logger.Error("Failed to propagate command to replica",
				zap.String("addr", addr),
				zap.Error(err),
			)
			// Schedule replica removal
			go func(conn net.Conn) {
				m.RemoveReplica(conn)
			}(replica.Conn)
		}
	}
}

// ProcessACK handles REPLCONF ACK from replica
func (m *MasterHandler) ProcessACK(addr string, offsetStr string) error {
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset in ACK: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if replica, exists := m.replicas[addr]; exists {
		replica.Offset = offset // Update replica's reported offset
		replica.LastAckTime = time.Now()

		m.logger.Debug("Processed replica ACK",
			zap.String("addr", addr),
			zap.Int64("replica_offset", offset),
			zap.Int64("master_offset", m.Offset))
	}

	return nil
}

// WaitForReplicas waits for the specified number of replicas to reach the given offset
func (m *MasterHandler) WaitForReplicas(numReplicas int, targetOffset int64, timeout time.Duration) int {
	if len(m.replicas) == 0 {
		return 0
	}

	// Create a deadline for timeout
	deadline := time.Now().Add(timeout)
	count := 0
	var unlocked bool
	// Safety defer in case of panic
	defer func() {
		if !unlocked {
			m.mu.RUnlock()
		}
	}()

	for time.Now().Before(deadline) {
		m.mu.RLock()
		unlocked = false
		for _, replica := range m.replicas {
			if replica.Offset >= targetOffset {
				count++
			}
		}
		m.mu.RUnlock()
		unlocked = true // Mark as unlocked

		// If we have enough replicas, return immediately
		if count >= numReplicas {
			return count
		}

		// Wait a bit before checking again
		time.Sleep(10 * time.Millisecond)
	}

	return count
}
