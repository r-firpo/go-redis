// internal/replication/manager.go

package replication

import (
	"context"
	"fmt"
	"go-redis/redis-server/internal/store"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go-redis/redis-server/internal/config"
	"go-redis/redis-server/internal/protocol"
)

// ReplicationState tracks the current replication state
type ReplicationState struct {
	masterConn      net.Conn
	masterReader    *protocol.RESPReader
	masterWriter    *protocol.RESPWriter
	offset          int64
	replid          string
	masterLinkState string
	lastIOTime      time.Time
	mu              sync.RWMutex
}

// Manager handles replication logic
type Manager struct {
	config *config.Config
	logger *zap.Logger
	state  *ReplicationState
	ctx    context.Context
	store  *store.DataStore
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewManager creates a new replication manager
func NewManager(cfg *config.Config, store *store.DataStore, logger *zap.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		config: cfg,
		store:  store,
		logger: logger,
		state:  &ReplicationState{masterLinkState: "down"},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins the replication process
func (m *Manager) Start() error {
	if m.config.Role != config.RoleSlave {
		return nil // Only slaves need to start replication
	}

	m.wg.Add(1)
	go m.connectLoop()

	return nil
}

// Stop gracefully stops replication
func (m *Manager) Stop() error {
	m.cancel()
	m.wg.Wait()

	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.state.masterConn != nil {
		m.state.masterConn.Close()
		m.state.masterConn = nil
	}

	return nil
}

// connectLoop continuously attempts to connect to master
func (m *Manager) connectLoop() {
	defer m.wg.Done()

	retryDelay := time.Second

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			if err := m.connectToMaster(); err != nil {
				m.logger.Error("Failed to connect to master",
					zap.Error(err),
					zap.String("master", fmt.Sprintf("%s:%d",
						m.config.Replication.MasterHost,
						m.config.Replication.MasterPort)),
				)
				time.Sleep(retryDelay)
				continue
			}

			// Wait for connection to be broken or context cancelled
			select {
			case <-m.ctx.Done():
				return
			case <-m.waitForDisconnect():
				m.logger.Warn("Lost connection to master, attempting reconnect")
				time.Sleep(retryDelay)
			}
		}
	}
}

// connectToMaster establishes connection and performs handshake
func (m *Manager) connectToMaster() error {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	// Connect to master
	conn, err := net.DialTimeout("tcp",
		fmt.Sprintf("%s:%d",
			m.config.Replication.MasterHost,
			m.config.Replication.MasterPort,
		),
		5*time.Second,
	)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}

	m.state.masterConn = conn
	m.state.masterReader = protocol.NewRESPReader(conn)
	m.state.masterWriter = protocol.NewRESPWriter(conn)
	m.state.lastIOTime = time.Now()

	// Perform handshake
	if err := m.handshake(); err != nil {
		conn.Close()
		m.state.masterConn = nil
		return fmt.Errorf("handshake failed: %w", err)
	}

	// Receive RDB file
	if err := m.receiveRDB(); err != nil {
		return fmt.Errorf("failed to receive RDB file: %w", err)
	}

	m.state.masterLinkState = "up"
	m.logger.Info("Successfully connected to master",
		zap.String("master", fmt.Sprintf("%s:%d",
			m.config.Replication.MasterHost,
			m.config.Replication.MasterPort)),
	)

	// Start processing the command stream
	if err := m.processCommandStream(); err != nil {
		return fmt.Errorf("failed to process command stream: %w", err)
	}

	return nil
}

func (m *Manager) processCommandStream() error {
	m.logger.Info("Starting to process command stream from master")

	for {
		// Set a longer read deadline for each command
		m.state.masterConn.SetReadDeadline(time.Now().Add(time.Second * 30))

		command, err := m.state.masterReader.ReadCommand()
		if err != nil {
			return fmt.Errorf("failed to read command: %w", err)
		}

		m.logger.Debug("Received command from master",
			zap.String("command", command.Name),
			zap.Strings("args", command.Args))

		// Process REPLCONF GETACK
		if command.Name == "REPLCONF" && len(command.Args) > 0 && command.Args[0] == "GETACK" {
			ackCmd := &protocol.Command{ // Note the & here
				Name: "REPLCONF",
				Args: []string{"ACK", strconv.FormatInt(m.state.offset, 10)},
			}
			if err := m.state.masterWriter.WriteArray([][]byte{
				protocol.EncodeBulkString(&ackCmd.Name),
				protocol.EncodeBulkString(&ackCmd.Args[0]),
				protocol.EncodeBulkString(&ackCmd.Args[1]),
			}); err != nil {
				return fmt.Errorf("failed to send ACK: %w", err)
			}
			continue
		}

		// Execute command locally
		if err := m.executeCommand(command); err != nil {
			m.logger.Error("Failed to execute command",
				zap.String("command", command.Name),
				zap.Error(err))
			continue
		}

		// Update offset - fix the encoding issue here
		m.state.offset += int64(len(protocol.EncodeCommand(command)))
	}
}

// executeCommand executes a command received from master locally
func (m *Manager) executeCommand(cmd *protocol.Command) error {
	switch cmd.Name {
	case "SET":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("invalid SET command arguments")
		}
		key, value := cmd.Args[0], cmd.Args[1]
		var px *int

		// Parse PX if present
		for i := 2; i < len(cmd.Args); i++ {
			if strings.ToUpper(cmd.Args[i]) == "PX" {
				if i+1 >= len(cmd.Args) {
					return fmt.Errorf("PX value missing")
				}
				ms, err := strconv.Atoi(cmd.Args[i+1])
				if err != nil {
					return fmt.Errorf("invalid PX value")
				}
				px = &ms
				i++
			}
		}

		return m.store.Set(key, value, px)

	case "DEL":
		if len(cmd.Args) != 1 {
			return fmt.Errorf("invalid DEL command arguments")
		}
		m.store.Delete(cmd.Args[0])
		return nil

	default:
		// Ignore commands we don't need to replicate
		return nil
	}
}

func (m *Manager) handshake() error {
	// Send PING as proper RESP array command
	pingCmd := &protocol.Command{
		Name: "PING",
		Args: []string{},
	}
	if err := m.state.masterWriter.WriteArray([][]byte{
		protocol.EncodeBulkString(&pingCmd.Name),
	}); err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}

	// Set read deadline for responses
	m.state.masterConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer m.state.masterConn.SetReadDeadline(time.Time{})

	// Read PONG response as simple string
	resp, err := m.state.masterReader.ReadSimpleString()
	if err != nil {
		return fmt.Errorf("failed to read PONG: %w", err)
	}
	if resp != "PONG" {
		return fmt.Errorf("expected PONG, got %s", resp)
	}

	// Send REPLCONF listening-port
	portCmd := &protocol.Command{
		Name: "REPLCONF",
		Args: []string{"listening-port", strconv.Itoa(m.config.Port)},
	}
	if err := m.state.masterWriter.WriteArray([][]byte{
		protocol.EncodeBulkString(&portCmd.Name),
		protocol.EncodeBulkString(&portCmd.Args[0]),
		protocol.EncodeBulkString(&portCmd.Args[1]),
	}); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %w", err)
	}

	// Read OK response
	resp, err = m.state.masterReader.ReadSimpleString()
	if err != nil {
		return fmt.Errorf("failed to read listening-port response: %w", err)
	}
	if resp != "OK" {
		return fmt.Errorf("expected OK, got %s", resp)
	}

	// Send REPLCONF capa psync2
	capaCmd := &protocol.Command{
		Name: "REPLCONF",
		Args: []string{"capa", "psync2"},
	}
	if err := m.state.masterWriter.WriteArray([][]byte{
		protocol.EncodeBulkString(&capaCmd.Name),
		protocol.EncodeBulkString(&capaCmd.Args[0]),
		protocol.EncodeBulkString(&capaCmd.Args[1]),
	}); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa: %w", err)
	}

	// Read OK response
	resp, err = m.state.masterReader.ReadSimpleString()
	if err != nil {
		return fmt.Errorf("failed to read capa response: %w", err)
	}
	if resp != "OK" {
		return fmt.Errorf("expected OK, got %s", resp)
	}

	// Send PSYNC
	psyncCmd := &protocol.Command{
		Name: "PSYNC",
		Args: []string{"?", "-1"},
	}
	if err := m.state.masterWriter.WriteArray([][]byte{
		protocol.EncodeBulkString(&psyncCmd.Name),
		protocol.EncodeBulkString(&psyncCmd.Args[0]),
		protocol.EncodeBulkString(&psyncCmd.Args[1]),
	}); err != nil {
		return fmt.Errorf("failed to send PSYNC: %w", err)
	}

	// Read FULLRESYNC response
	resp, err = m.state.masterReader.ReadSimpleString()
	if err != nil {
		return fmt.Errorf("failed to read PSYNC response: %w", err)
	}
	if !strings.HasPrefix(resp, "FULLRESYNC") {
		return fmt.Errorf("expected FULLRESYNC, got %s", resp)
	}

	// Parse replication ID and offset
	parts := strings.Fields(resp)
	if len(parts) == 3 {
		m.state.replid = parts[1]
		offset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset in FULLRESYNC: %w", err)
		}
		m.state.offset = offset
	}

	return nil
}

// waitForDisconnect returns a channel that closes when master connection is lost
func (m *Manager) waitForDisconnect() chan struct{} {
	ch := make(chan struct{})

	go func() {
		defer close(ch)

		buf := make([]byte, 1)
		m.state.masterConn.SetReadDeadline(time.Now().Add(time.Second))

		for {
			_, err := m.state.masterConn.Read(buf)
			if err != nil {
				return
			}
			m.state.masterConn.SetReadDeadline(time.Now().Add(time.Second))
		}
	}()

	return ch
}

// receiveRDB receives and processes RDB file from master
func (m *Manager) receiveRDB() error {
	// Read the RDB file length
	length, err := m.readLength()
	if err != nil {
		return fmt.Errorf("failed to read RDB length: %w", err)
	}

	m.logger.Info("Starting RDB transfer",
		zap.Int64("size", length))

	// Create temporary file
	tempFile := filepath.Join(m.config.Dir, fmt.Sprintf("temp-%d-%s",
		time.Now().UnixNano(), m.config.DBFilename))

	// Ensure directory exists
	if err := os.MkdirAll(m.config.Dir, 0755); err != nil {
		return fmt.Errorf("failed to create RDB directory: %w", err)
	}

	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		file.Close()
		os.Remove(tempFile) // Clean up temp file
	}()

	// Read file in chunks
	bytesReceived := int64(0)
	buffer := make([]byte, 8192) // 8KB chunks
	for bytesReceived < length {
		n := int64(len(buffer))
		if length-bytesReceived < n {
			n = length - bytesReceived
		}

		m.state.masterConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		nr, err := io.ReadFull(m.state.masterReader.Reader(), buffer[:n])
		if err != nil {
			return fmt.Errorf("failed to read RDB chunk: %w", err)
		}

		nw, err := file.Write(buffer[:nr])
		if err != nil {
			return fmt.Errorf("failed to write RDB chunk: %w", err)
		}
		if nw != nr {
			return fmt.Errorf("short write: wrote %d of %d bytes", nw, nr)
		}

		bytesReceived += int64(nr)
		m.logger.Debug("RDB transfer progress",
			zap.Int64("received", bytesReceived),
			zap.Int64("total", length))
	}

	// Sync file to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync RDB file: %w", err)
	}

	// Rename temp file to final RDB file
	finalPath := filepath.Join(m.config.Dir, m.config.DBFilename)
	if err := os.Rename(tempFile, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	m.logger.Info("RDB transfer completed",
		zap.Int64("bytes", bytesReceived))

	// Load the received RDB file
	if err := m.store.LoadRDB(); err != nil {
		return fmt.Errorf("failed to load received RDB: %w", err)
	}

	return nil
}

// readLength reads a RESP bulk string length
func (m *Manager) readLength() (int64, error) {
	m.state.masterConn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read the $ character
	b := make([]byte, 1)
	if _, err := io.ReadFull(m.state.masterReader.Reader(), b); err != nil {
		return 0, fmt.Errorf("failed to read length marker: %w", err)
	}
	if b[0] != '$' {
		return 0, fmt.Errorf("expected $, got %c", b[0])
	}

	// Read the length number until \r\n
	line, err := m.state.masterReader.Reader().ReadBytes('\n')
	if err != nil {
		return 0, fmt.Errorf("failed to read length: %w", err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return 0, fmt.Errorf("invalid length format")
	}

	// Parse the length
	length, err := strconv.ParseInt(string(line[:len(line)-2]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid length value: %w", err)
	}
	if length < 0 {
		return 0, fmt.Errorf("negative length")
	}

	return length, nil
}
