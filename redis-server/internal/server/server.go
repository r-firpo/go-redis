// internal/server/server.go

package server

import (
	"context"
	"fmt"
	"go-redis/redis-server/internal/config"
	"go-redis/redis-server/internal/replication"
	"go-redis/redis-server/internal/store"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go-redis/redis-server/internal/protocol"
)

// Server represents the Redis server instance
type Server struct {
	listener      net.Listener
	addr          string
	connManager   *ConnectionManager
	store         *store.DataStore
	logger        *zap.Logger
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	config        *config.Config
	replManager   *replication.Manager
	masterHandler *replication.MasterHandler
}

// NewServer creates a new Redis server instance
func NewServer(cfg *config.Config, logger *zap.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	store := store.NewDataStore(logger, cfg.Dir, cfg.DBFilename)

	srv := &Server{
		addr:          fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		connManager:   NewConnectionManager(logger),
		store:         store,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		config:        cfg,
		replManager:   replication.NewManager(cfg, store, logger),
		masterHandler: replication.NewMasterHandler(cfg, store, logger),
	}

	return srv
}

// Start begins accepting connections
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.logger.Info("Server started",
		zap.String("addr", s.addr),
		zap.String("role", string(s.config.Role)),
	)

	// Start replication if we're a slave
	if s.config.Role == config.RoleSlave {
		if err := s.replManager.Start(); err != nil {
			return fmt.Errorf("failed to start replication: %w", err)
		}
		s.logger.Info("Started replication",
			zap.String("master", fmt.Sprintf("%s:%d",
				s.config.Replication.MasterHost,
				s.config.Replication.MasterPort)),
		)
	}
	// Start connection monitoring
	s.wg.Add(1)
	go s.monitorConnections()

	// Accept connections
	s.wg.Add(1)
	go s.acceptConnections()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() error {
	s.cancel()

	// Stop replication manager first
	if s.replManager != nil {
		if err := s.replManager.Stop(); err != nil {
			s.logger.Error("Failed to stop replication manager", zap.Error(err))
		}
	}

	// Stop the store
	if err := s.store.Stop(); err != nil {
		s.logger.Error("Failed to stop data store", zap.Error(err))
	}

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Failed to close listener", zap.Error(err))
		}
	}

	s.wg.Wait()
	s.logger.Info("Server stopped")
	return nil
}

// acceptConnections handles incoming connections
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Error("Failed to accept connection", zap.Error(err))
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection processes client connections
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.connManager.RemoveConnection(conn)
		// If we're a master, clean up any replica connection
		if s.config.Role == config.RoleMaster && s.masterHandler != nil {
			s.masterHandler.RemoveReplica(conn)
		}
		s.wg.Done()
	}()

	s.connManager.AddConnection(conn)
	reader := protocol.NewRESPReader(conn)
	writer := protocol.NewRESPWriter(conn)

	// Set a reasonable read timeout for all connections
	conn.SetReadDeadline(time.Now().Add(time.Second * 30))

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			cmd, err := reader.ReadCommand()
			if err != nil {
				s.logger.Error("Failed to read command", zap.Error(err))
				return
			}

			// Reset deadline after successful read
			conn.SetReadDeadline(time.Now().Add(time.Second * 30))

			if err := s.handleCommand(cmd, writer); err != nil {
				s.logger.Error("Failed to handle command",
					zap.String("command", cmd.String()),
					zap.Error(err),
				)
				return
			}
		}
	}
}

// handleCommand processes Redis commands
func (s *Server) handleCommand(cmd *protocol.Command, writer *protocol.RESPWriter) error {
	s.logger.Debug("Processing command",
		zap.String("command", cmd.Name),
		zap.Strings("args", cmd.Args),
	)

	switch cmd.Name {
	case "REPLCONF":
		if len(cmd.Args) < 1 {
			return writer.WriteError("wrong number of arguments for REPLCONF")
		}
		return s.handleReplConf(cmd.Args, writer)

	case "PSYNC":
		if len(cmd.Args) != 2 {
			return writer.WriteError("wrong number of arguments for PSYNC")
		}
		return s.handlePSync(cmd.Args, writer)

	case "INFO":
		if len(cmd.Args) > 1 {
			return writer.WriteError("wrong number of arguments for INFO")
		}
		section := ""
		if len(cmd.Args) == 1 {
			section = strings.ToLower(cmd.Args[0])
		}
		return s.handleInfo(section, writer)

	case "PING":
		if len(cmd.Args) > 1 {
			return writer.WriteError("wrong number of arguments for PING command")
		}
		if len(cmd.Args) == 1 {
			return writer.WriteBulkString(&cmd.Args[0])
		}
		return writer.WriteSimpleString("PONG")

	case "SET":
		if len(cmd.Args) < 2 {
			return writer.WriteError("wrong number of arguments for SET command")
		}

		key, value := cmd.Args[0], cmd.Args[1]
		var px *int

		// Parse optional PX argument
		for i := 2; i < len(cmd.Args); i++ {
			if strings.ToUpper(cmd.Args[i]) == "PX" {
				if i+1 >= len(cmd.Args) {
					return writer.WriteError("value is required for PX option")
				}
				milliseconds, err := strconv.Atoi(cmd.Args[i+1])
				if err != nil {
					return writer.WriteError("value is not an integer or out of range")
				}
				if milliseconds <= 0 {
					return writer.WriteError("PX value must be positive")
				}
				if milliseconds > 999999999999999 {
					return writer.WriteError("PX value is too large")
				}
				px = &milliseconds
				i++ // Skip the PX value in next iteration
			} else {
				return writer.WriteError("syntax error")
			}
		}

		// If we're master, propagate before executing
		if s.config.Role == config.RoleMaster {
			cmdBytes := protocol.EncodeCommand(cmd)
			s.masterHandler.PropagateCommand(cmdBytes)
		}

		if err := s.store.Set(key, value, px); err != nil {
			return writer.WriteError("operation failed")
		}
		return writer.WriteSimpleString("OK")

	case "GET":
		if len(cmd.Args) != 1 {
			return writer.WriteError("wrong number of arguments for GET command")
		}

		value, exists := s.store.Get(cmd.Args[0])
		if !exists {
			return writer.WriteBulkString(nil)
		}
		return writer.WriteBulkString(&value)

	case "DEL":
		if len(cmd.Args) != 1 {
			return writer.WriteError("wrong number of arguments for DEL command")
		}

		// If we're master, propagate before executing
		if s.config.Role == config.RoleMaster {
			cmdBytes := protocol.EncodeCommand(cmd)
			s.masterHandler.PropagateCommand(cmdBytes)
		}

		success := s.store.Delete(cmd.Args[0])
		return writer.WriteInteger(map[bool]int{true: 1, false: 0}[success])

	case "KEYS":
		if len(cmd.Args) != 1 {
			return writer.WriteError("wrong number of arguments for KEYS command")
		}
		if cmd.Args[0] != "*" {
			return writer.WriteError("only * pattern is supported")
		}

		keys := s.store.Keys()
		keyBytes := make([][]byte, len(keys))
		for i, key := range keys {
			keyStr := key // Create a new variable to get a new address
			keyBytes[i] = protocol.EncodeBulkString(&keyStr)
		}
		return writer.WriteArray(keyBytes)

	case "CONFIG":
		if len(cmd.Args) < 2 {
			return writer.WriteError("wrong number of arguments for CONFIG command")
		}
		if strings.ToUpper(cmd.Args[0]) == "GET" {
			param := strings.ToLower(cmd.Args[1])
			var configValue string
			switch param {
			case "dir":
				configValue = s.config.Dir
			case "dbfilename":
				configValue = s.config.DBFilename
			default:
				return writer.WriteBulkString(nil)
			}
			return writer.WriteArray([][]byte{
				protocol.EncodeBulkString(&param),
				protocol.EncodeBulkString(&configValue),
			})
		}
		return writer.WriteError("unknown CONFIG subcommand " + cmd.Args[0])

	case "COMMAND":
		// Basic implementation to support redis-cli
		if len(cmd.Args) > 0 && cmd.Args[0] == "DOCS" {
			return writer.WriteArray(nil) // Empty array response
		}
		return writer.WriteArray(nil) // Empty array response

	default:
		return writer.WriteError("unknown command '" + cmd.Name + "'")
	}
}

// handleReplConf handles REPLCONF command
func (s *Server) handleReplConf(args []string, writer *protocol.RESPWriter) error {

	if s.config.Role != config.RoleMaster {
		return writer.WriteError("This instance is not a master")
	}

	subCmd := strings.ToLower(args[0])
	switch subCmd {
	case "listening-port":
		if len(args) != 2 {
			return writer.WriteError("wrong number of arguments for REPLCONF listening-port")
		}
		return writer.WriteSimpleString("OK")

	case "capa":
		if len(args) != 2 {
			return writer.WriteError("wrong number of arguments for REPLCONF capa")
		}
		return writer.WriteSimpleString("OK")

	case "ack":
		if len(args) != 2 {
			return writer.WriteError("wrong number of arguments for REPLCONF ACK")
		}
		addr := writer.RemoteAddr().String()
		if err := s.masterHandler.ProcessACK(addr, args[1]); err != nil {
			return writer.WriteError(fmt.Sprintf("invalid ACK: %v", err))
		}
		return writer.WriteSimpleString("OK")

	default:
		return writer.WriteError(fmt.Sprintf("unknown REPLCONF subcommand %s", subCmd))
	}
}

// Update handlePSync
func (s *Server) handlePSync(args []string, writer *protocol.RESPWriter) error {
	if s.config.Role != config.RoleMaster {
		return writer.WriteError("This instance is not a master")
	}

	// Register new replica
	if err := s.masterHandler.AddReplica(writer.Conn(), writer); err != nil {
		return writer.WriteError(fmt.Sprintf("failed to add replica: %v", err))
	}

	// Send FULLRESYNC response
	response := fmt.Sprintf("FULLRESYNC %s 0", s.config.Replication.MasterReplID)
	if err := writer.WriteSimpleString(response); err != nil {
		return err
	}

	// Send RDB file
	if err := s.masterHandler.SendRDBToReplica(writer); err != nil {
		s.masterHandler.RemoveReplica(writer.Conn())
		return fmt.Errorf("failed to send RDB: %w", err)
	}

	return nil
}

// Helper function to identify write commands
func isWriteCommand(cmd string) bool {
	writeCommands := map[string]bool{
		"SET":    true,
		"DEL":    true,
		"EXPIRE": true,
		// Add other write commands as needed
	}
	return writeCommands[cmd]
}

// handleInfo handles INFO command
func (s *Server) handleInfo(section string, writer *protocol.RESPWriter) error {
	if section == "" || section == "replication" {
		info := []string{
			"# Replication",
			fmt.Sprintf("role:%s", s.config.Role),
		}

		if s.config.Role == config.RoleMaster {
			info = append(info,
				fmt.Sprintf("connected_slaves:%d", s.config.Replication.ConnectedSlaves),
				fmt.Sprintf("master_replid:%s", s.config.Replication.MasterReplID),
				fmt.Sprintf("master_repl_offset:%d", s.config.Replication.MasterOffset),
			)
		} else {
			info = append(info,
				fmt.Sprintf("master_host:%s", s.config.Replication.MasterHost),
				fmt.Sprintf("master_port:%d", s.config.Replication.MasterPort),
				fmt.Sprintf("master_link_status:%s", "down"), // TODO: Update based on actual status
				"master_last_io_seconds_ago:-1",
				"master_sync_in_progress:0",
				"slave_repl_offset:0",
				"slave_priority:100",
				fmt.Sprintf("slave_read_only:%d", map[bool]int{true: 1, false: 0}[s.config.Replication.SlaveReadOnly]),
			)
		}

		return writer.WriteBulkString(stringPtr(strings.Join(info, "\r\n")))
	}

	return writer.WriteError(fmt.Sprintf("Invalid section name %s", section))
}

// Helper function for string pointers
func stringPtr(s string) *string {
	return &s
}

// monitorConnections periodically logs connection information
func (s *Server) monitorConnections() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C: // Using the ticker's channel directly
			info := s.connManager.GetConnectionInfo()
			s.logger.Info("Connection status", zap.String("info", info))
		}
	}
}
