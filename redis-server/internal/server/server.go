// internal/server/server.go

package server

import (
	"context"
	"fmt"
	"go-redis/redis-server/internal/config"
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
	listener    net.Listener
	addr        string
	connManager *ConnectionManager
	store       *store.DataStore
	logger      *zap.Logger
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	config      *config.Config
}

// NewServer creates a new Redis server instance
func NewServer(cfg *config.Config, logger *zap.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:        fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		connManager: NewConnectionManager(logger),
		store:       store.NewDataStore(logger, cfg.Dir, cfg.DBFilename),
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		config:      cfg,
	}
}

// Start begins accepting connections
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	s.logger.Info("Server started", zap.String("addr", s.addr))

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

	// Stop the store first to ensure data is saved
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
		s.wg.Done()
	}()

	s.connManager.AddConnection(conn)

	reader := protocol.NewRESPReader(conn)
	writer := protocol.NewRESPWriter(conn)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// Set read deadline to prevent hanging
			conn.SetReadDeadline(time.Now().Add(time.Second * 5))

			cmd, err := reader.ReadCommand()
			if err != nil {
				s.logger.Error("Failed to read command", zap.Error(err))
				return
			}

			// Process the command (we'll implement this next)
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
			bulk := protocol.EncodeBulkString(&keyStr)
			keyBytes[i] = bulk
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

	default:
		return writer.WriteError("unknown command '" + cmd.Name + "'")
	}
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
