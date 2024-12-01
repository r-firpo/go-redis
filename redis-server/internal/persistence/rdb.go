// internal/persistence/rdb.go

package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// RDB format constants matching Python implementation
const (
	RedisRDBVersion = 11

	// RDB Opcodes
	OpEOF          = 255 // 0xFF
	OpSelectDB     = 254 // 0xFE
	OpExpireTimeMS = 252 // 0xFC
	OpAux          = 250 // 0xFA

	// Value Types
	TypeString        = 0
	TypeStringEncoded = 251 // 0xFB
)

// Item represents a value with optional expiration
type Item struct {
	Value    string
	ExpireAt *int64 // Expiration time in milliseconds, nil if no expiration
}

// RDBHandler handles RDB file persistence
type RDBHandler struct {
	dir      string
	filename string
	logger   *zap.Logger
}

// NewRDBHandler creates a new RDB handler
func NewRDBHandler(dir, filename string, logger *zap.Logger) *RDBHandler {
	return &RDBHandler{
		dir:      dir,
		filename: filename,
		logger:   logger,
	}
}

func (h *RDBHandler) getTempFilePath() string {
	return filepath.Join(h.dir, fmt.Sprintf("temp-%d-%s", time.Now().UnixNano(), h.filename))
}

// Save saves the current dataset to RDB file
func (h *RDBHandler) Save(data map[string]Item) error {

	h.logger.Debug("Starting RDB save",
		zap.String("dir", h.dir),
		zap.String("filename", h.filename),
	)

	// Ensure directory exists
	if err := os.MkdirAll(h.dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create temp file with a unique name
	tempFile := h.getTempFilePath()
	h.logger.Debug("Creating temp file", zap.String("path", tempFile))

	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		// Only try to close if we haven't already explicitly closed it
		if file != nil {
			file.Close()
			// Clean up temp file if something went wrong
			if _, err := os.Stat(tempFile); err == nil {
				os.Remove(tempFile)
			}
		}
	}()

	writer := bufio.NewWriter(file)

	// Write header
	if err := h.writeHeader(writer); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write auxiliary fields
	if err := h.writeAuxField(writer, "redis-ver", "7.2.0"); err != nil {
		return fmt.Errorf("failed to write redis-ver: %w", err)
	}
	if err := h.writeAuxField(writer, "redis-bits", "64"); err != nil {
		return fmt.Errorf("failed to write redis-bits: %w", err)
	}

	// Write database selector
	if err := writer.WriteByte(OpSelectDB); err != nil {
		return fmt.Errorf("failed to write db selector: %w", err)
	}
	if err := writer.WriteByte(0); err != nil { // DB 0
		return fmt.Errorf("failed to write db number: %w", err)
	}

	// Write string section marker and counts
	if err := writer.WriteByte(TypeStringEncoded); err != nil {
		return fmt.Errorf("failed to write string section marker: %w", err)
	}

	// Count expired keys
	expiredCount := 0
	for _, item := range data {
		if item.ExpireAt != nil {
			expiredCount++
		}
	}

	// Write counts
	if err := writer.WriteByte(byte(len(data))); err != nil {
		return fmt.Errorf("failed to write data count: %w", err)
	}
	if err := writer.WriteByte(byte(expiredCount)); err != nil {
		return fmt.Errorf("failed to write expired count: %w", err)
	}

	// Write key-value pairs
	for key, item := range data {
		// Write expiration if exists
		if item.ExpireAt != nil {
			if err := writer.WriteByte(OpExpireTimeMS); err != nil {
				return fmt.Errorf("failed to write expire marker: %w", err)
			}
			if err := binary.Write(writer, binary.LittleEndian, *item.ExpireAt); err != nil {
				return fmt.Errorf("failed to write expire time: %w", err)
			}
		}

		// Write type
		if err := writer.WriteByte(TypeString); err != nil {
			return fmt.Errorf("failed to write value type: %w", err)
		}

		// Write key
		if err := h.writeLengthEncodedString(writer, key); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}

		// Write value
		if err := h.writeLengthEncodedString(writer, item.Value); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}

	// Write EOF marker
	if err := writer.WriteByte(OpEOF); err != nil {
		return fmt.Errorf("failed to write EOF marker: %w", err)
	}

	// TODO: Add checksum validation

	// Flush and sync
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	h.logger.Debug("Attempting to rename temp file",
		zap.String("from", tempFile),
		zap.String("to", filepath.Join(h.dir, h.filename)),
	)

	// Explicitly close before rename
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	finalPath := filepath.Join(h.dir, h.filename)
	h.logger.Debug("Attempting to rename temp file",
		zap.String("from", tempFile),
		zap.String("to", finalPath),
	)
	// On macOS, try removing the target file first
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing RDB file: %w", err)
	}

	if err := os.Rename(tempFile, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	h.logger.Debug("File rename completed")

	h.logger.Info("RDB save completed",
		zap.Int("keys", len(data)),
		zap.Int("expired_keys", expiredCount),
		zap.String("path", finalPath),
	)

	return nil
}

// Load loads data from RDB file
func (h *RDBHandler) Load() (map[string]Item, error) {
	path := filepath.Join(h.dir, h.filename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]Item), nil
		}
		return nil, fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	data := make(map[string]Item)

	// Verify header
	if err := h.verifyHeader(reader); err != nil {
		return nil, fmt.Errorf("invalid RDB header: %w", err)
	}

	for {
		// Read type byte
		typeByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read type byte: %w", err)
		}

		switch typeByte {
		case OpEOF:
			// TODO: Add checksum validation
			return data, nil

		case OpSelectDB:
			if _, err := h.readLengthEncoded(reader); err != nil {
				return nil, fmt.Errorf("failed to read db number: %w", err)
			}

		case OpAux:
			// Skip auxiliary fields
			if _, err := h.readLengthEncodedString(reader); err != nil {
				return nil, err
			}
			if _, err := h.readLengthEncodedString(reader); err != nil {
				return nil, err
			}

		case TypeStringEncoded:
			// Read counts
			count, err := reader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read count: %w", err)
			}

			expireCount, err := reader.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read expire count: %w", err)
			}

			h.logger.Debug("Reading string section",
				zap.Int("count", int(count)),
				zap.Int("expire_count", int(expireCount)),
			)

			for i := 0; i < int(count); i++ {
				var expireAt *int64

				// Check for expiration marker
				nextByte, err := reader.ReadByte()
				if err != nil {
					return nil, fmt.Errorf("failed to read next byte: %w", err)
				}

				if nextByte == OpExpireTimeMS {
					var expireTime int64
					if err := binary.Read(reader, binary.LittleEndian, &expireTime); err != nil {
						return nil, fmt.Errorf("failed to read expire time: %w", err)
					}
					expireAt = &expireTime

					// Read value type
					if valType, err := reader.ReadByte(); err != nil {
						return nil, fmt.Errorf("failed to read value type: %w", err)
					} else if valType != TypeString {
						return nil, fmt.Errorf("unexpected value type: %d", valType)
					}
				} else if nextByte != TypeString {
					return nil, fmt.Errorf("unexpected value type: %d", nextByte)
				}

				// Read key
				key, err := h.readLengthEncodedString(reader)
				if err != nil {
					return nil, fmt.Errorf("failed to read key: %w", err)
				}

				// Read value
				value, err := h.readLengthEncodedString(reader)
				if err != nil {
					return nil, fmt.Errorf("failed to read value: %w", err)
				}

				// Skip expired keys
				if expireAt != nil {
					if time.Now().UnixMilli() >= *expireAt {
						continue
					}
				}

				data[key] = Item{
					Value:    value,
					ExpireAt: expireAt,
				}
			}

		default:
			return nil, fmt.Errorf("unexpected type byte: %d", typeByte)
		}
	}

	return data, nil
}

// Helper functions

func (h *RDBHandler) writeHeader(w *bufio.Writer) error {
	if _, err := w.WriteString("REDIS"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%04d", RedisRDBVersion); err != nil {
		return err
	}
	return nil
}

func (h *RDBHandler) verifyHeader(r *bufio.Reader) error {
	magic := make([]byte, 5)
	if _, err := io.ReadFull(r, magic); err != nil {
		return err
	}
	if string(magic) != "REDIS" {
		return fmt.Errorf("invalid magic string: %q", string(magic))
	}

	version := make([]byte, 4)
	if _, err := io.ReadFull(r, version); err != nil {
		return err
	}
	// Verify version is <= our version
	if ver := btoi(version); ver > RedisRDBVersion {
		return fmt.Errorf("unsupported RDB version: %d", ver)
	}

	return nil
}

func (h *RDBHandler) writeAuxField(w *bufio.Writer, key, value string) error {
	if err := w.WriteByte(OpAux); err != nil {
		return err
	}
	if err := h.writeLengthEncodedString(w, key); err != nil {
		return err
	}
	return h.writeLengthEncodedString(w, value)
}

func (h *RDBHandler) writeLengthEncodedString(w *bufio.Writer, s string) error {
	length := len(s)
	if length < 64 {
		if err := w.WriteByte(byte(length)); err != nil {
			return err
		}
	} else if length < 16384 {
		if err := binary.Write(w, binary.BigEndian, uint16(length|0x4000)); err != nil {
			return err
		}
	} else {
		if err := w.WriteByte(0x80); err != nil {
			return err
		}
		// Fix: w.Write returns (n int, err error)
		bytes := []byte{
			byte(length >> 16),
			byte(length >> 8),
			byte(length),
		}
		if _, err := w.Write(bytes); err != nil {
			return err
		}
	}

	// WriteString also returns (n int, err error)
	_, err := w.WriteString(s)
	return err
}

func (h *RDBHandler) readLengthEncodedString(r *bufio.Reader) (string, error) {
	length, err := h.readLengthEncoded(r)
	if err != nil {
		return "", err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func (h *RDBHandler) readLengthEncoded(r *bufio.Reader) (int, error) {
	firstByte, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	if (firstByte & 0xC0) == 0 {
		return int(firstByte & 0x3F), nil
	} else if (firstByte & 0xC0) == 0x40 {
		nextByte, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return ((int(firstByte) & 0x3F) << 8) | int(nextByte), nil
	} else if (firstByte & 0xC0) == 0x80 {
		buf := make([]byte, 3)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return (int(buf[0]) << 16) | (int(buf[1]) << 8) | int(buf[2]), nil
	}

	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint32(buf)), nil
}

// btoi converts byte slice to integer
func btoi(b []byte) int {
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
