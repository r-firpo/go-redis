// internal/protocol/resp.go

package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RESPReader handles reading and parsing RESP protocol data
type RESPReader struct {
	reader *bufio.Reader
	// offset tracks the current reading position for error reporting
	offset int64
}

// NewRESPReader creates a new RESPReader
func NewRESPReader(reader io.Reader) *RESPReader {
	return &RESPReader{
		reader: bufio.NewReader(reader),
		offset: 0,
	}
}

// ReadCommand reads and parses a complete RESP command
func (r *RESPReader) ReadCommand() (*Command, error) {
	// Read the first byte to determine the type
	typ, err := r.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("error reading command type: %w", err)
	}
	r.offset++

	// We expect commands to be arrays
	if RESPType(typ) != Array {
		return nil, NewParseError("expected array", r.offset-1)
	}

	// Read array length
	length, err := r.readInteger()
	if err != nil {
		return nil, fmt.Errorf("error reading array length: %w", err)
	}

	if length < 1 {
		return nil, NewParseError("invalid array length", r.offset)
	}

	// Read array elements
	elements := make([]string, length)
	for i := 0; i < length; i++ {
		// Each element should be a bulk string
		element, err := r.readBulkString()
		if err != nil {
			return nil, fmt.Errorf("error reading array element %d: %w", i, err)
		}
		elements[i] = element
	}

	// First element is the command name, rest are arguments
	return &Command{
		Name: strings.ToUpper(elements[0]),
		Args: elements[1:],
	}, nil
}

// readInteger reads a RESP integer (including trailing CRLF)
func (r *RESPReader) readInteger() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	n, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, NewParseError("invalid integer", r.offset)
	}

	return n, nil
}

// readBulkString reads a RESP bulk string
func (r *RESPReader) readBulkString() (string, error) {
	// Read the type byte
	typ, err := r.reader.ReadByte()
	if err != nil {
		return "", fmt.Errorf("error reading bulk string type: %w", err)
	}
	r.offset++

	if RESPType(typ) != BulkString {
		return "", NewParseError("expected bulk string", r.offset-1)
	}

	// Read string length
	length, err := r.readInteger()
	if err != nil {
		return "", fmt.Errorf("error reading bulk string length: %w", err)
	}

	if length < 0 {
		return "", nil // Null bulk string
	}

	// Read string content
	buf := make([]byte, length)
	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return "", fmt.Errorf("error reading bulk string content: %w", err)
	}
	r.offset += int64(length)

	// Read trailing CRLF
	if err := r.readCRLF(); err != nil {
		return "", err
	}

	return string(buf), nil
}

// readLine reads until CRLF and returns the line without CRLF
func (r *RESPReader) readLine() ([]byte, error) {
	line, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading line: %w", err)
	}
	r.offset += int64(len(line))

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, NewParseError("invalid protocol", r.offset)
	}

	return line[:len(line)-2], nil
}

// readCRLF reads and verifies a CRLF sequence
func (r *RESPReader) readCRLF() error {
	cr, err := r.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading CR: %w", err)
	}
	r.offset++

	if cr != '\r' {
		return NewParseError("expected CR", r.offset-1)
	}

	lf, err := r.reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading LF: %w", err)
	}
	r.offset++

	if lf != '\n' {
		return NewParseError("expected LF", r.offset-1)
	}

	return nil
}

// RESPWriter handles writing RESP protocol responses
type RESPWriter struct {
	writer *bufio.Writer
}

// NewRESPWriter creates a new RESPWriter
func NewRESPWriter(writer io.Writer) *RESPWriter {
	return &RESPWriter{
		writer: bufio.NewWriter(writer),
	}
}

// WriteSimpleString writes a RESP simple string
func (w *RESPWriter) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.writer, "+%s\r\n", s)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

// WriteError writes a RESP error
func (w *RESPWriter) WriteError(errStr string) error {
	_, err := fmt.Fprintf(w.writer, "-ERR %s\r\n", errStr)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

// WriteInteger writes a RESP integer
func (w *RESPWriter) WriteInteger(i int) error {
	_, err := fmt.Fprintf(w.writer, ":%d\r\n", i)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

// WriteBulkString writes a RESP bulk string
func (w *RESPWriter) WriteBulkString(s *string) error {
	if s == nil {
		_, err := w.writer.WriteString("$-1\r\n")
		if err != nil {
			return err
		}
		return w.writer.Flush()
	}

	_, err := fmt.Fprintf(w.writer, "$%d\r\n%s\r\n", len(*s), *s)
	if err != nil {
		return err
	}
	return w.writer.Flush()
}

// WriteArray writes a RESP array
func (w *RESPWriter) WriteArray(items [][]byte) error {
	if items == nil {
		_, err := w.writer.WriteString("*-1\r\n")
		if err != nil {
			return err
		}
		return w.writer.Flush()
	}

	_, err := fmt.Fprintf(w.writer, "*%d\r\n", len(items))
	if err != nil {
		return err
	}

	for _, item := range items {
		_, err = w.writer.Write(item)
		if err != nil {
			return err
		}
	}

	return w.writer.Flush()
}
