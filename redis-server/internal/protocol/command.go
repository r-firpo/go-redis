// internal/protocol/command.go

package protocol

import "fmt"

// RESPType represents different RESP protocol data types
type RESPType byte

const (
	// RESP protocol type constants
	SimpleString RESPType = '+'
	Error        RESPType = '-'
	Integer      RESPType = ':'
	BulkString   RESPType = '$'
	Array        RESPType = '*'
)

// Command represents a parsed Redis command with its arguments
type Command struct {
	// Name is the command name in uppercase (e.g., "GET", "SET")
	Name string
	// Args contains the command arguments
	Args []string
}

// String returns a string representation of the Command for logging
func (c *Command) String() string {
	return fmt.Sprintf("%s %v", c.Name, c.Args)
}

// ParseError represents an error that occurred during RESP parsing
type ParseError struct {
	Message string
	Offset  int64
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at offset %d: %s", e.Offset, e.Message)
}

// NewParseError creates a new ParseError with the given message and offset
func NewParseError(msg string, offset int64) error {
	return &ParseError{
		Message: msg,
		Offset:  offset,
	}
}
