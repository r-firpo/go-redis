// internal/protocol/helpers.go

package protocol

import "fmt"

// EncodeBulkString encodes a string as a RESP bulk string
func EncodeBulkString(s *string) []byte {
	if s == nil {
		return []byte("$-1\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(*s), *s))
}

// EncodeCommand encodes a Command into RESP protocol format
func EncodeCommand(cmd *Command) []byte {
	// Calculate total number of elements (command name + args)
	numElements := 1 + len(cmd.Args)

	// Start with array header
	result := []byte(fmt.Sprintf("*%d\r\n", numElements))

	// Add command name as bulk string
	result = append(result, EncodeBulkString(&cmd.Name)...)

	// Add each argument as bulk string
	for _, arg := range cmd.Args {
		argCopy := arg // Create copy to get stable address
		result = append(result, EncodeBulkString(&argCopy)...)
	}

	return result
}
