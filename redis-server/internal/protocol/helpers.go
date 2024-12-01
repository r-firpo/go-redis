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
