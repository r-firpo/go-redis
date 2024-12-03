# Go Redis Server Implementation

A lightweight Redis server implementation in Go that supports basic Redis commands and replication. This implementation follows the Redis Serialization Protocol (RESP) and includes support for RDB persistence (version 11).

## Features

- **RESP Protocol Support**: Fully compliant with the Redis Serialization Protocol
- **RDB Persistence**: Compatible with RDB version 11
    - Periodic automatic saves
    - Forced saves for replication
    - Configurable save directory and filename
- **Primary-Replica Replication**
    - Support for primary/replica roles
    - PSYNC implementation
    - Replication backlog
    - WAIT command for synchronous replication
- **Connection Management**
    - Automatic connection tracking
    - Periodic connection monitoring
    - Configurable connection limits and timeouts
- **Supported Commands**
    - Basic Operations: `SET`, `GET`, `DEL`, `KEYS`
    - Server Information: `INFO`, `PING`, `CONFIG GET`
    - Replication: `REPLCONF`, `PSYNC`, `WAIT`
    - Key Expiration: Support for `PX` option in `SET` command

## Prerequisites

- Go 1.21 or higher
- Redis CLI (for testing)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/go-redis
cd go-redis
```

2. Build the server:
```bash
go build -o redis-server cmd/server/main.go
```

## Running the Server

### Basic Usage

Start the server with default settings:
```bash
./redis-server
```

### Configuration Options

The server supports various configuration flags:

```bash
./redis-server \
  --host localhost \
  --port 6379 \
  --dir /tmp/redis-files-rdb \
  --dbfilename dump.rdb \
  --loglevel info \
  --logformat console
```

Available flags:
- `--host`: Server host (default: localhost)
- `--port`: Server port (default: 6379)
- `--dir`: RDB directory path
- `--dbfilename`: RDB filename
- `--loglevel`: Log level (debug, info, warn, error)
- `--logformat`: Log format (console, json)
- `--logfile`: Log file path (empty for stdout)
- `--replicaof`: Primary host and port for replica mode (e.g., "localhost 6379")

## Testing with redis-cli

1. Start the redis-cli:
```bash
redis-cli -p 6379
```

2. Basic operations:
```redis
SET mykey "Hello, World!"
GET mykey
SET mykey "Expiring Value" PX 5000  # Expires in 5 seconds
DEL mykey
KEYS *
```

3. Check server info:
```redis
INFO replication
PING
CONFIG GET dir
```

## Setting Up Replication

1. Start the primary server:
```bash
./redis-server --port 6379
```

2. Start one or more replica servers:
```bash
./redis-server --port 6380 --replicaof localhost 6379
```

3. Test replication:
```bash
# Connect to primary
redis-cli -p 6379
SET mykey "test"
WAIT 1 0  # Wait for 1 replica to acknowledge

# Connect to replica
redis-cli -p 6380
GET mykey  # Should return "test"
```

## Implementation Details

### RDB Persistence
- Uses RDB version 11 format
- Periodic automatic saves (every 60 seconds)
- Supports key expiration
- Handles both save and load operations

### Replication Features
- Full synchronization with RDB transfer
- Command propagation from primary to replicas
- Replication offset tracking
- Backlog management for partial resync
- Replica acknowledgment handling

### Connection Management
- Tracks active connections
- Monitors connection statistics
- Implements connection timeouts
- Handles graceful disconnection

## TODOs and Future Enhancements

### Data Types and Commands
- [ ] Add support for Lists, Sets, and Sorted Sets
- [ ] Implement Hash data type operations
- [ ] Add support for Pub/Sub functionality
- [ ] Add scanning commands (SCAN, HSCAN, SSCAN, ZSCAN)
- [ ] Implement transactions (MULTI, EXEC, WATCH)

### Persistence and Durability
- [ ] Add AOF (Append-Only File) persistence
- [ ] Implement RESP3 protocol features
- [ ] Add support for RDB compression
- [ ] Implement point-in-time backups

### Performance and Scaling
- [ ] Add Redis Cluster support
- [ ] Implement connection pooling
- [ ] Add memory usage optimization
- [ ] Implement Redis Sentinel support
- [ ] Add support for Lua scripting

### Monitoring and Management
- [ ] Add prometheus metrics endpoint
- [ ] Implement slow log
- [ ] Add enhanced monitoring capabilities
- [ ] Implement memory analysis tools
- [ ] Add support for Redis ACLs

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## License

Apache License