## FifoDB [![Build Status](https://travis-ci.org/valinurovam/fifodb.svg?branch=main)](https://travis-ci.org/valinurovam/fifodb) [![Coverage Status](https://coveralls.io/repos/github/valinurovam/fifodb/badge.svg)](https://coveralls.io/github/valinurovam/fifodb) [![Go Report Card](https://goreportcard.com/badge/github.com/valinurovam/fifodb)](https://goreportcard.com/report/github.com/valinurovam/fifodb)

`fifodb` is an embedded, disk-based FIFO queue implemented in Go.  
It is designed for applications that require strict message ordering, crash resilience, and minimal external dependencies, without the overhead of a full message broker.

Unlike Kafka or RabbitMQ, `fifodb` is a **library**, not a service. It runs in your process and persists data to local disk, similar in spirit to BoltDB or Badger—but optimized for FIFO semantics.

## Guarantees

`fifodb` provides the following correctness and durability guarantees:

- **At-least-once delivery**:  
  Messages are not lost if `Ack` is successfully called. If the process crashes before acknowledgment, the message remains in the queue and will be redelivered on restart.

- **Crash safety**:  
  The database recovers correctly after an unclean shutdown (e.g., `kill -9`). Truncated or partially written messages at the end of a segment are safely ignored during recovery.

- **Durability**:  
  All `Ack` operations are written to a Write-Ahead Log (WAL). Applications may call `DB.Sync()` to ensure pending writes are flushed to stable storage.

- **Thread safety**:  
  All public methods (`Push`, `Pop`, `Ack`, `Sync`) are safe to call concurrently from multiple goroutines.

## Architectural Overview

- **Append-only segments**:  
  Data is stored in fixed-size segment files. New messages are appended to the active segment, enabling efficient sequential I/O.

- **Write-Ahead Log (WAL)**:  
  Every logical operation (`Push`, `Ack`) is recorded in a separate WAL file. This ensures atomicity and enables crash recovery.

- **In-memory acknowledgment tracking**:  
  Acknowledgment state is maintained via a compact bit array, minimizing memory overhead.

- **Automatic compaction**:  
  On open, `fifodb` rewrites segments, excluding acknowledged messages, to reclaim disk space.

- **Data integrity**:  
  Each message is protected by a CRC32-C (Castagnoli) checksum. Corrupted records are skipped during reads and compaction.

- **Zero dependencies**:  
  Built exclusively with the Go standard library.

## Use Cases

`fifodb` is well-suited for:

- Embedded systems (IoT, edge devices) requiring local queuing,
- Applications that prefer to avoid external dependencies like Redis or Kafka,
- Scenarios where predictable resource usage and simplicity are valued over distributed features,
- Developers seeking a transparent, auditable FIFO implementation.

It is **not intended** for:
- Distributed or replicated messaging,
- Multi-tenant architectures,
- High-throughput cluster workloads (e.g., millions of messages per second).

For such needs, consider Apache Kafka, NATS JetStream, or RabbitMQ.

## Quick Start

### Installation

```bash
go get github.com/valinurovam/fifodb
```

### Basic Usage

```go
package main

import (
	"log"
	"github.com/valinurovam/fifodb"
)

func main() {
	db, err := fifodb.Open(&fifodb.Options{
		Path:               "myqueue",      // directory to store data
		MaxBytesPerSegment: 1024 * 1024,    // 1 MB per segment
		WriteBufferSize:    4096,           // write buffer for segments
		ReadBufferSize:     4096,           // read buffer for segments
		WALWriteBufferSize: 4096,           // write buffer for WAL
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Push a message
	segID, offset, err := db.Push([]byte("Hello, fifodb!"))
	if err != nil {
		log.Fatal(err)
	}

	// Pop a message
	data, gotSegID, gotOffset, err := db.Pop()
	if err != nil {
		log.Fatal(err)
	}
	if data != nil {
		println(string(data)) // => "Hello, fifodb!"
	}

	// Acknowledge the message
	err = db.Ack(gotSegID, gotOffset)
	if err != nil {
		log.Fatal(err)
	}
}
```

> **Note**:
> - `Pop()` returns `(nil, 0, 0, nil)` when the queue is empty.
> - Messages are **not deleted** until explicitly acknowledged via `Ack`.

## Configuration Options

The following options can be passed to `fifodb.Open`:

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `Path` | `string` | Directory to store data files. Created if it does not exist. | *Required* |
| `WriteBufferSize` | `uint32` | Size of the write buffer for segment files (bytes). | `65536` (64 KB) |
| `ReadBufferSize` | `uint32` | Size of the read buffer for segment files (bytes). | `65536` (64 KB) |
| `MaxBytesPerSegment` | `uint32` | Maximum size of a single segment file (bytes). | `67108864` (64 MB) |
| `WALWriteBufferSize` | `uint32` | Size of the write buffer for the WAL (bytes). | `65536` (64 KB) |
| `Logger` | `Logger` | Logger instance (must implement `Printf(string, ...interface{})`). If `nil`, logging is disabled. | `nil` |

For crash-recovery testing, set all buffer sizes to `1` to ensure immediate disk writes.

## License

MIT

## Acknowledgements

Design inspired by:
- Bitcask (on-disk format),
- Kafka (log-structured storage and compaction),
- SQLite (WAL-based recovery).

However, `fifodb` is a minimal, focused solution for single-node FIFO queues.

This README was drafted with the assistance of Qwen (Tongyi Lab).