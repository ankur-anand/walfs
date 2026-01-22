# walfs

## This Has been extracted from the https://github.com/ankur-anand/unisondb

A high-performance Write-Ahead Log (WAL) implementation in Go using memory-mapped I/O (mmap), designed for both writing at scale and reading at scale.

**Key Features:**
* Memory-mapped I/O for low-overhead random access
* 8-byte aligned entries to support efficient page caching
* Built-in corruption detection using CRC32 and trailer markers
* Designed for fast recovery and streaming-based replication
* Automatic segment rotation and cleanup policies
* Reference-counted readers with safe concurrent access

## Use Cases

walfs is ideal for systems requiring durable, high-performance sequential data storage:

- **Databases**: Write-ahead logs, transaction logs, event sourcing, time-series storage
- **Distributed Systems**: Replication, change data capture (CDC), message queues, consensus protocols (Raft/Paxos)
- **Streaming**: Real-time data pipelines, log aggregation, commit logs (Kafka-like)
- **Recovery**: Point-in-time recovery, disaster recovery, state machine replication

## Installation

```bash
go get github.com/ankur-anand/walfs
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/ankur-anand/walfs"
)

func main() {
    // Create a new WAL
    wal, err := walfs.NewWALog("./data", ".wal")
    if err != nil {
        log.Fatal(err)
    }
    defer wal.Close()

    // Write some data
    data := []byte("Hello, WAL!")
    pos, err := wal.Write(data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Written at: %s\n", pos)

    // Read the data back
    readData, err := wal.Read(pos)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Read: %s\n", readData)
}
```

## Examples

### Batch Writing

The batch API allows you to write multiple records in a single operation.

```go
package main

import (
    "fmt"
    "log"

    "github.com/ankur-anand/walfs"
)

func main() {
    // Create a new WAL
    wal, err := walfs.NewWALog("./data", ".wal")
    if err != nil {
        log.Fatal(err)
    }
    defer wal.Close()

    // Prepare multiple records to write as a batch
    records := [][]byte{
        []byte("Transaction 1: User login"),
        []byte("Transaction 2: Update profile"),
        []byte("Transaction 3: Add to cart"),
        []byte("Transaction 4: Process payment"),
        []byte("Transaction 5: Send confirmation"),
    }

    // Write all records in a single batch operation
    positions, err := wal.WriteBatch(records)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Successfully wrote %d records\n", len(positions))
    for i, pos := range positions {
        fmt.Printf("Record %d written at: %s\n", i+1, pos)
    }

    // Read back the written records
    for i, pos := range positions {
        data, err := wal.Read(pos)
        if err != nil {
            log.Printf("Failed to read record %d: %v", i+1, err)
            continue
        }
        fmt.Printf("Read record %d: %s\n", i+1, data)
    }
}
```

- **Automatic Segment Rotation**: If the batch doesn't fit in the current segment, WriteBatch automatically handles rotation and continues writing to the new segment


### Log Tailing (Continuous Reading)

You can continuously tail the WAL, similar to `tail -f`. It's useful for replication, streaming, or real-time processing scenarios.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"
    "log"
    "time"

    "github.com/ankur-anand/walfs"
)

func main() {
    // Create or open existing WAL
    wal, err := walfs.NewWALog("./data", ".wal")
    if err != nil {
        log.Fatal(err)
    }
    defer wal.Close()

    // Start a goroutine to write data periodically
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        ticker := time.NewTicker(1 * time.Second)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                data := []byte(fmt.Sprintf("Log entry at %s", time.Now().Format(time.RFC3339)))
                pos, err := wal.Write(data)
                if err != nil {
                    log.Printf("Write error: %v", err)
                    continue
                }
                fmt.Printf("Written: %s (at %s)\n", data, pos)
            }
        }
    }()

    // Tail the log continuously
    reader := wal.NewReader()
    defer reader.Close()

    fmt.Println("Starting log tail...")
    for {
        data, pos, err := reader.Next()
        if err == io.EOF {
            // Reached end of sealed segments, wait for new data
            time.Sleep(100 * time.Millisecond)

            // Create new reader to pick up new segments
            reader.Close()
            reader = wal.NewReader()
            continue
        }
        if errors.Is(err, walfs.ErrNoNewData) {
            // No new data in active segment yet, wait a bit
            time.Sleep(100 * time.Millisecond)
            continue
        }
        if err != nil {
            log.Printf("Read error: %v", err)
            break
        }

        // Process the log entry
        fmt.Printf("Read: %s (from %s)\n", data, pos)
    }
}
```

### Tailing from a Specific Position

You can also tail the WAL starting from a specific position, useful for resuming replication or processing from a known checkpoint.

```go
package main

import (
    "errors"
    "fmt"
    "io"
    "log"
    "time"

    "github.com/ankur-anand/walfs"
)

func main() {
    // Create or open existing WAL
    wal, err := walfs.NewWALog("./data", ".wal")
    if err != nil {
        log.Fatal(err)
    }
    defer wal.Close()

    // Write some initial data
    for i := 0; i < 5; i++ {
        wal.Write([]byte(fmt.Sprintf("Initial entry %d", i)))
    }

    // Get a checkpoint position (e.g., saved from previous run)
    checkpointPos, _ := wal.Write([]byte("Checkpoint entry"))
    fmt.Printf("Checkpoint saved at: %s\n", checkpointPos)

    // Write more data after checkpoint
    for i := 0; i < 5; i++ {
        wal.Write([]byte(fmt.Sprintf("Post-checkpoint entry %d", i)))
    }

    // Resume tailing from the checkpoint position
    // Use NewReaderAfter to start AFTER the checkpoint (skip the checkpoint record itself)
    reader, err := wal.NewReaderAfter(checkpointPos)
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    fmt.Printf("Tailing from position: %s\n", checkpointPos)

    // Continuous tail loop
    for {
        data, pos, err := reader.Next()
        if err == io.EOF {
            // Reached end of sealed segments
            time.Sleep(100 * time.Millisecond)

            // Get last read position and create new reader from there
            lastPos := reader.LastRecordPosition()
            reader.Close()

            reader, err = wal.NewReaderAfter(lastPos)
            if err != nil {
                log.Printf("Failed to create new reader: %v", err)
                break
            }
            continue
        }
        if errors.Is(err, walfs.ErrNoNewData) {
            // No new data yet, wait a bit
            time.Sleep(100 * time.Millisecond)
            continue
        }
        if err != nil {
            log.Printf("Read error: %v", err)
            break
        }

        // Process the log entry
        fmt.Printf("Read: %s (from %s)\n", data, pos)

        // Optionally save the position as new checkpoint
        // saveCheckpoint(pos)
    }
}
```

## Benchmarks

Performance characteristics on Apple M2 Pro (tested with Go 1.24.0):

### Write Performance (NoSync mode)

| Data Size | Throughput    | Latency      | Allocations |
|-----------|---------------|--------------|-------------|
| 16B       | 252.90 MB/s   | 63.27 ns/op  | 0 allocs/op |
| 1KB       | 2,590.33 MB/s | 395.3 ns/op  | 0 allocs/op |
| 32KB      | 3,511.94 MB/s | 9.3 µs/op    | 0 allocs/op |
| 64KB      | 3,952.91 MB/s | 16.6 µs/op   | 0 allocs/op |
| 512KB     | 4,270.20 MB/s | 122.8 µs/op  | 0 allocs/op |

### Read Performance (NoSync mode)

| Data Size | Operation       | Throughput            | Latency      | Allocations |
|-----------|-----------------|----------------------|--------------|-------------|
| 16B       | Direct Read     | 3,047.17 MB/s        | 5.25 ns/op   | 0 allocs/op |
| 1KB       | Direct Read     | 195,042.14 MB/s      | 5.25 ns/op   | 0 allocs/op |
| 1KB       | Sequential Read | 100,798.86 MB/s      | 1.0 µs/op    | 4 allocs/op |
| 512KB     | Direct Read     | 99,683,253.02 MB/s   | 5.26 ns/op   | 0 allocs/op |
| 512KB     | Sequential Read | 29,715,205.69 MB/s   | 547.0 ns/op  | 4 allocs/op |

### Concurrent Performance (NoSync mode)

| Test                              | Throughput    | Latency      | Allocations |
|-----------------------------------|---------------|--------------|-------------|
| Concurrent Write (2 goroutines)   | 1,336.37 MB/s | 766.3 ns/op  | 0 allocs/op |
| Concurrent R/W (8 goroutines)     | 4,761.33 MB/s | 215.1 ns/op  | 8 B/op      |

### Sync Impact (SyncAfterWrite mode)

| Data Size | NoSync Latency | SyncAfterWrite Latency | Overhead |
|-----------|----------------|------------------------|----------|
| 1KB       | 337.2 ns/op    | 44,687 ns/op          | ~132x    |
| 32KB      | 8.5 µs/op      | 65,278 ns/op          | ~7.6x    |
| 64KB      | 25.6 µs/op     | 86,395 ns/op          | ~3.4x    |

**Notes:**
- Actual performance will vary based on hardware, OS, and workload patterns

Run benchmarks yourself:
```bash
go test -bench=. -benchmem -benchtime=3s
```

## Architecture

### WAL Structure

The Write-Ahead Log (WAL) consists of multiple **segments**. Each segment is an individual file that stores a sequential series of log records. When a segment reaches its maximum size (default 16MB), the WAL automatically rotates to a new segment.

```
WALog
├── Segment 1 (000000001.wal)
├── Segment 2 (000000002.wal)
├── Segment 3 (000000003.wal) [Active]
└── ...
```

The WALog manages:
- Automatic segment creation and rotation
- Concurrent read access across all segments
- Segment lifecycle (active → sealed → cleanup)
- Recovery from crashes by scanning segment files

### Segment File Structure

Each segment is divided into two main regions:

1. **Segment Header** (64 bytes) - Metadata about the segment
2. **Records** - Sequential log entries, each consisting of header + data + trailer

```
+----------------------+-----------------------------+-------------+
|   Segment Header     |      Record 1               |  Record 2   |
|     (64 bytes)       |  Header + Data + Trailer    |     ...     |
+----------------------+-----------------------------+-------------+
```

#### Segment Header (Metadata Section)

The first 64 bytes contain segment metadata:

| Offset | Size | Field           | Description                                  |
|--------|------|------------------|---------------------------------------------|
| 0      | 4    | Magic            | Magic number (`0x5557414C` - "UWAL")        |
| 4      | 4    | Version          | Metadata format version                     |
| 8      | 8    | CreatedAt        | Creation timestamp (nanoseconds)            |
| 16     | 8    | LastModifiedAt   | Last modification timestamp (nanoseconds)   |
| 24     | 8    | WriteOffset      | Offset where next chunk will be written     |
| 32     | 8    | EntryCount       | Total number of chunks written              |
| 40     | 4    | Flags            | Segment state flags (Active, Sealed)        |
| 44     | 12   | Reserved         | Reserved for future use                     |
| 56     | 4    | CRC              | CRC32 checksum of first 56 bytes            |
| 60     | 4    | Padding/Reserved | Ensures 64-byte alignment                   |

#### Record Format (Aligned)

Each record is written in its own 8-byte aligned frame. All components (header + data + trailer) are padded to the next multiple of 8 bytes.

**Properties:**
* Each record is individually checksummed and terminates with a trailer marker
* On recovery, scanning stops at the first invalid or torn record
* The segment header includes a CRC for metadata validation

**Record Layout:**

| Offset    | Size         | Field    | Description                                                  |
|-----------|--------------|----------|--------------------------------------------------------------|
| 0         | 4 bytes      | CRC      | CRC32 of `[Length \| Data]`                                   |
| 4         | 4 bytes      | Length   | Size of the data payload in bytes                            |
| 8         | N bytes      | Data     | User payload                                                 |
| 8 + N     | 8 bytes      | Trailer  | Canary marker (`0xDEADBEEFFEEEDFACE`)                        |
| ...       | ≥0 bytes     | Padding  | Zero padding to align full frame to 8-byte boundary          |

**Alignment Example:**

For an 11-byte data payload:
* Total = 8 (header) + 11 (data) + 8 (trailer) = 27 bytes
* Final aligned frame = 32 bytes (with 5 bytes of padding)

## License

This project is available as open source. Please check the repository for license details.

## Acknowledgments

walfs draws inspiration from production-proven WAL implementations and incorporates lessons learned from real-world issues:

- **Trailer Marker Concept**: Inspired by [etcd issue #6191](https://github.com/etcd-io/etcd/issues/6191#issuecomment-240268979) - detecting torn writes with canary markers
- **Alignment Strategy**: Influenced by [BoltDB issue #548](https://github.com/boltdb/bolt/issues/548) - reducing partial writes across page boundaries

