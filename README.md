# bitask

Bitask is based on Bitcask, a fast, log-structured key-value (KV) store optimized for high-performance reads and writes. Here's the summary in 10 lines:

1. Core Idea: Bitcask uses a log-structured design where writes are appended to immutable logs, ensuring fast and efficient write operations.
2. Indexing: Keys are stored in memory as a hash table, mapping to their positions in the logs, allowing O(1) lookups.
3. Data Layout: The storage consists of multiple files—active (for ongoing writes) and sealed files (immutable).
4. Reads: A lookup finds the key in memory, retrieves its offset, and reads the value from the corresponding log file.
5. Compaction: Old log files are periodically merged to eliminate stale keys and reclaim disk space.
6. Crash Recovery: On restart, the index is rebuilt by scanning the log files for keys and their latest values.
7. Advantages: Fast writes, efficient reads, and a simple design make it ideal for write-heavy workloads.
8. Challenges: High memory usage for indexing and slower recovery times due to index rebuilding are key trade-offs.
9. Use Cases: Bitcask is used in systems like Riak for storing small-to-moderate-sized key-value pairs.
10. Design Principles: Simplicity, immutability, and log-structured organization ensure its reliability and speed.

## Paper

https://riak.com/assets/bitcask-intro.pdf

## Project

This implementation provides:

- A command-line interface (`bitask`) for interacting with the key-value store
- A Rust library crate that can be used as a dependency in other projects
- Core Bitcask features including:
  - Append-only log files with automatic rotation (4MB per file)
  - In-memory key directory
  - Process-safe file locking
  - Crash recovery
- Only byte arrays (`Vec<u8>`) are supported for keys and values

## Usage as a CLI

```bash
bitask ask --key my_key
```

```bash
bitask put --key my_key --value my_value
```

## Usage as a library

```rust
use bitask::db::Bitask;
use std::path::Path;

// Open database with exclusive access
let mut db = Bitask::open("./db")?;

// Store a value
db.put(b"key".to_vec(), b"value".to_vec())?;

// Retrieve a value
let value = db.ask(b"key")?;
assert_eq!(value, b"value");

// Remove a value
db.remove(b"key")?;

// Only one writer can exist at a time
let another_db = Bitask::open("./db");
assert!(matches!(another_db.err().unwrap(), bitask::db::Error::WriterLock));
```

## Implementation Details

### Log Rotation
- Active log files are automatically rotated when they reach 4MB
- Each log file is named with a timestamp (e.g., `1234567890.log`)
- Active file has `.active.log` extension
- After rotation, the old file is renamed to `.log` and a new `.active.log` is created
