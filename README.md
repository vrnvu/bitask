# bitask

Bitask is a Rust implementation of Bitcask, a log-structured key-value store optimized for high-performance reads and writes. Here's the summary in 10 lines:

1. Core Idea: Bitcask uses append-only logs for writes, ensuring atomic and durable operations.
2. Indexing: Keys are stored in memory in a BTreeMap, mapping to their positions in the logs for O(1) lookups.
3. Data Layout: Storage consists of active file (for writes) and sealed immutable files.
4. Reads: Lookups find key positions in memory, then read values directly from log files.
5. Compaction: Manual compaction merges files to reclaim space and remove obsolete entries.
6. Crash Recovery: Index rebuilds on startup by scanning logs, using timestamps for conflict resolution.
7. Process Safety: File-based locking ensures single-writer, multiple-reader access.
8. Data Integrity: CRC32 checksums verify data correctness.
9. File Management: Automatic log rotation at 4MB with timestamp-based naming.
10. Design Principles: Simplicity, durability, and efficient reads/writes.

## Paper

https://riak.com/assets/bitcask-intro.pdf

## Project

This implementation provides:

- A Rust library crate for embedding in other projects
- Core Bitcask features including:
  - Append-only log structure with automatic rotation (4MB per file)
  - In-memory key directory using BTreeMap
  - Process-safe file locking
  - Crash recovery through log replay
  - Data integrity via CRC32 checksums
- Only byte arrays (`Vec<u8>`) are supported for keys and values

## Usage

```rust
use bitask::db::Bitask;

// Open database with exclusive write access
let mut db = Bitask::open("./db")?;

// Store a value
db.put(b"key".to_vec(), b"value".to_vec())?;

// Retrieve a value
let value = db.ask(b"key")?;
assert_eq!(value, b"value");

// Remove a value
db.remove(b"key".to_vec())?;

// Manual compaction
db.compact()?;

// Process safety demonstration
let another_db = Bitask::open("./db");
assert!(matches!(another_db.err().unwrap(), bitask::db::Error::WriterLock));
```

## Implementation Details

### Log Files
- Active file: `<timestamp>.active.log` - Current file being written to
- Sealed files: `<timestamp>.log` - Immutable files after rotation
- Lock file: `db.lock` - Ensures single-writer access

### Log Rotation
- Active log files rotate automatically at 4MB
- Files are named with millisecond timestamps
- After rotation, `.active.log` becomes `.log` and new `.active.log` is created

### Durability Guarantees
- Atomic single-key operations
- Crash recovery through log replay
- Data integrity verification via CRC32

### Limitations
- All keys must fit in memory
- Single writer at a time
- No multi-key transactions
