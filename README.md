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
  - Append-only log files
  - In-memory key directory
  - Basic compaction
  - Crash recovery
- Only Strings are supported for now to simplify the implementation

## Usage as a CLI

```bash
bitask ask --key my_key
```

```bash
bitask put --key my_key --value my_value
```

## Usage as a library

```rust
use bitask::Bitask;

let bitask = Bitask::open(dir);
bitask.put("my_key", "my_value");
let value = bitask.ask("my_key");
assert_eq!(value, "my_value");
```
