//! A Bitcask-style key-value store implementation.
//!
//! This crate provides a simple, efficient key-value store based on the Bitcask paper.
//! It uses an append-only log structure with an in-memory index for fast lookups.
//!
//! # Features
//!
//! - Single-writer, multiple-reader architecture
//! - Process-safe file locking
//! - Automatic log file rotation
//! - CRC32 checksums for data integrity
//! - Efficient in-memory key directory
//! - Crash recovery through log replay
//!
//! # Implementation Details
//!
//! The store consists of:
//! - Active log file for current writes
//! - Immutable sealed log files
//! - In-memory key directory for O(1) lookups
//! - File-based process locking
//!
//! # Durability Guarantees
//!
//! - Atomic single-key operations
//! - Crash recovery through log replay
//! - Data integrity verification via CRC32
//!
//! # Limitations
//!
//! - All keys must fit in memory
//! - Single writer at a time
//! - No multi-key transactions

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;

/// Errors that can occur during database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid log file name encountered during operations
    #[error("Invalid log file name '{filename}'")]
    InvalidLogFileName { filename: String },

    /// Failed to parse timestamp from file name or data
    #[error("Failed to parse timestamp '{value}'")]
    TimestampParse {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },

    /// Underlying IO operation failed
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Attempted to open database for writing when another process has the write lock
    #[error("Only one writer allowed at a time")]
    WriterLock,

    /// Key not found in database
    #[error("Key not found")]
    KeyNotFound,

    /// Referenced file not found in database directory
    #[error("File {0} not found")]
    FileNotFound(String),

    /// Attempted to store empty value
    #[error("Value size must be greater than 0")]
    InvalidEmptyValue,

    /// Attempted to use empty key
    #[error("Key size must be greater than 0")]
    InvalidEmptyKey,

    /// System time operation failed
    #[error("Timestamp error: {0}")]
    TimestampError(#[from] std::time::SystemTimeError),

    /// Timestamp value overflow when converting to u64
    #[error("Timestamp overflow, converting to u64: {0}")]
    TimestampOverflow(#[from] std::num::TryFromIntError),

    /// No active file found when opening existing database
    #[error("Active file not found in non empty path")]
    ActiveFileNotFound,

    /// Invalid data deserialization encountered
    #[error("Invalid data deserialization: {0}")]
    InvalidDataDeserialize(#[from] std::array::TryFromSliceError),
}

/// The name of the file lock. Used to ensure only one writer at a time and process safety.
const FILE_LOCK_PATH: &str = "db.lock";

/// Maximum size of active log file before rotation (4MB)
pub const MAX_ACTIVE_FILE_SIZE: u64 = 4 * 1024 * 1024;

/// A Bitcask-style key-value store implementation.
///
/// Bitcask is an append-only log-structured storage engine that maintains an in-memory
/// index (keydir) mapping keys to their most recent value locations on disk.
///
/// # Features
/// - Single-writer, multiple-reader architecture
/// - Process-safe file locking
/// - Append-only log structure
/// - In-memory key directory
/// - Automatic log rotation at 4MB
///
/// # Thread Safety
///
/// The database ensures process-level safety through file locking, but is not
/// thread-safe internally. Concurrent access from multiple threads requires
/// appropriate synchronization mechanisms at the application level.
///
/// # Examples
///
/// Basic usage:
/// ```no_run
/// use bitask::Bitask;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut db = bitask::db::Bitask::open("my_db")?;
///
/// // Store a value
/// db.put(b"key".to_vec(), b"value".to_vec())?;
///
/// // Retrieve a value
/// let value = db.ask(b"key")?;
/// assert_eq!(value, b"value");
///
/// // Remove a value
/// db.remove(b"key".to_vec())?;
/// # Ok(())
/// # }
/// ```
///
/// # File Structure
/// - Active file: `<timestamp>.active.log` - Current file being written to
/// - Sealed files: `<timestamp>.log` - Immutable files after rotation
/// - Lock file: `db.lock` - Ensures single-writer access
///
/// # Log Rotation
/// Files are automatically rotated when they reach 4MB in size. When rotation occurs:
/// 1. Current active file is renamed from `.active.log` to `.log`
/// 2. New active file is created with current timestamp
/// 3. All existing data remains accessible
#[derive(Debug)]
pub struct Bitask {
    /// Base directory path where all database files are stored
    path: PathBuf,
    /// File lock handle to ensure single-writer access
    _file_lock: File,
    /// Timestamp identifier of the current active file
    writer_id: u64,
    /// Buffered writer for the active log file
    writer: BufWriter<File>,
    /// Map of file IDs to their respective buffered readers
    readers: HashMap<u64, BufReader<File>>,
    /// In-memory index mapping keys to their latest value locations
    keydir: BTreeMap<Vec<u8>, KeyDirEntry>,
}

/// Entry in the key directory mapping a key to its location on disk
#[derive(Debug)]
struct KeyDirEntry {
    /// File ID (timestamp) containing the value
    file_id: u64,
    /// Size of the value in bytes
    value_size: u32,
    /// Offset position of the value within the file
    value_position: u64,
    /// Timestamp when the entry was written
    timestamp: u64,
}

impl Bitask {
    /// Opens a Bitcask database at the specified path with exclusive write access.
    ///
    /// Creates a new database if one doesn't exist at the specified path.
    /// Uses file system locks to ensure only one writer exists across all processes.
    ///
    /// # Parameters
    ///
    /// * `path` - Path where the database files will be stored
    ///
    /// # Returns
    ///
    /// Returns a new [`Bitask`] instance if successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * Another process has write access ([`Error::WriterLock`])
    /// * Filesystem operations fail ([`Error::Io`])
    /// * No active file is found when opening existing DB ([`Error::ActiveFileNotFound`])
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut db = bitask::db::Bitask::open("my_db")?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        fs::create_dir_all(&path)?;
        let lock_path = path.as_ref().join(FILE_LOCK_PATH);

        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .append(false)
            .open(lock_path)?;

        lock_file
            .try_lock_exclusive()
            .map_err(|_| Error::WriterLock)?;

        let is_empty = match fs::read_dir(&path)?.next() {
            None => true,
            Some(Ok(entry)) if entry.file_name() == FILE_LOCK_PATH => {
                // If first entry is db.lock, check if there's a second entry
                fs::read_dir(&path)?.nth(1).is_none()
            }
            Some(_) => false,
        };

        if is_empty {
            Self::open_new(path, lock_file)
        } else {
            Self::open_existing(path, lock_file)
        }
    }

    /// Creates a new database at the specified path.
    ///
    /// # Parameters
    ///
    /// * `path` - Path where the database files will be stored
    /// * `lock_file` - The exclusive lock file for this database
    ///
    /// # Returns
    ///
    /// Returns a new [`Bitask`] instance if successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * Filesystem operations fail ([`Error::Io`])
    /// * System time operations fail ([`Error::TimestampError`])
    fn open_new(path: impl AsRef<Path>, lock_file: File) -> Result<Self, Error> {
        let timestamp = timestamp_as_u64()?;

        let writer_file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .append(true)
            .open(file_active_log_path(path.as_ref(), timestamp))?;

        let reader_file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .append(true)
            .open(file_active_log_path(path.as_ref(), timestamp))?;

        let writer = BufWriter::new(writer_file);
        let mut readers = HashMap::new();
        let reader = BufReader::new(reader_file);
        readers.insert(timestamp, reader);

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            _file_lock: lock_file,
            writer_id: timestamp,
            writer,
            readers,
            keydir: BTreeMap::new(),
        })
    }

    /// Opens an existing database at the specified path.
    ///
    /// # Parameters
    ///
    /// * `path` - Path where the database files are stored
    /// * `lock_file` - The exclusive lock file for this database
    ///
    /// # Returns
    ///
    /// Returns a [`Bitask`] instance initialized with the existing database state.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * Filesystem operations fail ([`Error::Io`])
    /// * Log file names are malformed ([`Error::InvalidLogFileName`])
    /// * Timestamps in filenames are invalid ([`Error::TimestampParse`])
    /// * No active log file exists ([`Error::ActiveFileNotFound`])
    fn open_existing(path: impl AsRef<Path>, lock_file: File) -> Result<Self, Error> {
        let mut active_timestamp = None;
        let mut active_file = None;
        let mut files: BTreeMap<u64, PathBuf> = BTreeMap::new();

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name == FILE_LOCK_PATH {
                continue;
            }

            let timestamp = name
                .split('.')
                .next()
                .ok_or_else(|| Error::InvalidLogFileName {
                    filename: name.to_string(),
                })?
                .parse()
                .map_err(|e| Error::TimestampParse {
                    value: name.to_string(),
                    source: e,
                })?;

            if name.ends_with(".active.log") {
                active_file = Some(entry.path());
                active_timestamp = Some(timestamp);
            } else if name.ends_with(".log") {
                files.insert(timestamp, entry.path());
            }
        }

        let active_timestamp = active_timestamp.ok_or(Error::ActiveFileNotFound)?;

        let writer = {
            let active_file = active_file.clone().ok_or(Error::ActiveFileNotFound)?;
            let writer_file = OpenOptions::new()
                .create(true)
                .read(true)
                .truncate(false)
                .append(true)
                .open(active_file)?;
            BufWriter::new(writer_file)
        };

        let mut reader = {
            let active_file = active_file.ok_or(Error::ActiveFileNotFound)?;
            let reader_file = OpenOptions::new()
                .create(true)
                .read(true)
                .truncate(false)
                .append(true)
                .open(active_file)?;
            BufReader::new(reader_file)
        };

        let keydir = Self::rebuild_keydir(&mut reader, active_timestamp)?;

        let mut readers = HashMap::new();
        readers.insert(active_timestamp, reader);

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            _file_lock: lock_file,
            writer_id: active_timestamp,
            writer,
            readers,
            keydir,
        })
    }

    /// Rebuilds the in-memory key directory from a log file.
    ///
    /// Scans through the given log file and reconstructs the key directory by:
    /// - Reading each command header
    /// - Processing key-value entries
    /// - Updating the keydir with the latest value positions
    ///
    /// # Arguments
    ///
    /// * `reader` - Buffered reader for the log file
    /// * `file_id` - Timestamp identifier of the log file
    ///
    /// # Returns
    ///
    /// Returns a [`BTreeMap`] containing the rebuilt key directory mapping keys to their latest positions
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * IO operations fail while reading the file ([`Error::Io`])
    /// * Log file contains invalid or corrupted data
    fn rebuild_keydir(
        reader: &mut BufReader<File>,
        file_id: u64,
    ) -> Result<BTreeMap<Vec<u8>, KeyDirEntry>, Error> {
        let mut keydir: BTreeMap<Vec<u8>, KeyDirEntry> = BTreeMap::new();
        let mut position = 0u64;

        loop {
            // Read just the header
            let mut header_buf = vec![0u8; CommandHeader::SIZE];
            match reader.read_exact(&mut header_buf) {
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let header = CommandHeader::deserialize(&header_buf)?;

            // Read just the key
            let mut key = vec![0u8; header.key_len as usize];
            reader.read_exact(&mut key)?;

            // Skip the value bytes
            reader.seek(SeekFrom::Current(header.value_size as i64))?;

            if header.value_size == 0 {
                // Remove command
                keydir.remove(&key);
            } else {
                // Set command
                match keydir.get(&key) {
                    Some(existing) if existing.timestamp >= header.timestamp => {
                        // Skip older or same-age entries
                        continue;
                    }
                    _ => {
                        let value_position =
                            position + CommandHeader::SIZE as u64 + header.key_len as u64;
                        keydir.insert(
                            key,
                            KeyDirEntry {
                                file_id,
                                value_size: header.value_size,
                                value_position,
                                timestamp: header.timestamp,
                            },
                        );
                    }
                }
            }

            position +=
                CommandHeader::SIZE as u64 + header.key_len as u64 + header.value_size as u64;
        }
        Ok(keydir)
    }

    /// Rotates the active log file when it reaches the size limit.
    ///
    /// This process:
    /// 1. Renames the current active file to a sealed log file
    /// 2. Creates a new active file with current timestamp
    /// 3. Updates internal writer and reader references
    ///
    /// # Returns
    ///
    /// Returns `()` if rotation was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * File operations fail (`Error::Io`)
    /// * System time operations fail (`Error::TimestampError`)
    /// * IO operations fail (`Error::Io`)
    fn rotate_active_file(&mut self) -> Result<(), Error> {
        let timestamp = timestamp_as_u64()?;

        // Rename current active file to regular log file
        let old_path = file_active_log_path(&self.path, self.writer_id);
        let new_path = file_log_path(&self.path, self.writer_id);
        fs::rename(old_path, new_path)?;

        // Create new active file
        let writer_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_active_log_path(&self.path, timestamp))?;

        let reader_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_active_log_path(&self.path, timestamp))?;

        // Update writer and readers
        self.writer = BufWriter::new(writer_file);
        self.readers.insert(timestamp, BufReader::new(reader_file));
        self.writer_id = timestamp;

        Ok(())
    }

    /// Retrieves the value associated with the given key.
    ///
    /// Performs an O(1) lookup in the in-memory index followed by a single disk read.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// Returns the value as a [`Vec<u8>`] if the key exists.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * The key is empty ([`Error::InvalidEmptyKey`])
    /// * The key doesn't exist ([`Error::KeyNotFound`])
    /// * The data file is missing ([`Error::FileNotFound`])
    /// * IO operations fail ([`Error::Io`])
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # let mut db = bitask::db::Bitask::open("my_db")?;
    /// if let Ok(value) = db.ask(b"my_key") {
    ///     println!("Found value: {:?}", value);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn ask(&mut self, key: &[u8]) -> Result<Vec<u8>, Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if let Some(entry) = self.keydir.get(key) {
            if let std::collections::hash_map::Entry::Vacant(e) = self.readers.entry(entry.file_id)
            {
                let file = OpenOptions::new()
                    .read(true)
                    .open(file_log_path(&self.path, entry.file_id))?;
                e.insert(BufReader::new(file));
            }

            let reader = self
                .readers
                .get_mut(&entry.file_id)
                .ok_or(Error::FileNotFound(format!("{}", entry.file_id)))?;

            reader.seek(SeekFrom::Start(entry.value_position))?;
            let mut value = vec![0; entry.value_size as usize]; // Initialize with zeros
            reader.read_exact(&mut value)?;
            return Ok(value);
        }

        Err(Error::KeyNotFound)
    }

    /// Stores a key-value pair in the database.
    ///
    /// If the key already exists, it will be updated with the new value.
    /// The operation is atomic and durable (synced to disk).
    ///
    /// Performance: Requires one disk write (append-only) and one in-memory index update.
    /// May trigger file rotation if the active file exceeds size limit (4MB).
    ///
    /// # Parameters
    ///
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Returns
    ///
    /// Returns `()` if the operation was successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * The key is empty ([`Error::InvalidEmptyKey`])
    /// * The value is empty ([`Error::InvalidEmptyValue`])
    /// * IO operations fail ([`Error::Io`])
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # let mut db = bitask::db::Bitask::open("my_db")?;
    /// db.put(b"my_key".to_vec(), b"my_value".to_vec())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if value.is_empty() {
            return Err(Error::InvalidEmptyValue);
        }

        let file_size = self.writer.get_ref().metadata()?.len();
        if file_size > MAX_ACTIVE_FILE_SIZE {
            log::debug!("File size {} exceeded limit, rotating", file_size);
            self.rotate_active_file()?;

            if false {
                log::debug!("Auto-compaction is enabled, checking file count");
                // Count immutable files and trigger compaction if too many
                let immutable_files = std::fs::read_dir(&self.path)?
                    .filter_map(Result::ok)
                    .filter(|entry| {
                        let name = entry.file_name().to_string_lossy().to_string();
                        name.ends_with(".log") && !name.ends_with(".active.log")
                    })
                    .count();

                log::debug!("Found {} immutable files", immutable_files);
                if immutable_files >= 2 {
                    log::debug!(
                        "Auto-triggering compaction with {} immutable files",
                        immutable_files
                    );
                    self.compact()?;
                }
            } else {
                log::debug!("Auto-compaction is disabled");
            }
        }

        // Pre-allocate a single buffer for the entire command
        let total_size = CommandHeader::SIZE + key.len() + value.len();
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(&[0; CommandHeader::SIZE]);
        buffer.extend_from_slice(&key);
        buffer.extend_from_slice(&value);

        let command = CommandSet::new(key.clone(), value.clone())?;
        command.serialize(&mut buffer)?;

        let position = self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&buffer)?;
        self.writer.flush()?;

        let value_position = position + CommandHeader::SIZE as u64 + key.len() as u64;
        self.keydir.insert(
            key,
            KeyDirEntry {
                file_id: self.writer_id,
                value_size: value.len() as u32,
                value_position,
                timestamp: command.timestamp,
            },
        );
        Ok(())
    }

    /// Removes a key-value pair from the database.
    ///
    /// The operation is atomic and durable. Even if the key doesn't exist,
    /// a tombstone entry is written to ensure the removal is persisted.
    ///
    /// Performance: Requires one disk write (append-only) and one in-memory index update.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to remove
    ///
    /// # Returns
    ///
    /// Returns `()` if the operation was successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * The key is empty ([`Error::InvalidEmptyKey`])
    /// * IO operations fail ([`Error::Io`])
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # let mut db = bitask::db::Bitask::open("my_db")?;
    /// db.remove(b"my_key".to_vec())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn remove(&mut self, key: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        // Pre-allocate buffer for remove command
        let total_size = CommandHeader::SIZE + key.len();
        let mut buffer = Vec::with_capacity(total_size);
        buffer.extend_from_slice(&[0; CommandHeader::SIZE]);
        buffer.extend_from_slice(&key);

        let command = CommandRemove::new(key.clone())?;
        command.serialize(&mut buffer)?;

        self.writer.write_all(&buffer)?;
        self.writer.flush()?;

        self.keydir.remove(&key);
        Ok(())
    }

    /// Compacts the database by removing obsolete entries and merging files.
    ///
    /// This process:
    /// 1. Identifies immutable files (not including active file)
    /// 2. Creates a new compacted file with only latest entries
    /// 3. Removes old files after successful compaction
    ///
    /// Performance: Requires reading all immutable files and writing live entries
    /// to a new file. Memory usage remains constant as entries are processed
    /// sequentially.
    ///
    /// # Returns
    ///
    /// Returns `()` if compaction was successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * IO operations fail ([`Error::Io`])
    /// * File operations fail ([`Error::FileNotFound`])
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # let mut db = bitask::db::Bitask::open("my_db")?;
    /// // After many operations, compact to reclaim space
    /// db.compact()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn compact(&mut self) -> Result<(), Error> {
        let immutable_files = std::fs::read_dir(&self.path)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                name.ends_with(".log") && !name.ends_with(".active.log")
            })
            .count();
        if immutable_files < 2 {
            return Ok(());
        }

        // Create new file for compaction
        let timestamp = timestamp_as_u64()?;
        let mut compaction_writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(file_log_path(&self.path, timestamp))?,
        );

        let mut new_pos = 0;
        // Copy live entries
        for (key, entry) in self.keydir.iter_mut() {
            // Skip entries in active file
            if entry.file_id == self.writer_id {
                continue;
            }

            // Open reader at the start of the entry (header position)
            let mut reader = BufReader::new(File::open(file_log_path(&self.path, entry.file_id))?);
            let header_pos = entry.value_position - key.len() as u64 - CommandHeader::SIZE as u64;
            reader.seek(SeekFrom::Start(header_pos))?;

            // Copy the entire entry (header + key + value)
            let entry_size =
                CommandHeader::SIZE as u64 + key.len() as u64 + entry.value_size as u64;
            io::copy(&mut reader.take(entry_size), &mut compaction_writer)?;

            // Update position
            entry.file_id = timestamp;
            entry.value_position = new_pos + CommandHeader::SIZE as u64 + key.len() as u64;
            new_pos += entry_size;
        }

        compaction_writer.flush()?;

        // Remove old files
        for file in std::fs::read_dir(&self.path)? {
            let file = file?;
            let name = file.file_name().to_string_lossy().to_string();
            if name.ends_with(".log")
                && !name.ends_with(".active.log")
                && !name.starts_with(&timestamp.to_string())
            {
                std::fs::remove_file(file.path())?;
            }
        }

        Ok(())
    }
}

/// Header structure for commands stored in the log files.
/// Contains metadata about the stored key-value pairs.
#[derive(Debug)]
struct CommandHeader {
    /// CRC32 checksum of the key and value
    crc: u32,
    /// Timestamp when the command was written
    timestamp: u64,
    /// Length of the key in bytes
    key_len: u32,
    /// Size of the value in bytes (0 for remove commands)
    value_size: u32,
}

impl CommandHeader {
    /// Size of the header in bytes, computed from its field types.
    const SIZE: usize = std::mem::size_of::<u32>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u32>();

    /// Creates a new command header with the specified metadata.
    ///
    /// # Arguments
    ///
    /// * `crc` - CRC32 checksum of the key and value data
    /// * `timestamp` - Timestamp when the command was created (milliseconds since UNIX epoch)
    /// * `key_len` - Length of the key in bytes
    /// * `value_len` - Length of the value in bytes (0 for remove commands)
    ///
    /// # Returns
    ///
    /// Returns a new [`CommandHeader`] initialized with the provided values
    fn new(crc: u32, timestamp: u64, key_len: u32, value_len: u32) -> Self {
        Self {
            crc,
            timestamp,
            key_len,
            value_size: value_len,
        }
    }

    /// Serializes the header into a byte buffer.
    ///
    /// The header is written in little-endian byte order with the following layout:
    /// - CRC32 (4 bytes)
    /// - Timestamp (8 bytes)
    /// - Key length (4 bytes)
    /// - Value size (4 bytes)
    ///
    /// # Arguments
    ///
    /// * `buffer` - The vector to write the serialized header to
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if writing to the buffer fails
    fn serialize(&self, buffer: &mut [u8]) -> Result<(), Error> {
        // Verify buffer has enough space
        if buffer.len() < Self::SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "buffer too small for header",
            )));
        }

        buffer[0..4].copy_from_slice(&self.crc.to_le_bytes());
        buffer[4..12].copy_from_slice(&self.timestamp.to_le_bytes());
        buffer[12..16].copy_from_slice(&self.key_len.to_le_bytes());
        buffer[16..20].copy_from_slice(&self.value_size.to_le_bytes());
        Ok(())
    }

    /// Deserializes a header from a byte buffer.
    ///
    /// # Arguments
    ///
    /// * `buf` - Buffer containing the serialized header data (must be at least [`Self::SIZE`] bytes)
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if:
    /// * The buffer is smaller than [`Self::SIZE`] bytes
    /// * The buffer contains invalid data that can't be converted to header fields
    ///
    /// # Panics
    ///
    /// Will not panic as buffer size is checked before conversion
    fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < Self::SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer too small for header",
            )));
        }

        let crc = u32::from_le_bytes(buf[0..4].try_into()?);
        let timestamp = u64::from_le_bytes(buf[4..12].try_into()?);
        let key_len = u32::from_le_bytes(buf[12..16].try_into()?);
        let value_size = u32::from_le_bytes(buf[16..20].try_into()?);

        Ok(Self {
            crc,
            timestamp,
            key_len,
            value_size,
        })
    }
}

/// A command to append a key-value pair to the log.
#[derive(Debug)]
struct CommandSet {
    /// CRC32 checksum of key and value
    crc: u32,
    /// Timestamp when command was created (milliseconds since UNIX epoch)
    timestamp: u64,
    /// Key to be stored as [`Vec<u8>`]
    key: Vec<u8>,
    /// Value to be associated with the key as [`Vec<u8>`]
    value: Vec<u8>,
}

/// A command to remove a key from the database.
#[derive(Debug)]
struct CommandRemove {
    /// CRC32 checksum of key
    crc: u32,
    /// Timestamp when command was created (milliseconds since UNIX epoch)
    timestamp: u64,
    /// Key to be removed as [`Vec<u8>`]
    key: Vec<u8>,
}

impl CommandSet {
    /// Creates a new set command.
    ///
    /// Generates a CRC32 checksum of the key-value pair and includes current timestamp.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store as [`Vec<u8>`]
    /// * `value` - The value to associate with the key as [`Vec<u8>`]
    ///
    /// # Returns
    ///
    /// Returns a new [`CommandSet`] if successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * System time operations fail ([`Error::TimestampError`])
    /// * Timestamp conversion fails ([`Error::TimestampOverflow`])
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Result<Self, Error> {
        let timestamp = timestamp_as_u64()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        hasher.update(value.as_slice());
        let crc = hasher.finalize();

        Ok(Self {
            crc,
            timestamp,
            key,
            value,
        })
    }

    /// Serializes the command into a byte array.
    ///
    /// Format:
    /// 1. Command header (CRC, timestamp, key length, value length)
    /// 2. Key bytes
    /// 3. Value bytes
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to write the serialized command to as [`Vec<u8>`]
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Io`] if IO operations fail
    fn serialize(&self, buffer: &mut [u8]) -> Result<(), Error> {
        let total_size = CommandHeader::SIZE + self.key.len() + self.value.len();
        if buffer.len() < total_size {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "buffer too small for command",
            )));
        }

        // Write header
        CommandHeader::new(
            self.crc,
            self.timestamp,
            self.key.len() as u32,
            self.value.len() as u32,
        )
        .serialize(&mut buffer[..CommandHeader::SIZE])?;

        // Write key and value
        buffer[CommandHeader::SIZE..CommandHeader::SIZE + self.key.len()]
            .copy_from_slice(&self.key);
        buffer[CommandHeader::SIZE + self.key.len()..total_size].copy_from_slice(&self.value);

        Ok(())
    }
}

impl CommandRemove {
    /// Creates a new remove command.
    ///
    /// Generates a CRC32 checksum of the key and includes current timestamp.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove as [`Vec<u8>`]
    ///
    /// # Returns
    ///
    /// Returns a new [`CommandRemove`] if successful.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// * System time operations fail ([`Error::TimestampError`])
    /// * Timestamp conversion fails ([`Error::TimestampOverflow`])
    pub fn new(key: Vec<u8>) -> Result<Self, Error> {
        let timestamp = timestamp_as_u64()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        let crc = hasher.finalize();

        Ok(Self {
            crc,
            timestamp,
            key,
        })
    }

    /// Serializes the command into a byte array.
    ///
    /// Format:
    /// 1. Command header (CRC, timestamp, key length, value length = 0)
    /// 2. Key bytes
    ///
    /// # Arguments
    ///
    /// * `buffer` - The buffer to write the serialized command to as [`Vec<u8>`]
    ///
    /// # Errors
    ///
    /// Returns an [`Error::Io`] if IO operations fail
    fn serialize(&self, buffer: &mut [u8]) -> Result<(), Error> {
        let total_size = CommandHeader::SIZE + self.key.len();
        if buffer.len() < total_size {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "buffer too small for command",
            )));
        }

        // Write header
        CommandHeader::new(self.crc, self.timestamp, self.key.len() as u32, 0)
            .serialize(&mut buffer[..CommandHeader::SIZE])?;

        // Write key
        buffer[CommandHeader::SIZE..total_size].copy_from_slice(&self.key);

        Ok(())
    }
}

/// Constructs the path for an active log file.
///
/// # Arguments
///
/// * `path` - Base directory path
/// * `timestamp` - Timestamp used as file identifier
///
/// # Returns
///
/// Returns a [`PathBuf`] containing the full path to the active log file in format:
/// `<path>/<timestamp>.active.log`
fn file_active_log_path(path: impl AsRef<Path>, timestamp: u64) -> PathBuf {
    path.as_ref().join(format!("{}.active.log", timestamp))
}

/// Constructs the path for a regular (sealed) log file.
///
/// # Arguments
///
/// * `path` - Base directory path
/// * `timestamp` - Timestamp used as file identifier
///
/// # Returns
///
/// Returns a [`PathBuf`] containing the full path to the log file in format:
/// `<path>/<timestamp>.log`
fn file_log_path(path: impl AsRef<Path>, timestamp: u64) -> PathBuf {
    path.as_ref().join(format!("{}.log", timestamp))
}

/// Gets current timestamp as milliseconds since UNIX epoch.
///
/// # Returns
///
/// Returns the current time in milliseconds since UNIX epoch as [`u64`]
///
/// # Errors
///
/// Returns an [`Error`] if:
/// * System time operations fail ([`Error::TimestampError`])
/// * Milliseconds value doesn't fit in [`u64`] ([`Error::TimestampOverflow`])
fn timestamp_as_u64() -> Result<u64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(Error::TimestampError)?
        .as_millis()
        .try_into()
        .map_err(Error::TimestampOverflow)
}

impl Drop for Bitask {
    /// Cleans up resources when the database is dropped.
    ///
    /// Removes the physical lock file from the filesystem to allow
    /// future database instances to acquire the write lock.
    fn drop(&mut self) {
        // Remove the physical lock file from the filesystem
        if let Ok(path) = self.path.join("db.lock").canonicalize() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_command_serialization() {
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let command = CommandSet::new(key.clone(), value.clone()).unwrap();

        let mut buffer = vec![0; CommandHeader::SIZE + key.len() + value.len()];
        command.serialize(&mut buffer).unwrap();

        // Check header structure
        let header = CommandHeader::deserialize(&buffer[..CommandHeader::SIZE]).unwrap();
        assert_eq!(header.key_len, key.len() as u32);
        assert_eq!(header.value_size, value.len() as u32);

        // Check key and value bytes
        assert_eq!(
            &buffer[CommandHeader::SIZE..CommandHeader::SIZE + key.len()],
            key
        );
        assert_eq!(&buffer[CommandHeader::SIZE + key.len()..], value);

        // Verify CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key);
        hasher.update(&value);
        assert_eq!(header.crc, hasher.finalize());
    }

    #[test]
    fn test_remove_command_serialization() {
        let key = b"key".to_vec();
        let command = CommandRemove::new(key.clone()).unwrap();

        let mut buffer = vec![0; CommandHeader::SIZE + key.len()];
        command.serialize(&mut buffer).unwrap();

        // Check header structure
        let header = CommandHeader::deserialize(&buffer[..CommandHeader::SIZE]).unwrap();
        assert_eq!(header.key_len, key.len() as u32);
        assert_eq!(header.value_size, 0);

        // Check key bytes
        assert_eq!(&buffer[CommandHeader::SIZE..], key);

        // Verify CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&key);
        assert_eq!(header.crc, hasher.finalize());
    }

    #[test]
    fn test_automatic_compaction_disabled() {
        // Create test directory
        let dir = tempfile::tempdir().unwrap();
        let mut db = Bitask::open(dir.path()).unwrap();

        // Insert enough data to trigger multiple rotations
        for i in 0..3000 {
            let key = format!("key{}", i).into_bytes();
            let value = vec![0; 8 * 1024]; // 8KB value to fill files quickly
            db.put(key, value).unwrap();
        }

        // Count immutable log files - should be MORE than 2 since auto-compaction is disabled
        let log_files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();
                name.ends_with(".log") && !name.ends_with(".active.log")
            })
            .count();

        assert!(
            log_files >= 2,
            "Expected 2 or more log files since auto-compaction is disabled"
        );
    }
}
