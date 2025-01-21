//! A Bitcask-style key-value store implementation.
//!
//! This crate provides a simple, efficient key-value store based on the Bitcask paper.
//! It uses an append-only log structure with an in-memory index for fast lookups.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid log file name '{filename}'")]
    InvalidLogFileName { filename: String },

    #[error("Failed to parse timestamp '{value}'")]
    TimestampParse {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Only one writer allowed at a time")]
    WriterLock,

    #[error("Key not found")]
    KeyNotFound,

    #[error("File {0} not found")]
    FileNotFound(String),

    #[error("Value size must be greater than 0")]
    InvalidEmptyValue,

    #[error("Key size must be greater than 0")]
    InvalidEmptyKey,

    #[error("Timestamp error: {0}")]
    TimestampError(#[from] std::time::SystemTimeError),

    #[error("Timestamp overflow, converting to u64: {0}")]
    TimestampOverflow(#[from] std::num::TryFromIntError),

    #[error("Active file not found in non empty path")]
    ActiveFileNotFound,
}

/// The name of the file lock. Used to ensure only one writer at a time and process safety.
const FILE_LOCK_PATH: &str = "db.lock";

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
#[derive(Debug)]
pub struct Bitask {
    path: PathBuf,
    _file_lock: File,

    writer_id: u64,
    writer: BufWriter<File>,

    readers: HashMap<u64, BufReader<File>>,

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
    /// # Errors
    ///
    /// Returns an error if:
    /// * Another process has write access (`Error::WriterLock`)
    /// * Filesystem operations fail (`Error::Io`)
    /// * No active file is found when opening existing DB (`Error::ActiveFileNotFound`)
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
    /// # Errors
    ///
    /// Returns an error if:
    /// * Filesystem operations fail (`Error::Io`)
    /// * System time operations fail (`Error::TimestampError`)
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
    /// # Errors
    ///
    /// Returns an error if:
    /// * Filesystem operations fail (`Error::Io`)
    /// * Log file names are malformed (`Error::InvalidLogFileName`)
    /// * Timestamps in filenames are invalid (`Error::TimestampParse`)
    /// * No active log file exists (`Error::ActiveFileNotFound`)
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

    /// Retrieves the value associated with the given key.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to look up
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The key is empty (`Error::InvalidEmptyKey`)
    /// * The key doesn't exist (`Error::KeyNotFound`)
    /// * The data file is missing (`Error::FileNotFound`)
    /// * IO operations fail (`Error::Io`)
    #[must_use]
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
    /// # Parameters
    ///
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The key is empty (`Error::InvalidEmptyKey`)
    /// * The value is empty (`Error::InvalidEmptyValue`)
    /// * IO operations fail (`Error::Io`)
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if value.is_empty() {
            return Err(Error::InvalidEmptyValue);
        }

        let command = CommandSet::new(key.clone(), value.clone())?;
        let mut buffer = Vec::new();
        command.serialize(&mut buffer)?;

        // TODO optimize writer to store last offset/position
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
    /// # Parameters
    ///
    /// * `key` - The key to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The key is empty (`Error::InvalidEmptyKey`)
    /// * IO operations fail (`Error::Io`)
    pub fn remove(&mut self, key: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        let command = CommandRemove::new(key.clone())?;
        let mut buffer = Vec::new();
        command.serialize(&mut buffer)?;

        self.writer.write_all(&buffer)?;
        self.writer.flush()?;

        self.keydir.remove(&key);
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

    /// Creates a new command header.
    ///
    /// # Parameters
    ///
    /// * `crc` - CRC32 checksum of the key and value
    /// * `timestamp` - Current timestamp in milliseconds
    /// * `key_len` - Length of the key in bytes
    /// * `value_len` - Length of the value in bytes
    fn new(crc: u32, timestamp: u64, key_len: u32, value_len: u32) -> Self {
        Self {
            crc,
            timestamp,
            key_len,
            value_size: value_len,
        }
    }

    /// Serializes the header to a byte buffer.
    ///
    /// # Parameters
    ///
    /// * `buffer` - The buffer to write the header to
    ///
    /// # Errors
    ///
    /// Returns an error if the write operations fail (`Error::Io`)
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.write_all(&self.crc.to_le_bytes())?;
        buffer.write_all(&self.timestamp.to_le_bytes())?;
        buffer.write_all(&self.key_len.to_le_bytes())?;
        buffer.write_all(&self.value_size.to_le_bytes())?;
        Ok(())
    }

    /// Deserializes a header from a byte buffer.
    ///
    /// # Parameters
    ///
    /// * `buf` - The buffer containing the header data
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The buffer is too small (`Error::Io`)
    /// * The buffer contains invalid data
    fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < Self::SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer too small for header",
            )));
        }

        Ok(Self {
            crc: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            timestamp: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            key_len: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            value_size: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        })
    }
}

/// A command to append a key-value pair to the log.
#[derive(Debug)]
struct CommandSet {
    crc: u32,
    timestamp: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// A command to remove a key from the database.
#[derive(Debug)]
struct CommandRemove {
    crc: u32,
    timestamp: u64,
    key: Vec<u8>,
}

impl CommandSet {
    /// Creates a new set command.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * System time operations fail (`Error::TimestampError`)
    /// * Timestamp conversion fails (`Error::TimestampOverflow`)
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
    /// # Parameters
    ///
    /// * `buffer` - The buffer to write the serialized command to
    ///
    /// # Errors
    ///
    /// Returns an error if IO operations fail (`Error::Io`)
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        CommandHeader::new(
            self.crc,
            self.timestamp,
            self.key.len() as u32,
            self.value.len() as u32,
        )
        .serialize(buffer)?;
        buffer.write_all(&self.key)?;
        buffer.write_all(&self.value)?;
        Ok(())
    }
}

impl CommandRemove {
    /// Creates a new remove command.
    ///
    /// # Parameters
    ///
    /// * `key` - The key to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * System time operations fail (`Error::TimestampError`)
    /// * Timestamp conversion fails (`Error::TimestampOverflow`)
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
    /// # Parameters
    ///
    /// * `buffer` - The buffer to write the serialized command to
    ///
    /// # Errors
    ///
    /// Returns an error if IO operations fail (`Error::Io`)
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        CommandHeader::new(self.crc, self.timestamp, self.key.len() as u32, 0).serialize(buffer)?;
        buffer.write_all(&self.key)?;
        Ok(())
    }
}

/// Constructs the path for an active log file.
///
/// # Parameters
///
/// * `path` - Base directory path
/// * `timestamp` - Timestamp used as file identifier
///
/// # Returns
///
/// Returns a `PathBuf` containing the full path to the active log file
fn file_active_log_path(path: impl AsRef<Path>, timestamp: u64) -> PathBuf {
    path.as_ref().join(format!("{}.active.log", timestamp))
}

/// Constructs the path for a regular log file.
///
/// # Parameters
///
/// * `path` - Base directory path
/// * `timestamp` - Timestamp used as file identifier
///
/// # Returns
///
/// Returns a `PathBuf` containing the full path to the log file
fn file_log_path(path: impl AsRef<Path>, timestamp: u64) -> PathBuf {
    path.as_ref().join(format!("{}.log", timestamp))
}

/// Gets current timestamp as milliseconds since UNIX epoch.
///
/// # Errors
///
/// Returns an error if:
/// * System time operations fail (`Error::TimestampError`)
/// * Milliseconds value doesn't fit in u64 (`Error::TimestampOverflow`)
fn timestamp_as_u64() -> Result<u64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(Error::TimestampError)?
        .as_millis()
        .try_into()
        .map_err(Error::TimestampOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_command_serialization() {
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let command = CommandSet::new(key.clone(), value.clone()).unwrap();

        let mut buffer = Vec::new();
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

        let mut buffer = Vec::new();
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
}
