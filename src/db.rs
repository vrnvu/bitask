use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid log file name '{filename}'. Expected format: 'timestamp.log' or 'timestamp.active.log'")]
    InvalidLogFileName { filename: String },

    #[error("Failed to parse timestamp '{value}' from log filename: {source}")]
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
    #[error("Timestamp overflow, converting u64 to u32: {0}")]
    TimestampOverflow(#[from] std::num::TryFromIntError),
    #[error("Active file not found in non empty path")]
    ActiveFileNotFound,
}

/// A Bitask database.
///
/// Only one instance can have write access at a time across all processes and threads.
/// The locking is handled at the OS level through file system locks.
pub struct Bitask {
    path: PathBuf,
    file_lock: File,

    writer_id: u32,
    writer: BufWriter<File>,

    readers: HashMap<u32, BufReader<File>>,

    keydir: BTreeMap<Vec<u8>, KeyDirEntry>,
}

struct KeyDirEntry {
    file_id: u32,
    value_size: u32,
    value_position: u64,
    timestamp: u32,
}

impl Bitask {
    /// Open a Bitask database for exclusive writing and reading.
    /// Only one writer is allowed at a time across all processes and threads.
    /// This will create the database if it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        fs::create_dir_all(&path)?;
        let lock_path = path.as_ref().join("db.lock");

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
            Some(Ok(entry)) if entry.file_name() == "db.lock" => {
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

    fn open_new(path: impl AsRef<Path>, lock_file: File) -> Result<Self, Error> {
        let timestamp = timestamp_as_u32()?;

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
            file_lock: lock_file,
            writer_id: timestamp,
            writer,
            readers,
            keydir: BTreeMap::new(),
        })
    }

    fn open_existing(path: impl AsRef<Path>, lock_file: File) -> Result<Self, Error> {
        let mut active_timestamp = None;
        let mut active_file = None;
        let mut files: BTreeMap<u32, PathBuf> = BTreeMap::new();

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "db.lock" {
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
            file_lock: lock_file,
            writer_id: active_timestamp,
            writer,
            readers,
            keydir,
        })
    }

    fn rebuild_keydir(
        reader: &mut BufReader<File>,
        file_id: u32,
    ) -> Result<BTreeMap<Vec<u8>, KeyDirEntry>, Error> {
        let mut keydir = BTreeMap::new();
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
                let value_position = position + CommandHeader::SIZE as u64 + header.key_len as u64;
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

            position +=
                CommandHeader::SIZE as u64 + header.key_len as u64 + header.value_size as u64;
        }
        Ok(keydir)
    }

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

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if value.is_empty() {
            return Err(Error::InvalidEmptyValue);
        }

        let command = Command::set(key.clone(), value.clone())?;
        let mut buffer = Vec::new();
        command.serialize(&mut buffer)?;

        // TODO optimize writer to store last offset/position
        let position = self.writer.seek(SeekFrom::End(0))?;
        self.writer.write_all(&buffer)?;
        self.writer.flush()?;

        // 4(crc) + 4(timestamp) + 4(keylen) + 4(valuelen) + keylen
        let value_position = position + 16 + key.len() as u64;
        self.keydir.insert(
            key,
            KeyDirEntry {
                file_id: self.writer_id,
                value_size: value.len() as u32,
                value_position,
                timestamp: command.timestamp(),
            },
        );
        Ok(())
    }

    pub fn remove(&mut self, key: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        let command = Command::remove(key.clone())?;
        let mut buffer = Vec::new();
        command.serialize(&mut buffer)?;

        self.writer.write_all(&buffer)?;
        self.writer.flush()?;

        self.keydir.remove(&key);
        Ok(())
    }
}

#[derive(Debug)]
struct CommandHeader {
    crc: u32,
    timestamp: u32,
    key_len: u32,
    value_size: u32,
}

impl CommandHeader {
    const SIZE: usize = 16; // 4 bytes * 4 fields

    fn new(crc: u32, timestamp: u32, key_len: u32, value_len: u32) -> Self {
        Self {
            crc,
            timestamp,
            key_len,
            value_size: value_len,
        }
    }

    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        buffer.write_all(&self.crc.to_le_bytes())?;
        buffer.write_all(&self.timestamp.to_le_bytes())?;
        buffer.write_all(&self.key_len.to_le_bytes())?;
        buffer.write_all(&self.value_size.to_le_bytes())?;
        Ok(())
    }

    fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        if buf.len() < Self::SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "buffer too small for header",
            )));
        }

        Ok(Self {
            crc: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            timestamp: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            key_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            value_size: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
        })
    }
}

/// A command to be executed on the database.
/// Each command is serialized and appended to the active file WAL.
enum Command {
    /// Append a key-value pair.
    /// value_size must be greater than 0.
    Set {
        crc: u32,
        timestamp: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Remove a key.
    /// If value_size is 0, the key is removed from the keydir.
    Remove {
        crc: u32,
        timestamp: u32,
        key: Vec<u8>,
    },
}

impl Command {
    /// Get the timestamp of the command.
    pub fn timestamp(&self) -> u32 {
        match self {
            Self::Set { timestamp, .. } => *timestamp,
            Self::Remove { timestamp, .. } => *timestamp,
        }
    }

    /// Create a new set command.
    pub fn set(key: Vec<u8>, value: Vec<u8>) -> Result<Self, Error> {
        let timestamp = timestamp_as_u32()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        hasher.update(value.as_slice());
        let crc = hasher.finalize();

        Ok(Self::Set {
            crc,
            timestamp,
            key,
            value,
        })
    }

    /// Create a new remove command.
    pub fn remove(key: Vec<u8>) -> Result<Self, Error> {
        let timestamp = timestamp_as_u32()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        let crc = hasher.finalize();

        Ok(Self::Remove {
            crc,
            timestamp,
            key,
        })
    }

    /// Serialize the command into a byte array.
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            Self::Set {
                crc,
                timestamp,
                key,
                value,
            } => {
                let header =
                    CommandHeader::new(*crc, *timestamp, key.len() as u32, value.len() as u32);
                header.serialize(buffer)?;
                buffer.write_all(key)?;
                buffer.write_all(value)?;
            }
            Self::Remove {
                crc,
                timestamp,
                key,
            } => {
                let header = CommandHeader::new(*crc, *timestamp, key.len() as u32, 0);
                header.serialize(buffer)?;
                buffer.write_all(key)?;
            }
        }
        Ok(())
    }

    /// Deserialize the command from a byte array.
    fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        let header = CommandHeader::deserialize(buf)?;
        let key_start = CommandHeader::SIZE;
        let key_end = key_start + header.key_len as usize;

        let key = buf[key_start..key_end].to_vec();

        if header.value_size == 0 {
            Ok(Self::Remove {
                crc: header.crc,
                timestamp: header.timestamp,
                key,
            })
        } else {
            let value_end = key_end + header.value_size as usize;
            let value = buf[key_end..value_end].to_vec();

            Ok(Self::Set {
                crc: header.crc,
                timestamp: header.timestamp,
                key,
                value,
            })
        }
    }
}

fn file_active_log_path(path: impl AsRef<Path>, timestamp: u32) -> PathBuf {
    path.as_ref().join(format!("{}.active.log", timestamp))
}

fn file_log_path(path: impl AsRef<Path>, timestamp: u32) -> PathBuf {
    path.as_ref().join(format!("{}.log", timestamp))
}

fn timestamp_as_u32() -> Result<u32, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(Error::TimestampError)?
        .as_secs()
        .try_into()
        .map_err(Error::TimestampOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_serialization() {
        let command = Command::set(b"key".to_vec(), b"value".to_vec()).unwrap();

        // Extract original values before serialization
        let Command::Set {
            crc,
            timestamp,
            ref key,
            ref value,
        } = command
        else {
            panic!("Expected Set command");
        };

        // Serialize and deserialize
        let mut buffer = Vec::new();
        command.serialize(&mut buffer).unwrap();
        let deserialized = Command::deserialize(&buffer).unwrap();

        // Compare with deserialized
        match deserialized {
            Command::Set {
                crc: crc2,
                timestamp: ts2,
                key: key2,
                value: value2,
            } => {
                assert_eq!(crc, crc2);
                assert_eq!(timestamp, ts2);
                assert_eq!(*key, key2);
                assert_eq!(*value, value2);

                // Verify CRC is correct
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(key);
                hasher.update(value);
                assert_eq!(crc, hasher.finalize());
            }
            Command::Remove { .. } => panic!("Expected Set command"),
        }
    }

    #[test]
    fn test_remove_serialization() {
        let command = Command::remove(b"key".to_vec()).unwrap();

        // Extract original values
        let Command::Remove {
            crc,
            timestamp,
            ref key,
        } = command
        else {
            panic!("Expected Remove command");
        };

        // Serialize and deserialize
        let mut buffer = Vec::new();
        command.serialize(&mut buffer).unwrap();
        let deserialized = Command::deserialize(&buffer).unwrap();

        // Compare with deserialized
        match deserialized {
            Command::Remove {
                crc: crc2,
                timestamp: ts2,
                key: key2,
            } => {
                assert_eq!(crc, crc2);
                assert_eq!(timestamp, ts2);
                assert_eq!(*key, key2);

                // Verify CRC is correct
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(key);
                assert_eq!(crc, hasher.finalize());
            }
            Command::Set { .. } => panic!("Expected Remove command"),
        }
    }
}
