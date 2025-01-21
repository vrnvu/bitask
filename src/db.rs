use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
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
}

/// A Bitask database.
///
/// Only one instance can have write access at a time across all processes and threads.
/// The locking is handled at the OS level through file system locks.
pub struct Bitask {
    path: PathBuf,
    file_lock: File,
    writer: BufWriter<File>,
    readers: HashMap<u32, BufReader<File>>,
    keydir: BTreeMap<Vec<u8>, KeyDirEntry>,
}

struct KeyDirEntry {
    file_id: u32,
    value_size: u32,
    value_position: u64,
    timestamp: u64,
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

        let active_file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .append(true)
            .open(path.as_ref().join("0.log"))?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file_lock: lock_file,
            writer: BufWriter::new(active_file),
            readers: HashMap::new(),
            keydir: BTreeMap::new(),
        })
    }

    pub fn ask(&mut self, key: &[u8]) -> Result<Vec<u8>, Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if let Some(entry) = self.keydir.get(key) {
            if !self.readers.contains_key(&entry.file_id) {
                let file = OpenOptions::new()
                    .read(true)
                    .open(self.path.join(format!("{}.log", entry.file_id)))?;
                self.readers.insert(entry.file_id, BufReader::new(file));
            }

            let reader = self
                .readers
                .get_mut(&entry.file_id)
                .ok_or(Error::FileNotFound(format!("{}.log", entry.file_id)))?;

            reader.seek(SeekFrom::Start(entry.value_position))?;
            // TODO buffer pool
            let mut value = vec![0u8; entry.value_size as usize];
            reader.read_exact(&mut value)?;
            return Ok(value);
        }

        return Err(Error::KeyNotFound);
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        if key.is_empty() {
            return Err(Error::InvalidEmptyKey);
        }

        if value.is_empty() {
            return Err(Error::InvalidEmptyValue);
        }

        let command = Command::set(key.clone(), value.clone())?;
        // TODO buffer pool
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
                file_id: 0,
                value_size: value.len() as u32,
                value_position,
                timestamp: 0,
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
        value: Vec<u8>,
    },
}

impl Command {
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
            value: vec![],
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
                buffer.write_all(&crc.to_le_bytes())?;
                buffer.write_all(&timestamp.to_le_bytes())?;
                buffer.write_all(&(key.len() as u32).to_le_bytes())?;
                buffer.write_all(&(value.len() as u32).to_le_bytes())?;
                buffer.write_all(key.as_slice())?;
                buffer.write_all(value.as_slice())?;
            }
            Self::Remove {
                timestamp,
                crc,
                key,
                value,
            } => {
                buffer.write_all(&crc.to_le_bytes())?;
                buffer.write_all(&timestamp.to_le_bytes())?;
                buffer.write_all(&(key.len() as u32).to_le_bytes())?;
                buffer.write_all(&(value.len() as u32).to_le_bytes())?;
                buffer.write_all(key.as_slice())?;
                buffer.write_all(value.as_slice())?;
            }
        }
        Ok(())
    }

    /// Deserialize the command from a byte array.
    fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        let mut reader = Cursor::new(buf);

        let mut crc_buffer = [0u8; 4];
        reader.read_exact(&mut crc_buffer)?;
        let crc = u32::from_le_bytes(crc_buffer);

        let mut timestamp_buffer = [0u8; 4];
        reader.read_exact(&mut timestamp_buffer)?;
        let timestamp = u32::from_le_bytes(timestamp_buffer);

        let mut key_buffer = [0u8; 4];
        reader.read_exact(&mut key_buffer)?;
        let key_size = u32::from_le_bytes(key_buffer);

        let mut value_buffer = [0u8; 4];
        reader.read_exact(&mut value_buffer)?;
        let value_size = u32::from_le_bytes(value_buffer);

        let mut key = vec![0u8; key_size as usize];
        reader.read_exact(&mut key)?;

        if value_size == 0 {
            Ok(Self::Remove {
                timestamp,
                crc,
                key,
                value: vec![],
            })
        } else {
            let mut value = vec![0u8; value_size as usize];
            reader.read_exact(&mut value)?;

            Ok(Self::Set {
                timestamp,
                crc,
                key,
                value,
            })
        }
    }
}

fn timestamp_as_u32() -> Result<u32, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::TimestampError(e))?
        .as_secs()
        .try_into()
        .map_err(|e| Error::TimestampOverflow(e))
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
                hasher.update(&key);
                hasher.update(&value);
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
            ref value,
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
                value: value2,
            } => {
                assert_eq!(crc, crc2);
                assert_eq!(timestamp, ts2);
                assert_eq!(*key, key2);
                assert_eq!(*value, value2);

                // Verify CRC is correct
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&key);
                assert_eq!(crc, hasher.finalize());
            }
            Command::Set { .. } => panic!("Expected Remove command"),
        }
    }
}
