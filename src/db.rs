use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Cursor, Read, Write},
    path::Path,
}; // Add this import

use fs2::FileExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Only one writer allowed at a time")]
    WriterLock,
    #[error("Key not found")]
    KeyNotFound,
}

/// A Bitask database.
///
/// Only one instance can have write access at a time across all processes and threads.
/// The locking is handled at the OS level through file system locks.
pub struct Bitask {
    file_lock: File,
    active_file: BufReader<File>,
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
            file_lock: lock_file,
            active_file: BufReader::new(active_file),
        })
    }

    pub fn ask(&self, _key: String) -> Result<String, Error> {
        Err(Error::KeyNotFound)
    }

    pub fn put(&mut self, _key: String, _value: String) -> Result<(), Error> {
        unimplemented!();
    }
}

/// A command to be executed on the database.
/// Each command is serialized and appended to the active file WAL.
enum Command {
    /// Append a key-value pair.
    /// value_size must be greater than 0.
    Set {
        timestamp: u64,
        crc: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Remove a key.
    /// If value_size is 0, the key is removed from the keydir.
    Remove {
        timestamp: u64,
        crc: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

impl Command {
    /// Create a new set command.
    pub fn set(key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        hasher.update(value.as_slice());
        let crc = hasher.finalize();

        Self::Set {
            timestamp,
            crc,
            key,
            value,
        }
    }

    /// Create a new remove command.
    pub fn remove(key: Vec<u8>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_slice());
        let crc = hasher.finalize();

        Self::Remove {
            timestamp,
            crc,
            key,
            value: vec![],
        }
    }

    /// Serialize the command into a byte array.
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            Self::Set {
                timestamp,
                crc,
                key,
                value,
            } => {
                buffer.write_all(&timestamp.to_le_bytes())?;
                buffer.write_all(&crc.to_le_bytes())?;
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
                buffer.write_all(&timestamp.to_le_bytes())?;
                buffer.write_all(&crc.to_le_bytes())?;
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

        let mut timestamp_buffer = [0u8; 8];
        reader.read_exact(&mut timestamp_buffer)?;
        let timestamp = u64::from_le_bytes(timestamp_buffer);

        let mut crc_buffer = [0u8; 4];
        reader.read_exact(&mut crc_buffer)?;
        let crc = u32::from_le_bytes(crc_buffer);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_serialization() {
        let command = Command::set(b"key".to_vec(), b"value".to_vec());

        // Extract original values before serialization
        let Command::Set {
            timestamp,
            crc,
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
                timestamp: ts2,
                crc: crc2,
                key: key2,
                value: value2,
            } => {
                assert_eq!(timestamp, ts2);
                assert_eq!(crc, crc2);
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
        let command = Command::remove(b"key".to_vec());

        // Extract original values
        let Command::Remove {
            timestamp,
            crc,
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
                timestamp: ts2,
                crc: crc2,
                key: key2,
                value: value2,
            } => {
                assert_eq!(timestamp, ts2);
                assert_eq!(crc, crc2);
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
