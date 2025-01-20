use std::{
    fs::{self, File, OpenOptions},
    io::BufReader,
    path::Path,
};

use fs2::FileExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Only one writer allowed at a time")]
    WriterLock,
}

/// A Bitask database mode - either exclusive (read-write) or shared (read-only)
pub enum Mode {
    /// Exclusive mode. Holds a lock file.
    Exclusive(File),
    Shared,
}

/// A Bitask database.
///
/// Only one instance can have write access at a time across all processes and threads.
/// The locking is handled at the OS level through file system locks.
pub struct Bitask {
    mode: Mode,
    active_file: BufReader<File>,
}

impl Bitask {
    /// Open a Bitask database for exclusive writing and reading.
    /// Only one writer is allowed at a time across all processes and threads.
    /// This will create the database if it doesn't exist.
    pub fn exclusive(path: impl AsRef<Path>) -> Result<Self, Error> {
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
            .write(true)
            .truncate(false)
            .append(true)
            .open(path.as_ref().join("0.log"))?;

        Ok(Self {
            mode: Mode::Exclusive(lock_file),
            active_file: BufReader::new(active_file),
        })
    }

    /// Open a Bitask database for shared reading.
    /// Multiple readers are allowed concurrently with a single writer.
    pub fn shared(path: impl AsRef<Path>) -> Result<Self, Error> {
        let active_file = OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .truncate(false)
            .append(false)
            .open(path.as_ref().join("0.log"))
            .map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File {} not found", path.as_ref().join("0.log").display()),
                ))
            })?;

        Ok(Self {
            mode: Mode::Shared,
            active_file: BufReader::new(active_file),
        })
    }
}
