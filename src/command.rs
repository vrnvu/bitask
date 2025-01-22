use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::db;

/// Bitask CLI
#[derive(Parser, Debug)]
#[clap(
    version = env!("CARGO_PKG_VERSION"),
    author = "Arnau Diaz <arnaudiaz@duck.com>",
    about = "Bitask is a Rust implementation of Bitcask, a log-structured key-value store optimized for high-performance reads and writes."
)]
pub struct Bitask {
    /// Sets logging to "debug" level, defaults to "info"
    #[clap(short, long, global = true)]
    pub verbose: bool,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Get a value from the store
    ///
    /// Returns an error if the key doesn't exist
    Ask {
        /// The key to look up
        #[clap(long)]
        key: String,
    },
    /// Put a value into the store
    ///
    /// Creates a new entry or updates an existing one
    Put {
        /// The key to store
        #[clap(long)]
        key: String,

        /// The value to store
        #[clap(long)]
        value: String,
    },
    /// Remove a key from the store
    ///
    /// Returns success even if the key doesn't exist
    Remove {
        /// The key to remove
        #[clap(long)]
        key: String,
    },
    /// Compact the database
    ///
    /// Merges multiple log files into one and removes deleted entries
    Compact,
}

impl Bitask {
    pub fn exec(self) -> anyhow::Result<()> {
        if self.verbose {
            env::set_var("RUST_LOG", "debug")
        } else {
            env::set_var("RUST_LOG", "info")
        }
        env_logger::init();

        let db_path = env::var("BITASK_PATH")
            .map(PathBuf::from)
            .map_err(|_| anyhow::anyhow!("BITASK_PATH environment variable is required"))?;

        let mut db = db::Bitask::open(&db_path)?;

        match self.command {
            Command::Ask { key } => {
                let value = db.ask(key.as_bytes())?;
                println!("{}", String::from_utf8_lossy(&value));
            }
            Command::Put { key, value } => {
                db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec())?;
            }
            Command::Compact => {
                db.compact()?;
            }
            Command::Remove { key } => {
                db.remove(key.as_bytes().to_vec())?;
            }
        }

        Ok(())
    }
}
