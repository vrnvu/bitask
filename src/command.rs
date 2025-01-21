use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::db;

/// bitask cli
#[derive(Parser, Debug)]
#[clap(
    version = "0.1.0",
    author = "Arnau Diaz <arnaudiaz@duck.com>",
    about = "Bitask is a simple key-value store written in Rust."
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
    /// Ask for a value from the store
    Ask {
        /// sets the key to ask for
        #[clap(long)]
        key: String,
    },
    /// Put a value into the store
    Put {
        /// sets the key to put
        #[clap(long)]
        key: String,

        /// sets the value to put
        #[clap(long)]
        value: String,
    },
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
                println!("Value stored successfully");
            }
        }

        Ok(())
    }
}
