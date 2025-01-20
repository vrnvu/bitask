use std::env;

use clap::{Parser, Subcommand};

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

        match self.command {
            Command::Ask { key } => {
                println!("Asking for key: {}", key);
            }
            Command::Put { key, value } => {
                println!("Putting key: {} with value: {}", key, value);
            }
        }

        Ok(())
    }
}
