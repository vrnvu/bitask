use bitask::Bitask;
use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli = Bitask::parse();
    cli.exec()?;
    Ok(())
}
