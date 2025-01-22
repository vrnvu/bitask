use std::path::Path;
use std::process::Command;
use std::{thread, time};
use tempfile::tempdir;

fn command_put(db_path: &Path, key: &str, value: &str) -> anyhow::Result<std::process::Output> {
    Command::new("cargo")
        .args(["run", "--", "put", "--key", key, "--value", value])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()
        .map_err(Into::into)
}

fn command_ask(db_path: &Path, key: &str) -> anyhow::Result<std::process::Output> {
    Command::new("cargo")
        .args(["run", "--", "ask", "--key", key])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()
        .map_err(Into::into)
}

fn command_remove(db_path: &Path, key: &str) -> anyhow::Result<std::process::Output> {
    Command::new("cargo")
        .args(["run", "--", "remove", "--key", key])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()
        .map_err(Into::into)
}

fn command_compact(db_path: &Path) -> anyhow::Result<std::process::Output> {
    Command::new("cargo")
        .args(["run", "--", "compact"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()
        .map_err(Into::into)
}

#[test]
fn test_concurrent_processes_access() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // First process writes data
    let output = command_put(db_path, "foo", "bar")?;
    assert!(output.status.success());

    // Second process reads data
    let output = command_ask(db_path, "foo")?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "bar");

    Ok(())
}

#[test]
fn test_process_lock_contention() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Hold the database open
    let _db = bitask::db::Bitask::open(db_path)?;

    // Try to write using CLI, should fail due to lock
    let output = command_put(db_path, "foo", "bar")?;
    assert!(!output.status.success());

    Ok(())
}

#[test]
fn test_process_recovery_after_crash() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Write initial data
    let output = command_put(db_path, "foo", "bar")?;
    assert!(output.status.success());

    // Start a long-running CLI operation that we'll interrupt
    let mut child = Command::new("cargo")
        .args(["run", "--", "put", "--key", "foo2", "--value", "bar2"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .spawn()?;

    // Give it time to start but kill before completion
    thread::sleep(time::Duration::from_millis(100));
    child.kill()?;

    // Database should still be readable
    let output = command_ask(db_path, "foo")?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "bar");

    Ok(())
}

#[test]
fn test_lock_file_cleanup() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();
    let lock_path = db_path.join("db.lock");

    // Run a put command
    let output = command_put(db_path, "foo", "bar")?;
    assert!(output.status.success());

    // Verify lock file is gone after process exits
    assert!(
        !lock_path.exists(),
        "Lock file still exists after process exit"
    );

    // Try reading the value back
    let output = command_ask(db_path, "foo")?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "bar");

    Ok(())
}

#[test]
fn test_sequential_opens() -> anyhow::Result<()> {
    let temp = tempdir()?;

    // First open
    {
        let _db = bitask::db::Bitask::open(temp.path())?;
        // _db is dropped here at end of scope, releasing the lock
    }

    // Second open should work because first handle was dropped
    {
        let _db = bitask::db::Bitask::open(temp.path())?;
    }

    Ok(())
}

#[test]
fn test_remove_key() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Write initial data
    let output = command_put(db_path, "foo", "bar")?;
    assert!(output.status.success());

    // Remove the key
    let output = command_remove(db_path, "foo")?;
    assert!(output.status.success());

    // Verify key is gone
    let output = command_ask(db_path, "foo")?;
    assert!(!output.status.success());

    Ok(())
}

#[test]
fn test_compact_empty_db() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Run compaction on empty DB
    let output = command_compact(db_path)?;
    assert!(output.status.success());

    // Verify we have only one active file
    let files = std::fs::read_dir(db_path)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            name.ends_with(".log")
        })
        .count();

    assert_eq!(files, 1, "Empty DB should have exactly one active file");

    Ok(())
}

#[test]
fn test_compact_single_file() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Write some data (but not enough for multiple files)
    let output = command_put(db_path, "foo", "bar")?;
    assert!(output.status.success());

    // Run compaction
    let output = command_compact(db_path)?;
    assert!(output.status.success());

    // Verify data is still accessible
    let output = command_ask(db_path, "foo")?;
    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "bar");

    Ok(())
}

#[test]
fn test_compaction_with_cli() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    println!("Starting test - creating test data");
    let value = "*".repeat(2 * 1024 * 1024); // 2MiB value

    // Use library directly to write data quickly
    let mut db = bitask::db::Bitask::open(db_path)?;

    // Write 6 large values (12MiB total, should create multiple files)
    println!("Writing 6 large values...");
    for i in 0..6 {
        db.put(format!("key{}", i).into_bytes(), value.as_bytes().to_vec())?;
    }

    // Delete all keys
    println!("Removing all keys...");
    for i in 0..6 {
        db.remove(format!("key{}", i).into_bytes())?;
    }

    // Drop the db handle before using CLI
    drop(db);

    // Count files before compaction
    let files_before = std::fs::read_dir(db_path)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            name.ends_with(".log") && !name.contains(".active.log")
        })
        .count();
    println!("Files before compaction: {}", files_before);
    assert!(
        files_before > 1,
        "Expected multiple files due to large values"
    );

    // Use CLI for compaction
    println!("Running compaction...");
    let output = command_compact(db_path)?;
    assert!(output.status.success());

    // Count files after compaction
    let files_after = std::fs::read_dir(db_path)?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            name.ends_with(".log") && !name.contains(".active.log")
        })
        .count();
    println!("Files after compaction: {}", files_after);

    assert_eq!(
        files_after, 1,
        "Expected single file after compacting all deleted data"
    );

    // Verify all keys are gone using library
    let mut db = bitask::db::Bitask::open(db_path)?;
    for i in 0..6 {
        let key = format!("key{}", i).into_bytes();
        assert!(matches!(db.ask(&key), Err(bitask::db::Error::KeyNotFound)));
    }

    println!("Test completed successfully");
    Ok(())
}
