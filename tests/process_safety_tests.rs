use std::process::Command;
use std::{thread, time};
use tempfile::tempdir;

#[test]
fn test_concurrent_processes_access() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // First process writes data
    let output = Command::new("cargo")
        .args(&["run", "--", "put", "--key", "foo", "--value", "bar"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;
    assert!(output.status.success());

    // Second process reads data
    let output = Command::new("cargo")
        .args(&["run", "--", "ask", "--key", "foo"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;

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
    let output = Command::new("cargo")
        .args(&["run", "--", "put", "--key", "foo", "--value", "bar"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;

    assert!(!output.status.success());

    Ok(())
}

#[test]
fn test_process_recovery_after_crash() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path();

    // Write initial data
    let output = Command::new("cargo")
        .args(&["run", "--", "put", "--key", "foo", "--value", "bar"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;
    assert!(output.status.success());

    // Start a long-running CLI operation that we'll interrupt
    let mut child = Command::new("cargo")
        .args(&["run", "--", "put", "--key", "foo2", "--value", "bar2"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .spawn()?;

    // Give it time to start but kill before completion
    thread::sleep(time::Duration::from_millis(100));
    child.kill()?;

    // Database should still be readable
    let output = Command::new("cargo")
        .args(&["run", "--", "ask", "--key", "foo"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;

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
    let output = Command::new("cargo")
        .args(&["run", "--", "put", "--key", "foo", "--value", "bar"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;

    assert!(output.status.success());

    // Verify lock file is gone after process exits
    assert!(
        !lock_path.exists(),
        "Lock file still exists after process exit"
    );

    // Try reading the value back
    let output = Command::new("cargo")
        .args(&["run", "--", "ask", "--key", "foo"])
        .env("BITASK_PATH", db_path.to_str().unwrap())
        .output()?;

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
