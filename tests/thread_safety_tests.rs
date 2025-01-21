use std::sync::{Arc, Mutex};
use std::thread;
use tempfile::tempdir;

#[test]
fn test_concurrent_reads() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db = bitask::db::Bitask::open(temp.path())?;
    let db = Arc::new(Mutex::new(db));

    // Setup test data
    db.lock()
        .unwrap()
        .put(b"key1".to_vec(), b"value1".to_vec())?;

    let mut handles = vec![];
    for _ in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let value = db_clone.lock().unwrap().ask(b"key1").unwrap();
            assert_eq!(value, b"value1");
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}

#[test]
fn test_concurrent_writes() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db = bitask::db::Bitask::open(temp.path())?;
    let db = Arc::new(Mutex::new(db));
    let mut handles = vec![];

    // Create multiple writer threads
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            db_clone.lock().unwrap().put(key, value).unwrap();
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes succeeded
    for i in 0..10 {
        let key = format!("key{}", i).into_bytes();
        let expected = format!("value{}", i).into_bytes();
        let value = db.lock().unwrap().ask(&key)?;
        assert_eq!(value, expected);
    }

    Ok(())
}

#[test]
fn test_concurrent_mixed_operations() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let db = bitask::db::Bitask::open(temp.path())?;
    let db = Arc::new(Mutex::new(db));
    let mut handles = vec![];

    // Setup initial data
    db.lock()
        .unwrap()
        .put(b"shared_key".to_vec(), b"initial_value".to_vec())?;

    // Create threads that do mixed operations
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            if i % 2 == 0 {
                // Even threads write
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                db_clone.lock().unwrap().put(key, value).unwrap();
            } else {
                // Odd threads read
                let _ = db_clone.lock().unwrap().ask(b"shared_key").unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify writes succeeded
    for i in (0..10).step_by(2) {
        let key = format!("key{}", i).into_bytes();
        let expected = format!("value{}", i).into_bytes();
        let value = db.lock().unwrap().ask(&key)?;
        assert_eq!(value, expected);
    }

    Ok(())
}
