use tempfile::tempdir;

#[test]
fn test_basic_db_operations() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let mut db = bitask::db::Bitask::open(temp_dir.path())?;

    // Test put and get
    let key = b"test_key".to_vec();
    let value = b"test_value".to_vec();
    db.put(key.clone(), value.clone())?;

    let retrieved = db.ask(&key)?;
    assert_eq!(retrieved, value);

    // Test remove
    db.remove(key.clone())?;
    assert!(matches!(db.ask(&key), Err(bitask::db::Error::KeyNotFound)));

    Ok(())
}

#[test]
fn test_open_once() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let _db = bitask::db::Bitask::open(temp.path())?;
    Ok(())
}

#[test]
fn test_open_twice() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let _db = bitask::db::Bitask::open(temp.path())?;
    match bitask::db::Bitask::open(temp.path()) {
        Err(bitask::db::Error::WriterLock) => Ok(()),
        Ok(_) => panic!("Expected second open to fail with lock error"),
        Err(e) => panic!("Expected WriterLock error, got: {}", e),
    }
}

#[test]
fn test_ask_key_not_found() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    let value = db.ask(b"key");
    assert!(value.is_err());
    assert!(matches!(
        value.err().unwrap(),
        bitask::db::Error::KeyNotFound
    ));
    Ok(())
}

#[test]
fn test_put_get() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    db.put(b"key1".to_vec(), b"value1".to_vec())?;
    db.put(b"key2".to_vec(), b"value2".to_vec())?;

    let value = db.ask(b"key1")?;
    assert_eq!(value, b"value1");

    let value = db.ask(b"key2")?;
    assert_eq!(value, b"value2");
    Ok(())
}

#[test]
fn test_put_overwrite_and_get() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    db.put(b"key1".to_vec(), b"value1".to_vec())?;

    let value = db.ask(b"key1")?;
    assert_eq!(value, b"value1");

    db.put(b"key1".to_vec(), b"value2".to_vec())?;
    let value = db.ask(b"key1")?;
    assert_eq!(value, b"value2");
    Ok(())
}

#[test]
fn test_remove() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    db.put(b"key1".to_vec(), b"value1".to_vec())?;
    db.remove(b"key1".to_vec())?;
    let value = db.ask(b"key1");
    assert!(value.is_err());
    assert!(matches!(
        value.err().unwrap(),
        bitask::db::Error::KeyNotFound
    ));
    Ok(())
}

#[test]
fn test_invalid_empty_key_and_empty_value() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    let error = db.put(vec![], vec![]);
    assert!(error.is_err());
    assert!(matches!(
        error.err().unwrap(),
        bitask::db::Error::InvalidEmptyKey
    ));

    let error = db.ask(&[]);
    assert!(error.is_err());
    assert!(matches!(
        error.err().unwrap(),
        bitask::db::Error::InvalidEmptyKey
    ));

    let error = db.remove(vec![]);
    assert!(error.is_err());
    assert!(matches!(
        error.err().unwrap(),
        bitask::db::Error::InvalidEmptyKey
    ));
    Ok(())
}

#[test]
fn test_rebuild_keydir_on_open() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut db = bitask::db::Bitask::open(temp.path())?;

    // Write some data
    db.put(b"key1".to_vec(), b"value1".to_vec())?;
    db.put(b"key2".to_vec(), b"value2".to_vec())?;

    // Drop the database to close it
    drop(db);

    // Reopen and verify data persists
    let mut db = bitask::db::Bitask::open(temp.path())?;
    let value = db.ask(b"key1")?;
    assert_eq!(value, b"value1");

    let value = db.ask(b"key2")?;
    assert_eq!(value, b"value2");

    Ok(())
}

#[test]
fn test_rebuild_keydir_on_open_with_remove() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    db.put(b"key1".to_vec(), b"value1".to_vec())?;
    db.put(b"key2".to_vec(), b"value2".to_vec())?;
    db.remove(b"key1".to_vec())?;
    drop(db);

    let mut db = bitask::db::Bitask::open(temp.path())?;
    let value = db.ask(b"key1");
    assert!(value.is_err());
    assert!(matches!(
        value.err().unwrap(),
        bitask::db::Error::KeyNotFound
    ));

    let value = db.ask(b"key2")?;
    assert_eq!(value, b"value2");
    Ok(())
}

#[test]
fn test_multiple_operations_sequence() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;

    // Insert multiple key-value pairs
    for i in 0..100 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("value{}", i).into_bytes();
        db.put(key, value)?;
    }

    // Verify all values
    for i in 0..100 {
        let key = format!("key{}", i).into_bytes();
        let expected = format!("value{}", i).into_bytes();
        let value = db.ask(&key)?;
        assert_eq!(value, expected);
    }

    // Remove every other key
    for i in (0..100).step_by(2) {
        let key = format!("key{}", i).into_bytes();
        db.remove(key)?;
    }

    // Verify remaining and removed keys
    for i in 0..100 {
        let key = format!("key{}", i).into_bytes();
        let result = db.ask(&key);
        if i % 2 == 0 {
            assert!(matches!(
                result.err().unwrap(),
                bitask::db::Error::KeyNotFound
            ));
        } else {
            assert_eq!(result?, format!("value{}", i).into_bytes());
        }
    }

    Ok(())
}

#[test]
fn test_log_rotation() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;

    // Create a 4MiB value (just over the MAX_ACTIVE_FILE_SIZE)
    let key = b"large_key".to_vec();
    let value = vec![42u8; 4 * 1024 * 1024];

    // First write should create initial file
    db.put(key.clone(), value.clone())?;

    // Second write should trigger rotation
    let key2 = b"large_key2".to_vec();
    db.put(key2.clone(), value.clone())?;

    // Verify both values are readable
    assert_eq!(db.ask(&key)?, value);
    assert_eq!(db.ask(&key2)?, value);

    // Verify we have two files in the directory
    let file_count = std::fs::read_dir(temp.path())?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == "log")
                .unwrap_or(false)
        })
        .count();

    assert_eq!(file_count, 2);

    Ok(())
}
