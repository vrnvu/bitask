use bitask;

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

    let error = db.ask(&vec![]);
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
fn test_rebuild_keydir_on_open() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let mut db = bitask::db::Bitask::open(temp.path())?;
    db.put(b"key1".to_vec(), b"value1".to_vec())?;
    db.put(b"key2".to_vec(), b"value2".to_vec())?;
    drop(db);

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
