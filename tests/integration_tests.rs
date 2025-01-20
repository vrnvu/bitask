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
    let db = bitask::db::Bitask::open(temp.path())?;
    let value = db.ask("key".to_string());
    assert!(value.is_err());
    assert!(matches!(
        value.err().unwrap(),
        bitask::db::Error::KeyNotFound
    ));
    Ok(())
}
