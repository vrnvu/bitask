use bitask;

#[test]
fn test_exclusive_once() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let _db = bitask::db::Bitask::exclusive(temp.path())?;
    Ok(())
}

#[test]
fn test_shared_without_exclusive_fails() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    match bitask::db::Bitask::shared(temp.path()) {
        Err(bitask::db::Error::Io(e)) => {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
            Ok(())
        }
        Ok(_) => panic!("Expected second open to fail with lock error"),
        Err(e) => panic!("Expected WriterLock error, got: {}", e),
    }
}

#[test]
fn test_exclusive_twice() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let _db = bitask::db::Bitask::exclusive(temp.path())?;
    match bitask::db::Bitask::exclusive(temp.path()) {
        Err(bitask::db::Error::WriterLock) => Ok(()),
        Ok(_) => panic!("Expected second open to fail with lock error"),
        Err(e) => panic!("Expected WriterLock error, got: {}", e),
    }
}

#[test]
fn test_shared_twice() -> anyhow::Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let _exclusive = bitask::db::Bitask::exclusive(temp.path())?;
    let _shared1 = bitask::db::Bitask::shared(temp.path())?;
    let _shared2 = bitask::db::Bitask::shared(temp.path())?;
    Ok(())
}
