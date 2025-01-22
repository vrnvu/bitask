#[cfg(test)]
mod tests {
    use bitask::db::Bitask;

    use super::*;

    #[test]
    fn benchmark_operations() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Bitask::open(dir.path()).unwrap();

        // Write operations
        for i in 0..10000 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            db.put(key, value).unwrap();
        }

        // Read operations
        for i in 0..10000 {
            let key = format!("key{}", i).into_bytes();
            let _value = db.ask(&key).unwrap();
        }

        // Update operations
        for i in 0..10000 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            db.put(key, value).unwrap();
        }

        // Remove operations
        for i in 0..10000 {
            let key = format!("key{}", i).into_bytes();
            db.remove(key).unwrap();
        }
    }
}
