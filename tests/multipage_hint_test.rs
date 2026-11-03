//! Repeated sorted upserts must not append into a non-rightmost leaf.
use bbolt::Db;
use std::collections::BTreeMap;
fn hex(bytes: &str) -> Vec<u8> {
    bytes
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
// Fixture: SHA256 of the first 512 sorted gate/{0..9}/{0..209} keys;
// values contain the original key plus its stable checksummed record. No hash
// implementation or external process is required to run the native KV regression.
fn rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    include_str!("fixtures/sorted_hash_growth.txt")
        .lines()
        .map(|line| {
            let (key, value) = line.split_once(' ').unwrap();
            (hex(key), hex(value))
        })
        .collect()
}
fn verify(db: &Db, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    db.view(|tx| {
        let bucket = tx.bucket(b"rows")?.unwrap();
        for (key, value) in expected {
            assert_eq!(
                bucket.get(key)?.as_ref(),
                Some(value),
                "missing/changed old key after growth count={}",
                expected.len()
            );
        }
        let mut scan = BTreeMap::new();
        bucket.for_each(|key, value| {
            assert!(
                scan.insert(key.to_vec(), value.unwrap().to_vec()).is_none(),
                "duplicate scan key"
            );
            Ok(())
        })?;
        assert_eq!(&scan, expected);
        assert!(tx.check().is_empty());
        Ok(())
    })
    .unwrap();
}
#[test]
fn sorted_hash_growth_preserves_skipped_existing_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.bbolt");
    let mut db = Db::open(&path, 0o600, None).unwrap();
    db.update(|tx| {
        tx.create_bucket(b"rows")?;
        Ok(())
    })
    .unwrap();
    let full = rows();
    for count in [100, 256, 512] {
        let expected = full.iter().take(count).cloned().collect::<BTreeMap<_, _>>();
        db.update(|tx| {
            let bucket = tx.bucket(b"rows")?.unwrap();
            for (key, value) in &expected {
                if bucket.get(key)?.as_deref() != Some(value) {
                    bucket.put(key, value)?;
                }
            }
            Ok(())
        })
        .unwrap();
        verify(&db, &expected);
        drop(db);
        db = Db::open(&path, 0o600, None).unwrap();
        verify(&db, &expected);
    }
}

#[test]
fn randomized_sorted_upserts_deletes_scans_and_reopen_match_mirror() {
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};
    for seed in [1, 17, 999] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("random.bbolt");
        let mut db = Db::open(&path, 0o600, None).unwrap();
        db.update(|tx| {
            tx.create_bucket(b"rows")?;
            Ok(())
        })
        .unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let pool = (0..2048)
            .map(|_| {
                let mut key = vec![0; 32];
                rng.fill_bytes(&mut key);
                key
            })
            .collect::<Vec<_>>();
        let mut expected = BTreeMap::new();
        for round in 0..18 {
            let count = ((round + 1) * 128).min(pool.len());
            let mut removed = Vec::new();
            for _ in 0..8 {
                let key = &pool[rng.next_u32() as usize % count];
                expected.remove(key);
                removed.push(key.clone());
            }
            for (index, key) in pool.iter().take(count).enumerate() {
                if !expected.contains_key(key) || index % 11 == round % 11 {
                    let size = if index % 13 == 0 { 1800 } else { 64 };
                    expected.insert(key.clone(), vec![(round + index) as u8; size]);
                }
            }
            db.update(|tx| {
                let bucket = tx.bucket(b"rows")?.unwrap();
                for key in &removed {
                    bucket.delete(key)?;
                }
                for (key, value) in &expected {
                    if bucket.get(key)?.as_deref() != Some(value) {
                        bucket.put(key, value)?;
                    }
                }
                Ok(())
            })
            .unwrap();
            verify(&db, &expected);
            drop(db);
            db = Db::open(&path, 0o600, None).unwrap();
            verify(&db, &expected);
        }
    }
}

mod common;
#[test]
fn go_can_check_and_compact_repeated_sorted_growth_without_losing_keys() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("rust.bbolt");
    let destination = dir.path().join("go-compacted.bbolt");
    let db = Db::open(&source, 0o600, None).unwrap();
    db.update(|tx| {
        tx.create_bucket(b"rows")?;
        Ok(())
    })
    .unwrap();
    let full = rows();
    let mut expected = BTreeMap::new();
    for count in [100, 256, 512] {
        expected = full.iter().take(count).cloned().collect();
        db.update(|tx| {
            let bucket = tx.bucket(b"rows")?.unwrap();
            for (key, value) in &expected {
                if bucket.get(key)?.as_deref() != Some(value) {
                    bucket.put(key, value)?;
                }
            }
            Ok(())
        })
        .unwrap();
    }
    verify(&db, &expected);
    db.close().unwrap();
    drop(db);
    common::go_oracle::go_check(&source, "array");
    common::go_oracle::go_compact(&source, &destination, 4096, "array");
    common::go_oracle::go_check(&destination, "array");
    let db = Db::open(&destination, 0o600, None).unwrap();
    verify(&db, &expected);
}
