//! Ports of upstream `manydbs_test.go`.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

static SEED: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

// Go: TestManyDBs — moderated (Go uses 100 parallel DBs × 100 puts; short mode skips).
#[test]
fn test_many_dbs() {
    let n_dbs = 20;
    let puts_per_db = 50;
    let mut handles = Vec::with_capacity(n_dbs);
    for _ in 0..n_dbs {
        handles.push(thread::spawn(move || {
            let (_dir, db) = common::open_tmp();
            let db = Arc::new(db);
            for _ in 0..puts_per_db {
                let mut key = [0u8; 16];
                fill_rand(&mut key);
                db.update(|tx| {
                    let b = tx.create_bucket_if_not_exists(b"bucket")?;
                    b.put(&key, &[])?;
                    Ok(())
                })
                .unwrap();
            }
            db.close().unwrap();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn fill_rand(buf: &mut [u8]) {
    let mut x = SEED.fetch_add(0x2545_f491_4f6c_dd1d, Ordering::Relaxed);
    for b in buf.iter_mut() {
        x = x.wrapping_mul(0x2545_f491_4f6c_dd1d).wrapping_add(1);
        *b = (x >> 33) as u8;
    }
}
