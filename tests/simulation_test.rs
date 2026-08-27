//! Ports of upstream `simulation_test.go` / `simulation_no_freelist_sync_test.go`
//! for smaller op/process counts. Monster sizes (10000op / 1000p) are ignored.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use bbolt::{Db, Options};
use common::open_tmp_with;

/// In-memory mirror of nested bucket paths -> values (upstream `QuickDB`).
#[derive(Clone, Default)]
struct QuickDb {
    data: HashMap<Vec<Vec<u8>>, Vec<u8>>,
}

impl QuickDb {
    fn get(&self, keys: &[Vec<u8>]) -> Option<&[u8]> {
        self.data.get(keys).map(|v| v.as_slice())
    }

    fn put(&mut self, keys: Vec<Vec<u8>>, value: Vec<u8>) {
        self.data.insert(keys, value);
    }

    fn rand_keys(&self) -> Option<Vec<Vec<u8>>> {
        if self.data.is_empty() {
            return None;
        }
        let i = fastrand_usize(self.data.len());
        self.data.keys().nth(i).cloned()
    }
}

fn fastrand_usize(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    // Simple LCG from thread-local seed; good enough for simulation.
    use std::cell::Cell;
    thread_local! {
        static S: Cell<u64> = const { Cell::new(0x1234_5678_9abc_def0) };
    }
    S.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x as usize) % n
    })
}

fn rand_key() -> Vec<u8> {
    let n = 1 + fastrand_usize(8);
    (0..n).map(|_| (b'a' + fastrand_usize(26) as u8)).collect()
}

fn rand_keys() -> Vec<Vec<u8>> {
    let depth = 1 + fastrand_usize(3);
    (0..depth).map(|_| rand_key()).collect()
}

fn rand_value() -> Vec<u8> {
    let n = fastrand_usize(64);
    (0..n).map(|_| fastrand_usize(256) as u8).collect()
}

fn simulate_get(tx: &bbolt::Tx, qdb: &QuickDb) {
    let Some(keys) = qdb.rand_keys() else {
        return;
    };
    let mut b = tx.bucket(&keys[0]).expect("root bucket");
    for key in &keys[1..keys.len() - 1] {
        b = b.bucket(key).expect("nested bucket");
    }
    let expected = qdb.get(&keys).unwrap();
    let actual = b.get(&keys[keys.len() - 1]);
    assert_eq!(actual.as_deref(), Some(expected));
}

fn simulate_put(tx: &bbolt::Tx, qdb: &mut QuickDb) {
    let keys = rand_keys();
    let value = rand_value();
    let mut b = match tx.bucket(&keys[0]) {
        Some(b) => b,
        None => tx.create_bucket(&keys[0]).unwrap(),
    };
    for key in &keys[1..keys.len() - 1] {
        b = match b.bucket(key) {
            Some(nb) => nb,
            None => b.create_bucket(key).unwrap(),
        };
    }
    b.put(&keys[keys.len() - 1], &value).unwrap();
    qdb.put(keys, value);
}

/// Go: `testSimulate` — random concurrent readers/writers against a mirror.
fn test_simulate(opts: Options, rounds: usize, thread_count: usize, parallelism: usize) {
    let (_dir, db) = open_tmp_with(opts);
    let db = Arc::new(db);
    let versions: Arc<Mutex<HashMap<i64, QuickDb>>> = Arc::new(Mutex::new(HashMap::new()));
    versions.lock().unwrap().insert(1, QuickDb::default());

    for _ in 0..rounds {
        let mut handles = Vec::new();
        let sem = Arc::new(Mutex::new(parallelism));
        for _ in 0..thread_count {
            // Wait for a slot
            loop {
                let mut g = sem.lock().unwrap();
                if *g > 0 {
                    *g -= 1;
                    break;
                }
                drop(g);
                thread::yield_now();
            }
            let db = Arc::clone(&db);
            let versions = Arc::clone(&versions);
            let sem = Arc::clone(&sem);
            let writable = fastrand_usize(100) < 20;
            handles.push(thread::spawn(move || {
                let result = (|| -> bbolt::Result<()> {
                    let tx = db.begin(writable)?;
                    let mut qdb = {
                        let map = versions.lock().unwrap();
                        if writable {
                            map.get(&(tx.id() - 1))
                                .cloned()
                                .unwrap_or_default()
                        } else {
                            map.get(&tx.id()).cloned().unwrap_or_default()
                        }
                    };
                    if writable {
                        if fastrand_usize(2) == 0 {
                            simulate_put(&tx, &mut qdb);
                        } else if qdb.rand_keys().is_some() {
                            simulate_get(&tx, &qdb);
                        } else {
                            simulate_put(&tx, &mut qdb);
                        }
                        let id = tx.id();
                        tx.commit()?;
                        versions.lock().unwrap().insert(id, qdb);
                    } else {
                        if qdb.rand_keys().is_some() {
                            simulate_get(&tx, &qdb);
                        }
                        tx.rollback()?;
                    }
                    Ok(())
                })();
                *sem.lock().unwrap() += 1;
                result.unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}

// === Go: TestSimulate_*op_*p (short sizes always; large ignored) ===

#[test]
fn test_simulate_1op_1p() {
    // Go: TestSimulate_1op_1p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        1,
        1,
    );
}

#[test]
fn test_simulate_10op_1p() {
    // Go: TestSimulate_10op_1p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        10,
        1,
    );
}

#[test]
fn test_simulate_100op_1p() {
    // Go: TestSimulate_100op_1p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        1,
    );
}

#[test]
fn test_simulate_1000op_1p() {
    // Go: TestSimulate_1000op_1p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        1000,
        1,
    );
}

#[test]
fn test_simulate_10op_10p() {
    // Go: TestSimulate_10op_10p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        10,
        10,
    );
}

#[test]
fn test_simulate_100op_10p() {
    // Go: TestSimulate_100op_10p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        10,
    );
}

#[test]
fn test_simulate_1000op_10p() {
    // Go: TestSimulate_1000op_10p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        1000,
        10,
    );
}

#[test]
#[ignore = "slow; Go TestSimulate_100op_100p"]
fn test_simulate_100op_100p() {
    // Go: TestSimulate_100op_100p
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        100,
    );
}

#[test]
#[ignore = "slow; Go TestSimulate_10000op_* / 1000p monsters"]
fn test_simulate_10000op_1p() {
    test_simulate(
        Options {
            page_size: 4096,
            ..Options::default()
        },
        1,
        10000,
        1,
    );
}

// === Go: TestSimulateNoFreeListSync_* ===

#[test]
fn test_simulate_no_freelist_sync_1op_1p() {
    // Go: TestSimulateNoFreeListSync_1op_1p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        1,
        1,
    );
}

#[test]
fn test_simulate_no_freelist_sync_10op_1p() {
    // Go: TestSimulateNoFreeListSync_10op_1p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        10,
        1,
    );
}

#[test]
fn test_simulate_no_freelist_sync_100op_1p() {
    // Go: TestSimulateNoFreeListSync_100op_1p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        1,
    );
}

#[test]
fn test_simulate_no_freelist_sync_10op_10p() {
    // Go: TestSimulateNoFreeListSync_10op_10p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        10,
        10,
    );
}

#[test]
fn test_simulate_no_freelist_sync_100op_10p() {
    // Go: TestSimulateNoFreeListSync_100op_10p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        10,
    );
}

#[test]
fn test_simulate_no_freelist_sync_1000op_1p() {
    // Go: TestSimulateNoFreeListSync_1000op_1p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        1000,
        1,
    );
}

#[test]
fn test_simulate_no_freelist_sync_1000op_10p() {
    // Go: TestSimulateNoFreeListSync_1000op_10p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        1000,
        10,
    );
}

#[test]
#[ignore = "slow; Go TestSimulateNoFreeListSync_100op_100p"]
fn test_simulate_no_freelist_sync_100op_100p() {
    // Go: TestSimulateNoFreeListSync_100op_100p
    test_simulate(
        Options {
            no_freelist_sync: true,
            page_size: 4096,
            ..Options::default()
        },
        1,
        100,
        100,
    );
}

#[allow(dead_code)]
fn _db_type_hint(_: &Db) {}
