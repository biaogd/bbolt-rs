//! Coalesced write transactions (upstream `DB.Batch`).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;

use std::sync::atomic::Ordering;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::tx::Tx;

type BatchFn = Box<dyn Fn(&Tx) -> Result<()> + Send>;

struct Call {
    f: BatchFn,
    ret: mpsc::Sender<Result<()>>,
}

#[derive(Default)]
pub struct BatchState {
    calls: Vec<Call>,
    scheduled: bool,
}

fn safely_call(f: &BatchFn, tx: &Tx) -> Result<()> {
    match catch_unwind(AssertUnwindSafe(|| f(tx))) {
        Ok(r) => r,
        Err(_) => Err(Error::Corrupt("batch function panicked".into())),
    }
}

pub fn batch<F>(db: &Db, f: F) -> Result<()>
where
    F: Fn(&Tx) -> Result<()> + Send + 'static,
{
    if db.inner.max_batch_size.load(Ordering::Relaxed) <= 1 {
        return db.update(|tx| f(tx));
    }

    let (tx_ch, rx) = mpsc::channel();
    let ready;
    {
        let mut st = db.inner.batch.lock();
        st.calls.push(Call {
            f: Box::new(f),
            ret: tx_ch,
        });
        let delay = *db.inner.max_batch_delay.lock();
        let run_now = delay.is_zero()
            || st.calls.len() >= db.inner.max_batch_size.load(Ordering::Relaxed).max(1);
        ready = if run_now {
            std::mem::take(&mut st.calls)
        } else {
            Vec::new()
        };
        if !run_now && !st.scheduled {
            // At most one sleeping timer per database, including size-triggered drains.
            st.scheduled = true;
            let db2 = db.clone();
            thread::spawn(move || {
                thread::sleep(delay);
                drain_and_run(&db2);
            });
        }
    }
    run_calls(db, ready);
    rx.recv().unwrap_or(Err(Error::TxClosed))
}

fn drain_and_run(db: &Db) {
    let calls = {
        let mut st = db.inner.batch.lock();
        st.scheduled = false;
        std::mem::take(&mut st.calls)
    };
    run_calls(db, calls);
}

fn run_calls(db: &Db, mut calls: Vec<Call>) {
    while !calls.is_empty() {
        let mut fail_idx: Option<usize> = None;
        let result = db.update(|tx| {
            for (i, c) in calls.iter().enumerate() {
                if let Err(e) = safely_call(&c.f, tx) {
                    fail_idx = Some(i);
                    return Err(e);
                }
            }
            Ok(())
        });
        if let Some(i) = fail_idx {
            let c = calls.remove(i);
            let solo = db.update(|tx| safely_call(&c.f, tx));
            let _ = c.ret.send(solo);
            continue;
        }
        let result = result.map_err(std::sync::Arc::new);
        for c in calls.drain(..) {
            let msg = match &result {
                Ok(()) => Ok(()),
                Err(e) => Err(Error::BatchFailed(std::sync::Arc::clone(e))),
            };
            let _ = c.ret.send(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    #[test]
    fn zero_delay_never_schedules_a_timer() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("db"), 0o600, Some(Options::default())).unwrap();
        db.set_max_batch_delay(Duration::ZERO);
        db.batch(|tx| tx.create_bucket(b"zero")?.put(b"a", b"b"))
            .unwrap();
        let state = db.inner.batch.lock();
        assert!(!state.scheduled);
        assert!(state.calls.is_empty());
        drop(state);
        db.close().unwrap();
    }

    #[test]
    fn callback_failure_retries_survivors_and_preserves_queue_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("db"), 0o600, None).unwrap();
        db.update(|tx| tx.create_bucket(b"batch").map(|_| ()))
            .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let (a, ra) = mpsc::channel();
        let (b, rb) = mpsc::channel();
        let (c, rc) = mpsc::channel();
        let counter = count.clone();
        let calls = vec![
            Call {
                f: Box::new(move |tx| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    tx.bucket(b"batch")?.unwrap().put(b"key", b"first")
                }),
                ret: a,
            },
            Call {
                f: Box::new(|_| Err(Error::KeyRequired)),
                ret: b,
            },
            Call {
                f: Box::new(|tx| tx.bucket(b"batch")?.unwrap().put(b"key", b"last")),
                ret: c,
            },
        ];
        run_calls(&db, calls);
        ra.recv().unwrap().unwrap();
        assert!(matches!(rb.recv().unwrap(), Err(Error::KeyRequired)));
        rc.recv().unwrap().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        db.view(|tx| {
            assert_eq!(tx.bucket(b"batch")?.unwrap().get(b"key")?.unwrap(), b"last");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn zero_delay_concurrent_calls_all_complete() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("db"), 0o600, None).unwrap();
        db.update(|tx| tx.create_bucket(b"batch").map(|_| ()))
            .unwrap();
        db.set_max_batch_delay(Duration::ZERO);
        let barrier = Arc::new(Barrier::new(8));
        let mut threads = vec![];
        for i in 0u64..8 {
            let db = db.clone();
            let barrier = barrier.clone();
            threads.push(thread::spawn(move || {
                barrier.wait();
                db.batch(move |tx| tx.bucket(b"batch")?.unwrap().put(&i.to_le_bytes(), b"ok"))
            }));
        }
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        db.view(|tx| {
            let b = tx.bucket(b"batch")?.unwrap();
            for i in 0u64..8 {
                assert!(b.has_value(&i.to_le_bytes())?);
            }
            Ok(())
        })
        .unwrap();
        assert!(!db.inner.batch.lock().scheduled);
        db.close().unwrap();
    }
}
