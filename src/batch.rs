//! Coalesced write transactions (upstream `DB.Batch`).

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

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

pub fn batch<F>(db: &Db, f: F) -> Result<()>
where
    F: Fn(&Tx) -> Result<()> + Send + 'static,
{
    if db.inner.max_batch_size.load(Ordering::Relaxed) <= 1 {
        return db.update(|tx| f(tx));
    }

    let (tx_ch, rx) = mpsc::channel();
    let run_now;
    {
        let mut st = db.inner.batch.lock();
        st.calls.push(Call {
            f: Box::new(f),
            ret: tx_ch,
        });
        run_now = st.calls.len() >= db.inner.max_batch_size.load(Ordering::Relaxed);
        if run_now {
            st.scheduled = false;
        } else if !st.scheduled {
            st.scheduled = true;
            let db2 = db.clone();
            let delay = {
                let d = *db.inner.max_batch_delay.lock();
                d.max(Duration::from_millis(1))
            };
            thread::spawn(move || {
                thread::sleep(delay);
                drain_and_run(&db2);
            });
        }
    }
    if run_now {
        drain_and_run(db);
    }
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
                if let Err(e) = (c.f)(tx) {
                    fail_idx = Some(i);
                    return Err(e);
                }
            }
            Ok(())
        });
        if let Some(i) = fail_idx {
            let c = calls.swap_remove(i);
            let solo = db.update(|tx| (c.f)(tx));
            let _ = c.ret.send(solo);
            continue;
        }
        for c in calls.drain(..) {
            let msg = match &result {
                Ok(()) => Ok(()),
                Err(e) => Err(Error::Corrupt(e.to_string())),
            };
            let _ = c.ret.send(msg);
        }
    }
}
