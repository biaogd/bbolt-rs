//! Apples-to-apples workload harness matching `benches/go`.
//!
//! ```text
//! cargo run --release --bin bench_compare -- -dir DIR -workload NAME -n N
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use bbolt::{Db, FreelistType, Options};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "bench_compare", about = "Rust bbolt compare harness")]
struct Args {
    /// Directory for the database file
    #[arg(long)]
    dir: PathBuf,
    /// Workload name
    #[arg(long)]
    workload: String,
    /// Number of keys / ops
    #[arg(long, default_value_t = 100_000)]
    n: usize,
    #[arg(long, default_value_t = 8)]
    key_size: usize,
    #[arg(long, default_value_t = 32)]
    value_size: usize,
    #[arg(long, default_value_t = 4096)]
    page_size: usize,
    #[arg(long, default_value_t = false)]
    no_sync: bool,
    #[arg(long, default_value_t = 0.5)]
    delete_frac: f64,
    #[arg(long, default_value_t = 1)]
    batch_size: usize,
    #[arg(long, default_value_t = 1)]
    seed: u64,
}

fn main() -> ExitCode {
    if let Err(e) = run() {
        eprintln!("bench_compare: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<(), String> {
    let args = Args::parse();
    if args.key_size < 8 {
        return Err("key-size must be >= 8".into());
    }
    fs::create_dir_all(&args.dir).map_err(|e| e.to_string())?;
    let path = args.dir.join("bench.db");
    let _ = fs::remove_file(&path);

    let opts = Options {
        page_size: args.page_size,
        freelist_type: FreelistType::Array,
        no_sync: args.no_sync,
        no_grow_sync: false,
        ..Options::default()
    };

    let mut rng = XorShift64::new(args.seed);
    let (ops, elapsed_ns) = match args.workload.as_str() {
        "seq_put" | "one_large_tx" | "large_value" => {
            timed_seq_put(&path, &opts, args.n, args.key_size, args.value_size)?
        }
        "random_put" => {
            timed_random_put(&path, &opts, args.n, args.key_size, args.value_size, &mut rng)?
        }
        "random_get" => {
            prepare_seq_put(&path, &opts, args.n, args.key_size, args.value_size)?;
            timed_random_get(&path, &opts, args.n, args.key_size, &mut rng)?
        }
        "cursor_scan" => {
            prepare_seq_put(&path, &opts, args.n, args.key_size, args.value_size)?;
            timed_cursor_scan(&path, &opts)?
        }
        "deletes" => {
            prepare_seq_put(&path, &opts, args.n, args.key_size, args.value_size)?;
            timed_deletes(&path, &opts, args.n, args.key_size, args.delete_frac)?
        }
        "many_small_tx" => timed_many_small_tx(
            &path,
            &opts,
            args.n,
            args.key_size,
            args.value_size,
            args.batch_size,
        )?,
        other => return Err(format!("unknown workload {other:?}")),
    };

    let filesize = fs::metadata(&path).map_err(|e| e.to_string())?.len();
    let secs = elapsed_ns as f64 / 1e9;
    let ops_sec = ops as f64 / secs;
    println!(
        "impl=rust workload={} n={} elapsed_ns={} ops_sec={:.2} filesize={} nosync={} key_size={} value_size={}",
        args.workload, ops, elapsed_ns, ops_sec, filesize, args.no_sync, args.key_size, args.value_size
    );
    Ok(())
}

fn open_db(path: &Path, opts: &Options) -> Result<Db, String> {
    Db::open(path, 0o600, Some(opts.clone())).map_err(|e| e.to_string())
}

fn make_key(buf: &mut [u8], i: usize) {
    buf.fill(0);
    let n = buf.len();
    buf[n - 8..].copy_from_slice(&(i as u64).to_be_bytes());
}

fn make_val(buf: &mut [u8], i: usize) {
    for (j, b) in buf.iter_mut().enumerate() {
        *b = (i + j) as u8;
    }
}

fn prepare_seq_put(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    value_size: usize,
) -> Result<(), String> {
    let _ = fs::remove_file(path);
    timed_seq_put(path, opts, n, key_size, value_size).map(|_| ())
}

fn timed_seq_put(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    value_size: usize,
) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let mut key = vec![0u8; key_size];
    let mut val = vec![0u8; value_size];
    let start = Instant::now();
    db.update(|tx| {
        let b = tx.create_bucket_if_not_exists(b"bench")?;
        for i in 0..n {
            make_key(&mut key, i);
            make_val(&mut val, i);
            b.put(&key, &val)?;
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((n, elapsed))
}

fn timed_random_put(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    value_size: usize,
    rng: &mut XorShift64,
) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let order = rng.perm(n);
    let mut key = vec![0u8; key_size];
    let mut val = vec![0u8; value_size];
    let start = Instant::now();
    db.update(|tx| {
        let b = tx.create_bucket_if_not_exists(b"bench")?;
        for &i in &order {
            make_key(&mut key, i);
            make_val(&mut val, i);
            b.put(&key, &val)?;
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((n, elapsed))
}

fn timed_random_get(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    rng: &mut XorShift64,
) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let order = rng.perm(n);
    let mut key = vec![0u8; key_size];
    let start = Instant::now();
    db.view(|tx| {
        let b = tx.bucket(b"bench").ok_or(bbolt::Error::BucketNotFound)?;
        for &i in &order {
            make_key(&mut key, i);
            if b.get(&key).is_none() {
                return Err(bbolt::Error::Invalid);
            }
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((n, elapsed))
}

fn timed_cursor_scan(path: &Path, opts: &Options) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let mut count = 0usize;
    let start = Instant::now();
    db.view(|tx| {
        let b = tx.bucket(b"bench").ok_or(bbolt::Error::BucketNotFound)?;
        let mut c = b.cursor();
        let mut kv = c.first()?;
        while kv.0.is_some() {
            count += 1;
            kv = c.next()?;
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((count, elapsed))
}

fn timed_deletes(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    frac: f64,
) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let del_n = (n as f64 * frac) as usize;
    let mut key = vec![0u8; key_size];
    let start = Instant::now();
    db.update(|tx| {
        let b = tx.bucket(b"bench").ok_or(bbolt::Error::BucketNotFound)?;
        for i in 0..del_n {
            make_key(&mut key, i);
            b.delete(&key)?;
        }
        Ok(())
    })
    .map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((del_n, elapsed))
}

fn timed_many_small_tx(
    path: &Path,
    opts: &Options,
    n: usize,
    key_size: usize,
    value_size: usize,
    batch_size: usize,
) -> Result<(usize, u128), String> {
    let db = open_db(path, opts)?;
    let batch_size = batch_size.max(1);
    let mut key = vec![0u8; key_size];
    let mut val = vec![0u8; value_size];
    let start = Instant::now();
    let mut i = 0;
    while i < n {
        let end = (i + batch_size).min(n);
        let lo = i;
        let hi = end;
        db.update(|tx| {
            let b = tx.create_bucket_if_not_exists(b"bench")?;
            for j in lo..hi {
                make_key(&mut key, j);
                make_val(&mut val, j);
                b.put(&key, &val)?;
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?;
        i = end;
    }
    let elapsed = start.elapsed().as_nanos();
    db.close().map_err(|e| e.to_string())?;
    Ok((n, elapsed))
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed | 1,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn perm(&mut self, n: usize) -> Vec<usize> {
        let mut v: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            let j = (self.next_u64() as usize) % (i + 1);
            v.swap(i, j);
        }
        v
    }
}
