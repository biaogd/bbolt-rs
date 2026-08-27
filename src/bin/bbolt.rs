//! `bbolt` CLI — subset of upstream `cmd/bbolt` commands.

use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use bbolt::{compact_files, CheckOptions, Db, FreelistType, Options};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "bbolt",
    version = env!("CARGO_PKG_VERSION"),
    about = "bbolt database tool (Rust port)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print version
    Version,
    /// Print basic database information
    Info { path: PathBuf },
    /// List top-level buckets
    Buckets { path: PathBuf },
    /// List keys in a (sub)bucket
    Keys {
        path: PathBuf,
        buckets: Vec<String>,
    },
    /// Get a value
    Get {
        path: PathBuf,
        /// Bucket path then key (last argument is the key)
        args: Vec<String>,
    },
    /// Verify integrity
    Check {
        path: PathBuf,
        #[arg(long, default_value_t = 0)]
        from_page: u64,
    },
    /// Compact into a new file
    Compact {
        #[arg(short = 'o', long)]
        output: PathBuf,
        source: PathBuf,
        #[arg(long, default_value_t = 65536)]
        tx_max_size: i64,
    },
    /// Print page list
    Pages { path: PathBuf },
    /// Inspect bucket tree
    Inspect { path: PathBuf },
    /// Print freelist-oriented stats
    Stats {
        path: PathBuf,
        #[arg(long, default_value = "array")]
        freelist: String,
    },
}

fn open_ro(path: &PathBuf) -> Result<Db, bbolt::Error> {
    Db::open(
        path,
        0o600,
        Some(Options {
            read_only: true,
            pre_load_freelist: true,
            ..Options::default()
        }),
    )
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("Error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Version => {
            println!("bbolt {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Info { path } => {
            let db = open_ro(&path)?;
            let info = db.info();
            println!("PageSize: {}", info.page_size);
            db.view(|tx| {
                println!("PageCount: {}", tx.high_water_mark());
                println!("Txid: {}", tx.id());
                Ok(())
            })?;
        }
        Commands::Buckets { path } => {
            let db = open_ro(&path)?;
            db.view(|tx| {
                tx.for_each(|name, _| {
                    println!("{}", String::from_utf8_lossy(name));
                    Ok(())
                })
            })?;
        }
        Commands::Keys { path, buckets } => {
            let db = open_ro(&path)?;
            db.view(|tx| {
                let mut b = None;
                for (i, name) in buckets.iter().enumerate() {
                    let next = if i == 0 {
                        tx.bucket(name.as_bytes())
                    } else {
                        b.as_ref()
                            .and_then(|bb: &bbolt::Bucket| bb.bucket(name.as_bytes()))
                    };
                    b = Some(next.ok_or(bbolt::Error::BucketNotFound)?);
                }
                let b = b.ok_or(bbolt::Error::BucketNameRequired)?;
                b.for_each(|k, _| {
                    println!("{}", String::from_utf8_lossy(k));
                    Ok(())
                })
            })?;
        }
        Commands::Get { path, args } => {
            if args.len() < 2 {
                return Err("usage: get <file> <bucket...> <key>".into());
            }
            let key = args.last().unwrap().clone();
            let buckets = &args[..args.len() - 1];
            let db = open_ro(&path)?;
            db.view(|tx| {
                let mut b = tx
                    .bucket(buckets[0].as_bytes())
                    .ok_or(bbolt::Error::BucketNotFound)?;
                for name in &buckets[1..] {
                    b = b
                        .bucket(name.as_bytes())
                        .ok_or(bbolt::Error::BucketNotFound)?;
                }
                match b.get(key.as_bytes()) {
                    Some(v) => {
                        io::stdout()
                            .write_all(&v)
                            .map_err(|e| bbolt::Error::io("<stdout>", e))?;
                        io::stdout()
                            .write_all(b"\n")
                            .map_err(|e| bbolt::Error::io("<stdout>", e))?;
                    }
                    None => return Err(bbolt::Error::Corrupt("key not found".into())),
                }
                Ok(())
            })?;
        }
        Commands::Check { path, from_page } => {
            let db = open_ro(&path)?;
            let errs = db.view(|tx| {
                Ok(tx.check_with(CheckOptions {
                    page_id: from_page,
                }))
            })?;
            if errs.is_empty() {
                println!("OK");
            } else {
                for e in &errs {
                    eprintln!("{e}");
                }
                return Err("check failed".into());
            }
        }
        Commands::Compact {
            output,
            source,
            tx_max_size,
        } => {
            compact_files(&output, &source, tx_max_size, 0)?;
            let src_sz = std::fs::metadata(&source)?.len();
            let dst_sz = std::fs::metadata(&output)?.len();
            println!("{source:?} -> {output:?}");
            println!("{src_sz} -> {dst_sz} bytes ({:.1}% smaller)", {
                if src_sz == 0 {
                    0.0
                } else {
                    (1.0 - dst_sz as f64 / src_sz as f64) * 100.0
                }
            });
        }
        Commands::Pages { path } => {
            let db = open_ro(&path)?;
            println!(
                "{:<10} {:<10} {:<10} {:<10}",
                "ID", "TYPE", "COUNT", "OVERFLOW"
            );
            db.view(|tx| {
                let hwm = tx.high_water_mark();
                for id in 0..hwm {
                    let info = tx.page_info(id)?;
                    println!(
                        "{:<10} {:<10} {:<10} {:<10}",
                        info.id, info.page_type, info.count, info.overflow
                    );
                }
                Ok(())
            })?;
        }
        Commands::Inspect { path } => {
            let db = open_ro(&path)?;
            db.view(|tx| {
                let tree = tx.inspect();
                print_tree(&tree, 0);
                Ok(())
            })?;
        }
        Commands::Stats { path, freelist } => {
            let fl = if freelist == "hashmap" {
                FreelistType::HashMap
            } else {
                FreelistType::Array
            };
            let db = Db::open(
                &path,
                0o600,
                Some(Options {
                    read_only: true,
                    pre_load_freelist: true,
                    freelist_type: fl,
                    ..Options::default()
                }),
            )?;
            let st = db.stats();
            println!("FreePageN: {}", st.free_page_n);
            println!("PendingPageN: {}", st.pending_page_n);
            println!("FreeAlloc: {}", st.free_alloc);
            println!("FreelistInuse: {}", st.freelist_inuse);
        }
    }
    Ok(())
}

fn print_tree(bs: &bbolt::BucketStructure, depth: usize) {
    let pad = "  ".repeat(depth);
    println!("{pad}{} (keys={})", bs.name, bs.key_n);
    for c in &bs.children {
        print_tree(c, depth + 1);
    }
}
