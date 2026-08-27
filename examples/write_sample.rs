fn main() {
    let path = std::env::args().nth(1).expect("path");
    let db = bbolt::Db::open(
        &path,
        0o600,
        Some(bbolt::Options {
            page_size: 4096,
            ..bbolt::Options::default()
        }),
    )
    .unwrap();
    db.update(|tx| {
        let b = tx.create_bucket(b"users")?;
        b.put(b"alice", b"from-rust")?;
        b.create_bucket(b"nested")?.put(b"x", b"y")?;
        Ok(())
    })
    .unwrap();
    println!("wrote {path}");
}
