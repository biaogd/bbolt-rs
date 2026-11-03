//! This integration test is a separate host executable linking the library.
#[global_allocator]
static HOST_ALLOCATOR: std::alloc::System = std::alloc::System;

#[test]
fn host_can_choose_global_allocator() {
    let dir = tempfile::tempdir().unwrap();
    let db = bbolt::Db::open(dir.path().join("db"), 0o600, None).unwrap();
    db.update(|tx| tx.create_bucket(b"host")?.put(b"key", b"value"))
        .unwrap();
    db.close().unwrap();
}
