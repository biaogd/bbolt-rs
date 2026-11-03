#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let mut bytes = [0u8; std::mem::size_of::<usize>()];
    let n = bytes.len().min(data.len());
    bytes[..n].copy_from_slice(&data[..n]);
    for index in [0, 1, usize::MAX, usize::from_le_bytes(bytes)] {
        let _ = bbolt::leaf_at(data, index);
        let _ = bbolt::branch_at(data, index);
        let _ = bbolt::branch_pgid(data, index);
    }
    let _ = bbolt::read_inodes(data);
});
