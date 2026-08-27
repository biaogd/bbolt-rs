//! On-disk page layout, matching etcd-io/bbolt (version 2).
//!
//! All multi-byte integers are little-endian, as produced by Go on amd64.

use crate::error::{Error, Result};

pub type Pgid = u64;
pub type Txid = u64;

pub const MAGIC: u32 = 0xED0C_DAED;
pub const VERSION: u32 = 2;
pub const PGID_NO_FREELIST: Pgid = u64::MAX;

pub const PAGE_HEADER_SIZE: usize = 16;
pub const BRANCH_PAGE_ELEMENT_SIZE: usize = 16;
pub const LEAF_PAGE_ELEMENT_SIZE: usize = 16;
pub const BUCKET_HEADER_SIZE: usize = 16;
pub const META_SIZE: usize = 64;
pub const META_CHECKSUM_LEN: usize = 56;

pub const BRANCH_PAGE_FLAG: u16 = 0x01;
pub const LEAF_PAGE_FLAG: u16 = 0x02;
pub const META_PAGE_FLAG: u16 = 0x04;
pub const FREELIST_PAGE_FLAG: u16 = 0x10;
pub const BUCKET_LEAF_FLAG: u32 = 0x01;

pub const MIN_KEYS_PER_PAGE: usize = 2;
pub const MAX_KEY_SIZE: usize = 32_768;
pub const MAX_VALUE_SIZE: usize = (1 << 31) - 2;
pub const DEFAULT_FILL_PERCENT: f64 = 0.5;
pub const MIN_FILL_PERCENT: f64 = 0.1;
pub const MAX_FILL_PERCENT: f64 = 1.0;

pub const MAX_MMAP_STEP: usize = 1 << 30;
pub const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
pub const DEFAULT_MAX_BATCH_DELAY_MS: u64 = 10;
pub const DEFAULT_ALLOC_SIZE: usize = 16 * 1024 * 1024;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

#[inline]
pub fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(buf[off..off + 2].try_into().unwrap())
}

#[inline]
pub fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

#[inline]
pub fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

#[inline]
pub fn write_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn write_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InBucket {
    pub root: Pgid,
    pub sequence: u64,
}

impl InBucket {
    pub fn new(root: Pgid, sequence: u64) -> Self {
        Self { root, sequence }
    }

    pub fn read(buf: &[u8]) -> Self {
        Self {
            root: read_u64(buf, 0),
            sequence: read_u64(buf, 8),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        write_u64(buf, 0, self.root);
        write_u64(buf, 8, self.sequence);
    }

    #[allow(dead_code)]
    pub fn to_bytes(self) -> [u8; BUCKET_HEADER_SIZE] {
        let mut b = [0u8; BUCKET_HEADER_SIZE];
        self.write(&mut b);
        b
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub flags: u32,
    pub root: InBucket,
    pub freelist: Pgid,
    pub pgid: Pgid,
    pub txid: Txid,
    pub checksum: u64,
}

impl Meta {
    pub fn read(buf: &[u8]) -> Self {
        Self {
            magic: read_u32(buf, 0),
            version: read_u32(buf, 4),
            page_size: read_u32(buf, 8),
            flags: read_u32(buf, 12),
            root: InBucket::read(&buf[16..32]),
            freelist: read_u64(buf, 32),
            pgid: read_u64(buf, 40),
            txid: read_u64(buf, 48),
            checksum: read_u64(buf, 56),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        write_u32(buf, 0, self.magic);
        write_u32(buf, 4, self.version);
        write_u32(buf, 8, self.page_size);
        write_u32(buf, 12, self.flags);
        self.root.write(&mut buf[16..32]);
        write_u64(buf, 32, self.freelist);
        write_u64(buf, 40, self.pgid);
        write_u64(buf, 48, self.txid);
        write_u64(buf, 56, self.checksum);
    }

    pub fn sum64(&self) -> u64 {
        let mut buf = [0u8; META_SIZE];
        self.write(&mut buf);
        fnv1a64(&buf[..META_CHECKSUM_LEN])
    }

    pub fn finish_checksum(&mut self) {
        self.checksum = self.sum64();
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Invalid);
        }
        if self.version != VERSION {
            return Err(Error::VersionMismatch);
        }
        if self.checksum != self.sum64() {
            return Err(Error::Checksum);
        }
        Ok(())
    }

    pub fn is_freelist_persisted(&self) -> bool {
        self.freelist != PGID_NO_FREELIST
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PageHeader {
    pub id: Pgid,
    pub flags: u16,
    pub count: u16,
    pub overflow: u32,
}

impl PageHeader {
    pub fn read(buf: &[u8]) -> Self {
        Self {
            id: read_u64(buf, 0),
            flags: read_u16(buf, 8),
            count: read_u16(buf, 10),
            overflow: read_u32(buf, 12),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        write_u64(buf, 0, self.id);
        write_u16(buf, 8, self.flags);
        write_u16(buf, 10, self.count);
        write_u32(buf, 12, self.overflow);
    }

    pub fn is_branch(&self) -> bool {
        self.flags == BRANCH_PAGE_FLAG
    }

    pub fn is_leaf(&self) -> bool {
        self.flags == LEAF_PAGE_FLAG
    }

    pub fn is_meta(&self) -> bool {
        self.flags == META_PAGE_FLAG
    }

    pub fn is_freelist(&self) -> bool {
        self.flags == FREELIST_PAGE_FLAG
    }

    #[allow(dead_code)]
    pub fn is_valid_type(&self) -> bool {
        self.is_branch() || self.is_leaf() || self.is_meta() || self.is_freelist()
    }

    pub fn typ(&self) -> String {
        if self.is_branch() {
            "branch".into()
        } else if self.is_leaf() {
            "leaf".into()
        } else if self.is_meta() {
            "meta".into()
        } else if self.is_freelist() {
            "freelist".into()
        } else {
            format!("unknown<{:02x}>", self.flags)
        }
    }

    #[allow(dead_code)]
    pub fn element_size(&self) -> usize {
        if self.is_leaf() {
            LEAF_PAGE_ELEMENT_SIZE
        } else {
            BRANCH_PAGE_ELEMENT_SIZE
        }
    }
}

pub fn page_id(buf: &[u8]) -> Pgid {
    read_u64(buf, 0)
}

pub fn set_page_id(buf: &mut [u8], id: Pgid) {
    write_u64(buf, 0, id);
}

pub fn set_page_flags(buf: &mut [u8], flags: u16) {
    write_u16(buf, 8, flags);
}

pub fn set_page_count(buf: &mut [u8], count: u16) {
    write_u16(buf, 10, count);
}

pub fn set_page_overflow(buf: &mut [u8], overflow: u32) {
    write_u32(buf, 12, overflow);
}

/// Leaf element at `index`, returning (flags, key, value).
pub fn leaf_at(page: &[u8], index: usize) -> (u32, &[u8], &[u8]) {
    let off = PAGE_HEADER_SIZE + index * LEAF_PAGE_ELEMENT_SIZE;
    let flags = read_u32(page, off);
    let pos = read_u32(page, off + 4) as usize;
    let ksize = read_u32(page, off + 8) as usize;
    let vsize = read_u32(page, off + 12) as usize;
    let key_off = off + pos;
    let key = &page[key_off..key_off + ksize];
    let val = &page[key_off + ksize..key_off + ksize + vsize];
    (flags, key, val)
}

/// Branch element at `index`, returning (pgid, key).
pub fn branch_at(page: &[u8], index: usize) -> (Pgid, &[u8]) {
    let off = PAGE_HEADER_SIZE + index * BRANCH_PAGE_ELEMENT_SIZE;
    let pos = read_u32(page, off) as usize;
    let ksize = read_u32(page, off + 4) as usize;
    let pgid = read_u64(page, off + 8);
    let key_off = off + pos;
    (pgid, &page[key_off..key_off + ksize])
}

pub fn branch_pgid(page: &[u8], index: usize) -> Pgid {
    let off = PAGE_HEADER_SIZE + index * BRANCH_PAGE_ELEMENT_SIZE;
    read_u64(page, off + 8)
}

pub fn meta_from_page(page: &[u8]) -> Meta {
    Meta::read(&page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + META_SIZE])
}

pub fn write_meta_page(page: &mut [u8], meta: &Meta) {
    let hdr = PageHeader {
        id: meta.txid % 2,
        flags: META_PAGE_FLAG,
        count: 0,
        overflow: 0,
    };
    hdr.write(page);
    let mut m = meta.clone();
    if m.root.root >= m.pgid {
        panic!(
            "root bucket pgid ({}) above high water mark ({})",
            m.root.root, m.pgid
        );
    }
    if m.freelist >= m.pgid && m.freelist != PGID_NO_FREELIST {
        panic!(
            "freelist pgid ({}) above high water mark ({})",
            m.freelist, m.pgid
        );
    }
    m.finish_checksum();
    m.write(&mut page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + META_SIZE]);
}

pub fn write_inodes(page: &mut [u8], is_leaf: bool, inodes: &[Inode]) {
    let elem_size = if is_leaf {
        LEAF_PAGE_ELEMENT_SIZE
    } else {
        BRANCH_PAGE_ELEMENT_SIZE
    };
    let flags = if is_leaf {
        LEAF_PAGE_FLAG
    } else {
        BRANCH_PAGE_FLAG
    };
    set_page_flags(page, flags);
    if inodes.len() >= 0xFFFF {
        panic!("inode overflow: {} (pgid={})", inodes.len(), page_id(page));
    }
    set_page_count(page, inodes.len() as u16);
    if inodes.is_empty() {
        return;
    }
    let mut off = PAGE_HEADER_SIZE + elem_size * inodes.len();
    for (i, inode) in inodes.iter().enumerate() {
        assert!(!inode.key.is_empty(), "write: zero-length inode key");
        let elem_off = PAGE_HEADER_SIZE + i * elem_size;
        let pos = (off - elem_off) as u32;
        if is_leaf {
            write_u32(page, elem_off, inode.flags);
            write_u32(page, elem_off + 4, pos);
            write_u32(page, elem_off + 8, inode.key.len() as u32);
            write_u32(page, elem_off + 12, inode.value.len() as u32);
        } else {
            write_u32(page, elem_off, pos);
            write_u32(page, elem_off + 4, inode.key.len() as u32);
            write_u64(page, elem_off + 8, inode.pgid);
            assert!(inode.pgid != page_id(page), "write: circular dependency");
        }
        page[off..off + inode.key.len()].copy_from_slice(&inode.key);
        off += inode.key.len();
        if !inode.value.is_empty() {
            page[off..off + inode.value.len()].copy_from_slice(&inode.value);
            off += inode.value.len();
        }
    }
}

pub fn read_inodes(page: &[u8]) -> Vec<Inode> {
    let hdr = PageHeader::read(page);
    let count = hdr.count as usize;
    let mut inodes = Vec::with_capacity(count);
    if hdr.is_leaf() {
        for i in 0..count {
            let (flags, key, value) = leaf_at(page, i);
            inodes.push(Inode {
                flags,
                pgid: 0,
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
    } else {
        for i in 0..count {
            let (pgid, key) = branch_at(page, i);
            inodes.push(Inode {
                flags: 0,
                pgid,
                key: key.to_vec(),
                value: Vec::new(),
            });
        }
    }
    inodes
}

/// Serialized size of `inodes` on a leaf or branch page.
pub fn inodes_size(is_leaf: bool, inodes: &[Inode]) -> usize {
    let elsz = if is_leaf {
        LEAF_PAGE_ELEMENT_SIZE
    } else {
        BRANCH_PAGE_ELEMENT_SIZE
    };
    let mut sz = PAGE_HEADER_SIZE;
    for inode in inodes {
        sz += elsz + inode.key.len() + inode.value.len();
    }
    sz
}

#[derive(Clone, Debug, Default)]
pub struct Inode {
    pub flags: u32,
    pub pgid: Pgid,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub fn merge_pgids(a: &[Pgid], b: &[Pgid]) -> Vec<Pgid> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] < b[j] {
            out.push(a[i]);
            i += 1;
        } else {
            out.push(b[j]);
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

pub fn freelist_ids(page: &[u8]) -> Vec<Pgid> {
    let hdr = PageHeader::read(page);
    assert!(hdr.is_freelist(), "not a freelist page");
    let mut idx = 0usize;
    let mut count = hdr.count as usize;
    if count == 0xFFFF {
        idx = 1;
        count = read_u64(page, PAGE_HEADER_SIZE) as usize;
    }
    let mut ids = Vec::with_capacity(count);
    for n in 0..count {
        let off = PAGE_HEADER_SIZE + (idx + n) * 8;
        ids.push(read_u64(page, off));
    }
    ids
}

pub fn write_freelist(page: &mut [u8], ids: &[Pgid]) {
    set_page_flags(page, FREELIST_PAGE_FLAG);
    let l = ids.len();
    if l == 0 {
        set_page_count(page, 0);
        return;
    }
    if l < 0xFFFF {
        set_page_count(page, l as u16);
        for (i, id) in ids.iter().enumerate() {
            write_u64(page, PAGE_HEADER_SIZE + i * 8, *id);
        }
    } else {
        set_page_count(page, 0xFFFF);
        write_u64(page, PAGE_HEADER_SIZE, l as u64);
        for (i, id) in ids.iter().enumerate() {
            write_u64(page, PAGE_HEADER_SIZE + (i + 1) * 8, *id);
        }
    }
}

pub fn estimated_freelist_page_size(n: usize) -> usize {
    let mut n = n;
    if n >= 0xFFFF {
        n += 1;
    }
    PAGE_HEADER_SIZE + 8 * n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_matches_go_meta0_txid2() {
        let mut m = Meta {
            magic: MAGIC,
            version: VERSION,
            page_size: 4096,
            flags: 0,
            root: InBucket::new(5, 0),
            freelist: 6,
            pgid: 7,
            txid: 2,
            checksum: 0,
        };
        m.finish_checksum();
        assert_eq!(m.checksum, 0x38c1_0c38_2f8a_ff2d);
    }

    #[test]
    fn fnv_matches_go_init_meta() {
        let mut m0 = Meta {
            magic: MAGIC,
            version: VERSION,
            page_size: 4096,
            flags: 0,
            root: InBucket::new(3, 0),
            freelist: 2,
            pgid: 4,
            txid: 0,
            checksum: 0,
        };
        m0.finish_checksum();
        assert_eq!(m0.checksum, 0x0751_6e11_4689_fdee);

        let mut m1 = m0.clone();
        m1.txid = 1;
        m1.finish_checksum();
        assert_eq!(m1.checksum, 0x264c_351a_5179_480f);
    }

    // Go: TestPgids_merge
    #[test]
    fn merge_pgids_union() {
        let a = vec![4, 5, 6, 10, 11, 12, 13, 27];
        let b = vec![1, 3, 8, 9, 25, 30];
        assert_eq!(
            merge_pgids(&a, &b),
            vec![1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 25, 27, 30]
        );
    }

    // Go: TestPgids_merge_quick (property: merge equals sorted concat; may keep dupes)
    #[test]
    fn merge_pgids_quick() {
        for _ in 0..200 {
            let mut a: Vec<u64> = (0..20).map(|_| fastrand_u64(50)).collect();
            let mut b: Vec<u64> = (0..20).map(|_| fastrand_u64(50)).collect();
            a.sort_unstable();
            b.sort_unstable();
            let m = merge_pgids(&a, &b);
            let mut exp = a.clone();
            exp.extend_from_slice(&b);
            exp.sort_unstable();
            assert_eq!(m, exp);
        }
    }

    fn fastrand_u64(n: u64) -> u64 {
        use std::cell::Cell;
        thread_local! {
            static S: Cell<u64> = const { Cell::new(0x9e37_79b9_7f4a_7c15) };
        }
        S.with(|s| {
            let mut x = s.get();
            x = x.wrapping_mul(0x2545_f491_4f6c_dd1d).wrapping_add(1);
            s.set(x);
            x % n.max(1)
        })
    }

    #[test]
    fn leaf_roundtrip() {
        let mut page = vec![0u8; 4096];
        set_page_id(&mut page, 4);
        let inodes = vec![
            Inode {
                flags: 0,
                pgid: 0,
                key: b"alice".to_vec(),
                value: b"data1".to_vec(),
            },
            Inode {
                flags: BUCKET_LEAF_FLAG,
                pgid: 0,
                key: b"nested".to_vec(),
                value: vec![0u8; 16],
            },
        ];
        write_inodes(&mut page, true, &inodes);
        let hdr = PageHeader::read(&page);
        assert!(hdr.is_leaf());
        assert_eq!(hdr.count, 2);
        let (flags, key, val) = leaf_at(&page, 0);
        assert_eq!(flags, 0);
        assert_eq!(key, b"alice");
        assert_eq!(val, b"data1");
        let (flags, key, _) = leaf_at(&page, 1);
        assert_eq!(flags, BUCKET_LEAF_FLAG);
        assert_eq!(key, b"nested");
    }

    // Go: TestPage_typ
    #[test]
    fn page_type_names() {
        assert_eq!(
            PageHeader {
                flags: BRANCH_PAGE_FLAG,
                ..Default::default()
            }
            .typ(),
            "branch"
        );
        assert_eq!(
            PageHeader {
                flags: LEAF_PAGE_FLAG,
                ..Default::default()
            }
            .typ(),
            "leaf"
        );
        assert_eq!(
            PageHeader {
                flags: META_PAGE_FLAG,
                ..Default::default()
            }
            .typ(),
            "meta"
        );
        assert_eq!(
            PageHeader {
                flags: FREELIST_PAGE_FLAG,
                ..Default::default()
            }
            .typ(),
            "freelist"
        );
        assert_eq!(
            PageHeader {
                flags: 20000,
                ..Default::default()
            }
            .typ(),
            "unknown<4e20>"
        );
    }
}
