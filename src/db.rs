//! Database file, mmap, allocation, and the public [`Db`] type.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use memmap2::Mmap;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::freelist::Freelist;
use crate::inner::TxInner;
use crate::page::{
    meta_from_page, write_meta_page, InBucket, Meta, PageHeader, Pgid, Txid, DEFAULT_ALLOC_SIZE,
    DEFAULT_MAX_BATCH_DELAY_MS, DEFAULT_MAX_BATCH_SIZE, FREELIST_PAGE_FLAG, LEAF_PAGE_FLAG, MAGIC,
    MAX_MMAP_STEP, META_SIZE, PAGE_HEADER_SIZE, VERSION,
};
use crate::tx::Tx;
use std::cell::RefCell;
use std::rc::Rc;

pub const FLOCK_RETRY: Duration = Duration::from_millis(50);

pub struct FlagLock {
    locked: Mutex<bool>,
    cv: Condvar,
}

impl FlagLock {
    fn new() -> Self {
        Self {
            locked: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    pub fn lock(&self) {
        let mut g = self.locked.lock();
        while *g {
            self.cv.wait(&mut g);
        }
        *g = true;
    }

    pub fn unlock(&self) {
        let mut g = self.locked.lock();
        *g = false;
        self.cv.notify_one();
    }
}

pub struct MmapSlot {
    pub mmap: Option<Mmap>,
    pub datasz: usize,
}

pub struct DbInner {
    pub path: PathBuf,
    pub file: File,
    pub page_size: usize,
    pub no_sync: bool,
    pub no_grow_sync: bool,
    pub read_only: bool,
    pub max_size: usize,
    pub alloc_size: usize,
    pub max_batch_size: usize,
    pub max_batch_delay: Duration,
    pub mmap: RwLock<MmapSlot>,
    pub writer: FlagLock,
    pub metalock: Mutex<()>,
    pub committed_meta: Mutex<Meta>,
    pub freelist: Mutex<Freelist>,
    pub opened: AtomicBool,
    pub batch: Mutex<crate::batch::BatchState>,
}

impl DbInner {
    pub fn io(&self, e: std::io::Error) -> Error {
        Error::io(&self.path, e)
    }

    pub fn read_page(&self, pgid: Pgid) -> Result<Vec<u8>> {
        let slot = self.mmap.read();
        let mmap = slot.mmap.as_ref().ok_or(Error::InvalidMapping)?;
        let start = pgid as usize * self.page_size;
        if start + PAGE_HEADER_SIZE > mmap.len() {
            return Err(Error::Corrupt(format!(
                "page {pgid} starts past mmap ({} bytes)",
                mmap.len()
            )));
        }
        let hdr = PageHeader::read(&mmap[start..]);
        let n = (hdr.overflow as usize + 1) * self.page_size;
        if start + n > mmap.len() {
            return Err(Error::Corrupt(format!(
                "page {pgid} overflow extends past mmap"
            )));
        }
        Ok(mmap[start..start + n].to_vec())
    }

    pub fn write_at(&self, buf: &[u8], off: u64) -> Result<()> {
        self.file.write_all_at(buf, off).map_err(|e| self.io(e))
    }

    pub fn fdatasync(&self) -> Result<()> {
        // SAFETY: `self.file` is an open fd owned by this `DbInner`.
        let rc = unsafe { libc::fdatasync(self.file.as_raw_fd()) };
        if rc != 0 {
            return Err(self.io(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    pub fn file_size(&self) -> Result<u64> {
        Ok(self.file.metadata().map_err(|e| self.io(e))?.len())
    }

    #[allow(dead_code)]
    pub fn mmap_size(page_size: usize, size: usize) -> Result<usize> {
        for i in 15..=30 {
            if size <= 1 << i {
                return Ok(1 << i);
            }
        }
        if size > isize::MAX as usize {
            return Err(Error::Corrupt("mmap too large".into()));
        }
        let mut sz = size as u64;
        if sz % MAX_MMAP_STEP as u64 != 0 {
            sz += MAX_MMAP_STEP as u64 - (sz % MAX_MMAP_STEP as u64);
        }
        let ps = page_size as u64;
        if sz % ps != 0 {
            sz = ((sz / ps) + 1) * ps;
        }
        Ok(sz as usize)
    }

    #[allow(dead_code)]
    pub fn remap(&self, minsz: usize) -> Result<()> {
        let file_size = self.file_size()? as usize;
        let mut size = file_size.max(minsz);
        size = Self::mmap_size(self.page_size, size)?;
        if self.max_size > 0 && size > self.max_size && size > file_size {
            // grow is checked at allocate time; mapping extra is ok on unix
        }
        let mut slot = self.mmap.write();
        slot.mmap = None;
        let mmap = map_file(
            &self.file,
            &self.path,
            size.min(file_size.max(self.page_size * 2)),
        )?;
        slot.datasz = mmap.len();
        slot.mmap = Some(mmap);
        Ok(())
    }

    /// Grow the mmap to at least `minsz` bytes, extending the mapping as the file grows.
    pub fn ensure_mapped(&self, minsz: usize) -> Result<()> {
        {
            let slot = self.mmap.read();
            if slot.datasz >= minsz && slot.mmap.is_some() {
                return Ok(());
            }
        }
        let file_size = self.file_size()? as usize;
        let need = minsz.max(file_size);
        let map_len = need.max(file_size);
        let mut slot = self.mmap.write();
        slot.mmap = None;
        let mmap = map_file(&self.file, &self.path, map_len)?;
        slot.datasz = mmap.len();
        slot.mmap = Some(mmap);
        Ok(())
    }

    pub fn grow(&self, sz: usize) -> Result<()> {
        let file_size = self.file_size()? as usize;
        if sz <= file_size {
            return Ok(());
        }
        let sz = self.grow_size(sz);
        if !self.no_grow_sync && !self.read_only {
            self.file.set_len(sz as u64).map_err(|e| self.io(e))?;
            self.file.sync_all().map_err(|e| self.io(e))?;
        }
        Ok(())
    }

    fn grow_size(&self, grow_size: usize) -> usize {
        let datasz = self.mmap.read().datasz;
        if datasz <= self.alloc_size {
            datasz.max(grow_size)
        } else {
            grow_size + self.alloc_size
        }
    }

    pub fn allocate(
        &self,
        txid: Txid,
        count: usize,
        meta_pgid: &mut Pgid,
    ) -> Result<(Pgid, Vec<u8>)> {
        let mut buf = vec![0u8; count * self.page_size];
        crate::page::set_page_overflow(&mut buf, (count - 1) as u32);
        let id = self.freelist.lock().allocate(txid, count);
        if id != 0 {
            crate::page::set_page_id(&mut buf, id);
            return Ok((id, buf));
        }
        let id = *meta_pgid;
        let minsz = (id as usize + count + 1) * self.page_size;
        if self.max_size > 0 && minsz > self.max_size {
            return Err(Error::MaxSizeReached);
        }
        if minsz > self.mmap.read().datasz {
            // File may still be smaller; mapping uses file size. Allocation
            // assigns pgids; grow() extends the file at commit.
            let _ = self.ensure_mapped(self.file_size()? as usize);
        }
        *meta_pgid = id + count as Pgid;
        crate::page::set_page_id(&mut buf, id);
        Ok((id, buf))
    }

    pub fn load_freelist(&self) -> Result<()> {
        let meta = self.committed_meta.lock().clone();
        let mut fl = self.freelist.lock();
        if !meta.is_freelist_persisted() {
            // Reconstructing via a full scan is not implemented; default
            // path always persists the freelist.
            fl.init(Vec::new());
            return Ok(());
        }
        let page = self.read_page(meta.freelist)?;
        fl.read_page(&page);
        Ok(())
    }
}

/// Options for [`Db::open`].
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub timeout: Option<Duration>,
    pub no_grow_sync: bool,
    pub no_sync: bool,
    pub read_only: bool,
    pub page_size: usize,
    pub initial_mmap_size: usize,
    pub max_size: usize,
    pub no_freelist_sync: bool,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }
}

/// An open bbolt database (a single file).
pub struct Db {
    pub(crate) inner: Arc<DbInner>,
}

impl Clone for Db {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Db {
    /// Open or create a database file.
    ///
    /// `mode` is the Unix file mode used when creating the file (e.g. `0o600`).
    pub fn open<P: AsRef<Path>>(path: P, mode: u32, options: Option<Options>) -> Result<Self> {
        let path = path.as_ref();
        let opts = options.unwrap_or_default();
        let page_size = if opts.page_size == 0 {
            os_page_size()
        } else {
            opts.page_size
        };
        if page_size < 1024 || !page_size.is_power_of_two() {
            return Err(Error::Corrupt(format!(
                "page size must be a power of two >= 1024, got {page_size}"
            )));
        }

        let mut open = OpenOptions::new();
        open.read(true);
        if opts.read_only {
            open.write(false);
        } else {
            open.write(true).create(true).mode(mode);
        }
        let mut file = open.open(path).map_err(|e| Error::io(path, e))?;

        flock(&file, !opts.read_only, opts.timeout)?;

        let file_len = file.metadata().map_err(|e| Error::io(path, e))?.len();
        let mut page_size = page_size;
        if file_len == 0 {
            init_file(&mut file, path, page_size)?;
        } else {
            page_size = get_page_size(&mut file, path, page_size)?;
        }

        let mmap = {
            let len = file.metadata().map_err(|e| Error::io(path, e))?.len() as usize;
            let mmap = map_file(&file, path, len)?;
            MmapSlot {
                datasz: mmap.len(),
                mmap: Some(mmap),
            }
        };

        let meta = pick_meta(&mmap).ok_or(Error::Invalid)?;

        let inner = Arc::new(DbInner {
            path: path.to_path_buf(),
            file,
            page_size,
            no_sync: opts.no_sync,
            no_grow_sync: opts.no_grow_sync,
            read_only: opts.read_only,
            max_size: opts.max_size,
            alloc_size: DEFAULT_ALLOC_SIZE,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_batch_delay: Duration::from_millis(DEFAULT_MAX_BATCH_DELAY_MS),
            mmap: RwLock::new(mmap),
            writer: FlagLock::new(),
            metalock: Mutex::new(()),
            committed_meta: Mutex::new(meta),
            freelist: Mutex::new(Freelist::new()),
            opened: AtomicBool::new(true),
            batch: Mutex::new(crate::batch::BatchState::default()),
        });

        if !opts.read_only {
            inner.load_freelist()?;
        } else if !opts.no_freelist_sync {
            // read-only may still load freelist for stats; skip if not requested
        }

        if !opts.read_only && !opts.no_freelist_sync {
            let persisted = inner.committed_meta.lock().is_freelist_persisted();
            if !persisted {
                let db = Self {
                    inner: Arc::clone(&inner),
                };
                db.update(|_| Ok(()))?;
            }
        }

        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn page_size(&self) -> usize {
        self.inner.page_size
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.fdatasync()
    }

    /// Start a transaction. Only one write transaction may be active at a time.
    pub fn begin(&self, writable: bool) -> Result<Tx> {
        if !self.inner.opened.load(Ordering::SeqCst) {
            return Err(Error::DatabaseNotOpen);
        }
        if writable {
            if self.inner.read_only {
                return Err(Error::DatabaseReadOnly);
            }
            self.inner.writer.lock();
            let _g = self.inner.metalock.lock();
            if !self.inner.opened.load(Ordering::SeqCst) {
                self.inner.writer.unlock();
                return Err(Error::DatabaseNotOpen);
            }
            if self.inner.mmap.read().mmap.is_none() {
                self.inner.writer.unlock();
                return Err(Error::InvalidMapping);
            }
            let mut meta = self.inner.committed_meta.lock().clone();
            meta.txid += 1;
            self.inner.freelist.lock().release_pending_pages();
            let tx = TxInner::new(Arc::clone(&self.inner), meta, true);
            Ok(Tx {
                inner: Rc::new(RefCell::new(tx)),
            })
        } else {
            let _g = self.inner.metalock.lock();
            if !self.inner.opened.load(Ordering::SeqCst) {
                return Err(Error::DatabaseNotOpen);
            }
            if self.inner.mmap.read().mmap.is_none() {
                return Err(Error::InvalidMapping);
            }
            let meta = self.inner.committed_meta.lock().clone();
            self.inner.freelist.lock().add_readonly_txid(meta.txid);
            let tx = TxInner::new(Arc::clone(&self.inner), meta, false);
            Ok(Tx {
                inner: Rc::new(RefCell::new(tx)),
            })
        }
    }

    /// Run `f` inside a managed write transaction.
    /// Returning `Ok` commits; `Err` rolls back.
    pub fn update<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Tx) -> Result<T>,
    {
        let tx = self.begin(true)?;
        tx.inner.borrow_mut().managed = true;
        let result = f(&tx);
        tx.inner.borrow_mut().managed = false;
        match result {
            Ok(v) => {
                tx.commit()?;
                Ok(v)
            }
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            }
        }
    }

    /// Run `f` inside a managed read-only transaction.
    pub fn view<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Tx) -> Result<T>,
    {
        let tx = self.begin(false)?;
        tx.inner.borrow_mut().managed = true;
        let result = f(&tx);
        tx.inner.borrow_mut().managed = false;
        let rb = tx.rollback();
        match result {
            Ok(v) => {
                rb?;
                Ok(v)
            }
            Err(e) => {
                let _ = rb;
                Err(e)
            }
        }
    }

    /// Combine concurrent writers. The function must be idempotent; it may run more than once.
    pub fn batch<F>(&self, f: F) -> Result<()>
    where
        F: Fn(&Tx) -> Result<()> + Send + 'static,
    {
        crate::batch::batch(self, f)
    }

    pub fn close(&self) -> Result<()> {
        if !self.inner.opened.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.writer.lock();
        let _m = self.inner.metalock.lock();
        {
            let mut slot = self.inner.mmap.write();
            slot.mmap = None;
            slot.datasz = 0;
        }
        if !self.inner.read_only {
            let _ = funlock(&self.inner.file);
        }
        self.inner.writer.unlock();
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.close();
        }
    }
}

fn os_page_size() -> usize {
    // SAFETY: sysconf(_SC_PAGESIZE) has no preconditions.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if n > 0 {
        n as usize
    } else {
        4096
    }
}

fn flock(file: &File, exclusive: bool, timeout: Option<Duration>) -> Result<()> {
    let start = Instant::now();
    let mut flag = libc::LOCK_NB;
    flag |= if exclusive {
        libc::LOCK_EX
    } else {
        libc::LOCK_SH
    };
    loop {
        // SAFETY: `file` is open; flock is the POSIX advisory-lock interface.
        let rc = unsafe { libc::flock(file.as_raw_fd(), flag) };
        if rc == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        let code = err.raw_os_error();
        if code != Some(libc::EWOULDBLOCK) && code != Some(libc::EAGAIN) {
            return Err(Error::io("<flock>", err));
        }
        if let Some(t) = timeout {
            if start.elapsed() + FLOCK_RETRY >= t {
                return Err(Error::Timeout);
            }
        }
        std::thread::sleep(FLOCK_RETRY);
    }
}

fn funlock(file: &File) -> Result<()> {
    // SAFETY: `file` is open; LOCK_UN releases a lock we acquired.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if rc != 0 {
        return Err(Error::io("<funlock>", std::io::Error::last_os_error()));
    }
    Ok(())
}

fn map_file(file: &File, path: impl AsRef<Path>, len: usize) -> Result<Mmap> {
    // SAFETY: `file` remains open for the lifetime of the mapping. Callers drop
    // the `Mmap` before closing the file. Like upstream bbolt, an external
    // truncate of the file can cause SIGBUS on access.
    let path = path.as_ref();
    unsafe {
        memmap2::MmapOptions::new()
            .len(len)
            .map(file)
            .map_err(|e| Error::io(path, e))
    }
}

fn init_file(file: &mut File, path: &Path, page_size: usize) -> Result<()> {
    let mut buf = vec![0u8; page_size * 4];
    for i in 0..2u64 {
        let mut meta = Meta {
            magic: MAGIC,
            version: VERSION,
            page_size: page_size as u32,
            flags: 0,
            root: InBucket::new(3, 0),
            freelist: 2,
            pgid: 4,
            txid: i,
            checksum: 0,
        };
        meta.finish_checksum();
        write_meta_page(&mut buf[i as usize * page_size..], &meta);
        // write_meta_page sets id from txid%2, which matches i.
    }
    // page 2: empty freelist
    let p2 = &mut buf[2 * page_size..3 * page_size];
    crate::page::set_page_id(p2, 2);
    crate::page::set_page_flags(p2, FREELIST_PAGE_FLAG);
    crate::page::set_page_count(p2, 0);
    // page 3: empty leaf (root bucket)
    let p3 = &mut buf[3 * page_size..4 * page_size];
    crate::page::set_page_id(p3, 3);
    crate::page::set_page_flags(p3, LEAF_PAGE_FLAG);
    crate::page::set_page_count(p3, 0);

    file.write_all(&buf).map_err(|e| Error::io(path, e))?;
    file.sync_all().map_err(|e| Error::io(path, e))?;
    Ok(())
}

fn get_page_size(file: &mut File, path: &Path, fallback: usize) -> Result<usize> {
    let mut buf = [0u8; 4096];
    file.seek(SeekFrom::Start(0))
        .map_err(|e| Error::io(path, e))?;
    let n = file.read(&mut buf).map_err(|e| Error::io(path, e))?;
    if n >= PAGE_HEADER_SIZE + META_SIZE {
        let m = meta_from_page(&buf);
        if m.validate().is_ok() && m.page_size != 0 {
            return Ok(m.page_size as usize);
        }
    }
    let file_size = file.metadata().map_err(|e| Error::io(path, e))?.len();
    for i in 0..=14u32 {
        let pos = 1024u64 << i;
        if pos >= file_size.saturating_sub(1024) {
            break;
        }
        file.seek(SeekFrom::Start(pos))
            .map_err(|e| Error::io(path, e))?;
        let n = file.read(&mut buf).map_err(|e| Error::io(path, e))?;
        if n >= PAGE_HEADER_SIZE + META_SIZE {
            let m = meta_from_page(&buf);
            if m.validate().is_ok() && m.page_size != 0 {
                return Ok(m.page_size as usize);
            }
        }
    }
    if n >= PAGE_HEADER_SIZE {
        return Ok(fallback);
    }
    Err(Error::Invalid)
}

fn pick_meta(slot: &MmapSlot) -> Option<Meta> {
    let mmap = slot.mmap.as_ref()?;
    if mmap.len() < PAGE_HEADER_SIZE + META_SIZE {
        return None;
    }
    let hdr0 = PageHeader::read(mmap);
    let page_size = {
        let m = meta_from_page(mmap);
        if m.page_size == 0 {
            return None;
        }
        m.page_size as usize
    };
    let meta0 = if hdr0.is_meta() {
        let m = meta_from_page(mmap);
        if m.validate().is_ok() {
            Some(m)
        } else {
            None
        }
    } else {
        None
    };
    let meta1 = if mmap.len() >= page_size + PAGE_HEADER_SIZE + META_SIZE {
        let p1 = &mmap[page_size..];
        let m = meta_from_page(p1);
        if m.validate().is_ok() {
            Some(m)
        } else {
            None
        }
    } else {
        None
    };
    match (meta0, meta1) {
        (Some(a), Some(b)) => {
            if a.txid > b.txid {
                Some(a)
            } else {
                Some(b)
            }
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn init_matches_go_fixture() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.db");
        let db = Db::open(
            &path,
            0o600,
            Some(Options {
                page_size: 4096,
                ..Options::default()
            }),
        )
        .unwrap();
        db.close().unwrap();
        let got = std::fs::read(&path).unwrap();
        let exp = std::fs::read("tests/fixtures/go_init.db").unwrap();
        assert_eq!(got.len(), exp.len());
        assert_eq!(got, exp);
    }
}
