use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::Mmap;

use crate::error::{Error, Result};
use crate::platform::io_err;

pub const FLOCK_RETRY: Duration = Duration::from_millis(50);

pub fn open_db_file(path: &Path, read_only: bool, mode: u32) -> Result<File> {
    let mut open = OpenOptions::new();
    open.read(true);
    if read_only {
        open.write(false);
    } else {
        open.write(true).create(true).mode(mode);
    }
    open.open(path).map_err(|e| io_err(path, e))
}

pub fn apply_create_mode(opts: &mut OpenOptions, mode: u32) {
    opts.mode(mode);
}

pub fn flock(file: &File, exclusive: bool, timeout: Option<Duration>) -> Result<()> {
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

pub fn funlock(file: &File) -> Result<()> {
    // SAFETY: `file` is open; LOCK_UN releases a lock we acquired.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if rc != 0 {
        return Err(Error::io("<funlock>", std::io::Error::last_os_error()));
    }
    Ok(())
}

pub fn fdatasync(file: &File) -> Result<()> {
    fdatasync_impl(file)
}

/// Linux/Android: POSIX fdatasync (matches Go boltsync_linux.go).
#[cfg(any(target_os = "linux", target_os = "android"))]
fn fdatasync_impl(file: &File) -> Result<()> {
    // SAFETY: `file` is an open fd.
    let rc = unsafe { libc::fdatasync(file.as_raw_fd()) };
    if rc != 0 {
        return Err(Error::io("<fdatasync>", std::io::Error::last_os_error()));
    }
    Ok(())
}

/// Darwin: fcntl(F_FULLFSYNC), matching Go os.File.Sync() on macOS.
#[cfg(target_os = "macos")]
fn fdatasync_impl(file: &File) -> Result<()> {
    // SAFETY: `file` is an open fd; F_FULLFSYNC takes no extra arg.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    if rc != 0 {
        return Err(Error::io("<F_FULLFSYNC>", std::io::Error::last_os_error()));
    }
    Ok(())
}

/// Other Unix: fsync via std (Go boltsync_unix.go uses file.Sync()).
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn fdatasync_impl(file: &File) -> Result<()> {
    file.sync_all()
        .map_err(|e| Error::io("<File::sync_all>", e))
}

pub fn map_file(file: &File, path: &Path, len: usize, _mmap_flags: i32) -> Result<Mmap> {
    // Note: Go passes MAP_SHARED|db.MmapFlags. memmap2 maps shared read-only;
    // extra Linux MAP_* flags from Options::mmap_flags are accepted for API
    // parity but not applied through memmap2 (documented in README).
    let _ = _mmap_flags;
    // SAFETY: `file` remains open for the lifetime of the mapping.
    unsafe {
        memmap2::MmapOptions::new()
            .len(len)
            .map(file)
            .map_err(|e| io_err(path, e))
    }
}

pub fn write_at(file: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    file.write_all_at(buf, off)
}

pub fn read_at(file: &File, buf: &mut [u8], off: u64) -> std::io::Result<usize> {
    file.read_at(buf, off)
}

pub fn os_page_size() -> usize {
    // SAFETY: sysconf(_SC_PAGESIZE) has no preconditions.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if n > 0 {
        n as usize
    } else {
        4096
    }
}

#[allow(dead_code)]
pub fn truncate_for_mmap(_file: &File, _sz: u64) -> Result<()> {
    // On Unix, InitialMmapSize does not truncate/grow the file (Go behavior).
    Ok(())
}
