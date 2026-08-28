//! Windows file lock / sync / mmap paths (parity with Go `bolt_windows.go`).
//!
//! These paths compile on Windows targets. They are not exercised on this Linux
//! CI/agent VM; treat them as best-effort ports of upstream syscall usage.

use std::fs::{File, OpenOptions};
use std::os::windows::fs::FileExt;
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::time::{Duration, Instant};

use memmap2::Mmap;
use windows_sys::Win32::Foundation::{ERROR_LOCK_VIOLATION, HANDLE};
use windows_sys::Win32::Storage::FileSystem::{
    LockFileEx, UnlockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

use crate::error::{Error, Result};
use crate::platform::io_err;

pub const FLOCK_RETRY: Duration = Duration::from_millis(50);

pub fn open_db_file(path: &Path, read_only: bool, _mode: u32) -> Result<File> {
    let mut open = OpenOptions::new();
    open.read(true);
    if read_only {
        open.write(false);
    } else {
        open.write(true).create(true);
    }
    open.open(path).map_err(|e| io_err(path, e))
}

pub fn apply_create_mode(_opts: &mut OpenOptions, _mode: u32) {}

pub fn flock(file: &File, exclusive: bool, timeout: Option<Duration>) -> Result<()> {
    let start = Instant::now();
    let mut flags = LOCKFILE_FAIL_IMMEDIATELY;
    if exclusive {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    // Byte-range -1..0 as in Go bbolt (issue #121).
    let m1: u32 = u32::MAX;
    loop {
        let mut overlapped = OVERLAPPED {
            Internal: 0,
            InternalHigh: 0,
            Anonymous: unsafe { std::mem::zeroed() },
            hEvent: std::ptr::null_mut(),
        };
        // SAFETY: OVERLAPPED Offset/OffsetHigh layout via Anonymous union.
        unsafe {
            let anon = &mut overlapped.Anonymous as *mut _ as *mut u64;
            // Offset at start of Anonymous; OffsetHigh follows (Windows layout).
            let parts = anon as *mut u32;
            *parts = m1;
            *parts.add(1) = m1;
        }
        // SAFETY: LockFileEx with our open handle; overlapped is stack-local.
        let ok = unsafe {
            LockFileEx(
                file.as_raw_handle() as HANDLE,
                flags,
                0,
                1,
                0,
                &mut overlapped,
            )
        };
        if ok != 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(ERROR_LOCK_VIOLATION as i32) {
            return Err(Error::io("<LockFileEx>", err));
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
    let m1: u32 = u32::MAX;
    let mut overlapped = OVERLAPPED {
        Internal: 0,
        InternalHigh: 0,
        Anonymous: unsafe { std::mem::zeroed() },
        hEvent: std::ptr::null_mut(),
    };
    unsafe {
        let parts = &mut overlapped.Anonymous as *mut _ as *mut u32;
        *parts = m1;
        *parts.add(1) = m1;
    }
    // SAFETY: UnlockFileEx for a lock we acquired on this handle.
    let ok = unsafe {
        UnlockFileEx(
            file.as_raw_handle() as HANDLE,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if ok == 0 {
        return Err(Error::io(
            "<UnlockFileEx>",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(())
}

pub fn fdatasync(file: &File) -> Result<()> {
    file.sync_all()
        .map_err(|e| Error::io("<File::sync_all>", e))
}

pub fn map_file(file: &File, path: &Path, len: usize, _mmap_flags: i32) -> Result<Mmap> {
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
    let mut written = 0;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], off + written as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        written += n;
    }
    Ok(())
}

pub fn read_at(file: &File, buf: &mut [u8], off: u64) -> std::io::Result<usize> {
    file.seek_read(buf, off)
}

pub fn os_page_size() -> usize {
    // SAFETY: GetSystemInfo fills SYSTEM_INFO; no preconditions beyond writable out-param.
    unsafe {
        let mut info: windows_sys::Win32::System::SystemInformation::SYSTEM_INFO =
            std::mem::zeroed();
        windows_sys::Win32::System::SystemInformation::GetSystemInfo(&mut info);
        let n = info.dwPageSize as usize;
        if n > 0 {
            n
        } else {
            4096
        }
    }
}

pub fn truncate_for_mmap(file: &File, sz: u64) -> Result<()> {
    // Go truncates the database to the mmap size on Windows before mapping.
    file.set_len(sz)
        .map_err(|e| Error::io("<truncate>", e))
}
