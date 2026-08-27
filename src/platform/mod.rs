//! OS-specific file locking, sync, and mapping helpers.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

use memmap2::Mmap;

use crate::error::{Error, Result};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix as os;
#[cfg(windows)]
use windows as os;

pub fn open_db_file(path: &Path, read_only: bool, mode: u32) -> Result<File> {
    os::open_db_file(path, read_only, mode)
}

pub fn flock(file: &File, exclusive: bool, timeout: Option<Duration>) -> Result<()> {
    os::flock(file, exclusive, timeout)
}

pub fn funlock(file: &File) -> Result<()> {
    os::funlock(file)
}

pub fn fdatasync(file: &File) -> Result<()> {
    os::fdatasync(file)
}

pub fn map_file(file: &File, path: &Path, len: usize, mmap_flags: i32) -> Result<Mmap> {
    os::map_file(file, path, len, mmap_flags)
}

pub fn write_at(file: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    os::write_at(file, buf, off)
}

pub fn read_at(file: &File, buf: &mut [u8], off: u64) -> std::io::Result<usize> {
    os::read_at(file, buf, off)
}

pub fn os_page_size() -> usize {
    os::os_page_size()
}

#[allow(dead_code)]
pub fn truncate_for_mmap(file: &File, sz: u64) -> Result<()> {
    os::truncate_for_mmap(file, sz)
}

/// Read exactly `buf.len()` bytes at `off`, or error.
pub fn read_exact_at(file: &File, buf: &mut [u8], off: u64) -> std::io::Result<()> {
    let mut got = 0;
    while got < buf.len() {
        let n = read_at(file, &mut buf[got..], off + got as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof",
            ));
        }
        got += n;
    }
    Ok(())
}

/// Ensure OpenOptions can create with a Unix mode; no-op mode on Windows.
#[allow(dead_code)]
pub fn apply_create_mode(opts: &mut OpenOptions, mode: u32) {
    os::apply_create_mode(opts, mode);
}

#[allow(dead_code)]
pub fn sync_file(file: &mut File) -> std::io::Result<()> {
    file.sync_all()
}

#[allow(dead_code)]
pub fn seek_start(file: &mut File) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    Ok(())
}

#[allow(dead_code)]
pub fn write_all(file: &mut File, buf: &[u8]) -> std::io::Result<()> {
    file.write_all(buf)
}

#[allow(dead_code)]
pub fn read_to_end(file: &mut File, buf: &mut Vec<u8>) -> std::io::Result<usize> {
    file.read_to_end(buf)
}

pub(crate) fn io_err(path: impl AsRef<Path>, e: std::io::Error) -> Error {
    Error::io(path.as_ref(), e)
}
