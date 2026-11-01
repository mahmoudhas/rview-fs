//! A file of another node, once fetched: in a memfd, which the kernel can pass reads and
//! mappings through to, and mapped, for the reads rview-fs serves itself.

use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::FromRawFd as _;
use std::sync::Arc;

use bytes::Bytes;

#[derive(Clone)]
pub struct Content {
    pub file: Arc<File>,
    pub bytes: Bytes,
}

/// A memfd of `size` bytes named `name` (seen in /proc/<pid>/fd), and its mapping to
/// write the bytes into. None for an empty file, which has nothing to map.
pub fn memfd(name: &str, size: u64) -> io::Result<(File, Option<memmap2::MmapMut>)> {
    let name = CString::new(name.replace('\0', "")).map_err(io::Error::other)?;
    // SAFETY: a valid C string, and the fd returned is ours alone.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a new fd, owned by nothing else.
    let file = unsafe { File::from_raw_fd(fd) };
    file.set_len(size)?;
    if size == 0 {
        return Ok((file, None));
    }
    // SAFETY: the memfd is ours; nothing else maps or resizes it.
    let map = unsafe { memmap2::MmapMut::map_mut(&file)? };
    Ok((file, Some(map)))
}

impl Content {
    /// The memfd once its bytes are written into `map`.
    pub fn new(file: File, map: Option<memmap2::MmapMut>) -> io::Result<Self> {
        let bytes = match map {
            Some(map) => Bytes::from_owner(map.make_read_only()?),
            None => Bytes::new(),
        };
        Ok(Self {
            file: Arc::new(file),
            bytes,
        })
    }
}

#[cfg(test)]
#[path = "content_test.rs"]
mod tests;
