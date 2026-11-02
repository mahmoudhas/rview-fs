//! Memory a file of another node is fetched into: anonymous, in transparent huge pages
//! where the kernel gives them (`transparent_hugepage` set to `madvise` or `always`), and
//! read-only once written.

use std::io;

use bytes::Bytes;
use memmap2::MmapMut;

/// Room for `size` bytes to fetch into. None for an empty file, which needs none.
pub fn buffer(size: u64) -> io::Result<Option<MmapMut>> {
    if size == 0 {
        return Ok(None);
    }
    let len = usize::try_from(size).map_err(io::Error::other)?;
    let map = MmapMut::map_anon(len)?;
    // Fewer pages to fault in, and fewer TLB misses copying in and reading out.
    if let Err(e) = map.advise(memmap2::Advice::HugePage) {
        tracing::debug!(%e, "no huge pages for a fetched file");
    }
    Ok(Some(map))
}

/// The bytes fetched into `map`, read-only.
pub fn freeze(map: Option<MmapMut>) -> io::Result<Bytes> {
    match map {
        Some(map) => Ok(Bytes::from_owner(map.make_read_only()?)),
        None => Ok(Bytes::new()),
    }
}

#[cfg(test)]
#[path = "content_test.rs"]
mod tests;
