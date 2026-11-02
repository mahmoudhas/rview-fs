//! Files the kernel reads itself (FUSE passthrough, Linux 6.9): a file opened is
//! registered with the FUSE device as a backing file, and the kernel then reads and maps
//! it with no request to rview-fs.
//!
//! The kernel takes one backing file per inode at a time, and refuses another (EBUSY)
//! while an open of the inode is passed through to the first. So every open of an inode
//! shares one backing file, registered by the first open and closed with the last.

use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::health::Degraded;

/// struct fuse_backing_map, from linux/fuse.h.
#[repr(C)]
struct BackingMap {
    fd: i32,
    flags: u32,
    padding: u64,
}

/// _IOW(FUSE_DEV_IOC_MAGIC, nr, size), as linux/fuse.h defines the backing ioctls.
/// fractal-fuse 0.6.1 encodes them otherwise, which the kernel answers with ENOTTY.
const fn fuse_dev_iow(nr: libc::c_ulong, size: usize) -> libc::c_ulong {
    (1 << 30) | ((size as libc::c_ulong) << 16) | (229 << 8) | nr
}

const FUSE_DEV_IOC_BACKING_OPEN: libc::c_ulong = fuse_dev_iow(1, std::mem::size_of::<BackingMap>());
const FUSE_DEV_IOC_BACKING_CLOSE: libc::c_ulong = fuse_dev_iow(2, std::mem::size_of::<u32>());

fn backing_open(fuse: RawFd, file: RawFd) -> io::Result<i32> {
    let map = BackingMap {
        fd: file,
        flags: 0,
        padding: 0,
    };
    // SAFETY: the kernel reads a struct fuse_backing_map from the pointer, which is valid
    // for the call.
    let id = unsafe { libc::ioctl(fuse, FUSE_DEV_IOC_BACKING_OPEN, &map) };
    if id < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(id)
}

fn backing_close(fuse: RawFd, id: i32) -> io::Result<()> {
    let id = id as u32;
    // SAFETY: the kernel reads a u32 from the pointer, which is valid for the call.
    if unsafe { libc::ioctl(fuse, FUSE_DEV_IOC_BACKING_CLOSE, &id) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The backing file of each inode passed through, and how many opens use it.
#[derive(Default)]
struct Backings {
    by_inode: HashMap<u64, (i32, usize)>,
}

impl Backings {
    /// The backing file of `ino`, for one more open of it.
    fn reuse(&mut self, ino: u64) -> Option<i32> {
        let (id, opens) = self.by_inode.get_mut(&ino)?;
        *opens += 1;
        Some(*id)
    }

    /// Make `id` the backing file of `ino`, for one open of it; unless another open
    /// registered one meanwhile, which it then uses, and `id` is left over.
    fn adopt(&mut self, ino: u64, id: i32) -> (i32, Option<i32>) {
        match self.reuse(ino) {
            Some(theirs) => (theirs, Some(id)),
            None => {
                self.by_inode.insert(ino, (id, 1));
                (id, None)
            }
        }
    }

    /// One open of `ino` is done: the backing file to close, when it was the last.
    fn release(&mut self, ino: u64) -> Option<i32> {
        let (id, opens) = self.by_inode.get_mut(&ino)?;
        *opens -= 1;
        if *opens > 0 {
            return None;
        }
        let id = *id;
        self.by_inode.remove(&ino);
        Some(id)
    }
}

/// An empty memfd named `name`, to try passthrough with.
fn memfd(name: &str) -> io::Result<File> {
    let name = CString::new(name).map_err(io::Error::other)?;
    // SAFETY: a valid C string, and the fd returned is ours alone.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a new fd, owned by nothing else.
    Ok(unsafe { File::from_raw_fd(fd) })
}

pub struct Passthrough {
    fuse: Arc<OwnedFd>,
    degraded: Arc<Degraded>,
    /// Off for good once the kernel, or our privileges, refuse it.
    on: AtomicBool,
    confirmed: AtomicBool,
    // Taken for a moment by every open and release; the ioctls run after it is released.
    backings: Mutex<Backings>,
}

impl Passthrough {
    /// Files registered with the FUSE device `fuse`; what cannot be, reported to
    /// `degraded`.
    pub fn new(fuse: Arc<OwnedFd>, degraded: Arc<Degraded>) -> Self {
        Self {
            fuse,
            degraded,
            on: AtomicBool::new(true),
            confirmed: AtomicBool::new(false),
            backings: Mutex::default(),
        }
    }

    pub fn is_on(&self) -> bool {
        self.on.load(Ordering::Relaxed)
    }

    /// Try passthrough with an empty memfd, once the mount answers: the kernel grants it
    /// as it takes the reply to INIT.
    pub fn probe(&self) {
        match memfd("rview-fs-probe") {
            Ok(file) => {
                if let Some(id) = self.register(&file) {
                    self.unregister(id);
                }
            }
            Err(e) => tracing::error!(%e, "could not make a memfd to try passthrough with"),
        }
    }

    /// The backing file of inode `ino`, when an open of it is passed through already: one
    /// more open of it uses it too.
    pub fn reuse(&self, ino: u64) -> Option<i32> {
        if !self.is_on() {
            return None;
        }
        self.backings.lock().reuse(ino)
    }

    /// Pass `file` through as inode `ino`, for one open of it; or None, having said why
    /// not. Another open that did so meanwhile has its backing file used instead.
    pub fn open(&self, ino: u64, file: &File) -> Option<i32> {
        let id = self.register(file)?;
        let (id, left_over) = self.backings.lock().adopt(ino, id);
        if let Some(left_over) = left_over {
            self.unregister(left_over);
        }
        Some(id)
    }

    /// One open of inode `ino` passed through is done; its backing file goes with the
    /// last.
    pub fn release(&self, ino: u64) {
        let last = self.backings.lock().release(ino);
        if let Some(id) = last {
            self.unregister(id);
        }
    }

    /// Register `file` for the kernel to read, or None, having said why not.
    fn register(&self, file: &File) -> Option<i32> {
        if !self.is_on() {
            return None;
        }
        let e = match backing_open(self.fuse.as_raw_fd(), file.as_raw_fd()) {
            Ok(id) => {
                if !self.confirmed.swap(true, Ordering::Relaxed) {
                    tracing::info!("passthrough is on: the kernel reads files itself");
                }
                return Some(id);
            }
            Err(e) => e,
        };
        match e.raw_os_error().unwrap_or(libc::EIO) {
            // Not in the kernel, not granted when mounting, or not ours (CAP_SYS_ADMIN).
            libc::ENOTTY | libc::EOPNOTSUPP | libc::EPERM => {
                self.on.store(false, Ordering::Relaxed);
                self.degraded.report(
                    "passthrough",
                    format!(
                        "{e}: rview-fs serves every read itself, several times slower. It needs \
                         Linux 6.9 or later built with CONFIG_FUSE_PASSTHROUGH, and CAP_SYS_ADMIN"
                    ),
                );
            }
            libc::ELOOP => self.degraded.report(
                "passthrough of this machine's files",
                format!(
                    "{e}: the export is on a stacked file system (overlayfs, for one), which the \
                     kernel does not pass through, so rview-fs serves its files itself, several \
                     times slower. Put the export on a plain file system"
                ),
            ),
            _ => self.degraded.report(
                "passthrough of some files",
                format!("{e}: rview-fs serves them itself"),
            ),
        }
        None
    }

    fn unregister(&self, id: i32) {
        if let Err(e) = backing_close(self.fuse.as_raw_fd(), id) {
            tracing::warn!(%e, id, "unregistering a file passed through failed");
        }
    }
}

#[cfg(test)]
#[path = "passthrough_test.rs"]
mod tests;
