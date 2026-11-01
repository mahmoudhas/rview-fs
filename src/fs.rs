//! The union as a read-only FUSE file system: the folders and files of every node's
//! export (index.rs).
//!
//! With passthrough (Linux 6.9), the kernel reads a file itself: a file of this node
//! from the export, and a file of another node from a memfd it is fetched into when it
//! is opened. Reads and mappings then never come to rview-fs.
//!
//! Otherwise rview-fs serves the reads. A file of this node is read from the local disk.
//! A file of another node is staged there when it is opened, which keeps it even if that
//! node's cleanup deletes it, and fetched into memory when any byte of it is first read:
//! a mapped page moves when the first fault touches it. Once read, the open file keeps
//! its bytes until it is closed, which a mapping of it is only once unmapped: the kernel
//! may drop the pages it read and fault them in again, and that must not need the other
//! node still up.
//!
//! FUSE requests run on fractal-fuse's threads. Anything that blocks or talks to velo
//! runs on the tokio runtime, and the request awaits it.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt as _;
use std::sync::Arc;
use std::time::Duration;

use fractal_fuse::abi::{
    FOPEN_KEEP_CACHE, FOPEN_PASSTHROUGH, FUSE_ROOT_ID, fuse_dirent_size, fuse_direntplus_size,
};
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EIO, EISDIR, ENOENT, Errno, FileAttr, FileHandleId,
    FileType, Filesystem, FsResult, InodeId, ReplyAttr, ReplyEntry, ReplyOpen, ReplyStatfs,
    Request, Timestamp,
};
use parking_lot::Mutex;

use crate::cache::Key;
use crate::content::Content;
use crate::index::{Entry, EntryKind, FileView};
use crate::passthrough::Passthrough;
use crate::path::join;
use crate::state::State;
use crate::transfer::Staged;

// Files never change once written, but they come and go.
const TTL: Duration = Duration::from_secs(1);

pub struct RviewFs {
    state: Arc<State>,
    runtime: tokio::runtime::Handle,
    passthrough: Option<Arc<Passthrough>>,
    // Taken by every request for a moment, on fractal-fuse's threads; never across an
    // await.
    inodes: Mutex<Inodes>,
    open: Mutex<OpenFiles>,
}

/// The path of every inode the kernel holds, with how many lookups it holds it by.
struct Inodes {
    by_ino: HashMap<u64, (String, u64)>,
    by_path: HashMap<String, u64>,
    next: u64,
}

impl Inodes {
    fn new() -> Self {
        Self {
            by_ino: HashMap::from([(FUSE_ROOT_ID, (String::new(), 1))]),
            by_path: HashMap::from([(String::new(), FUSE_ROOT_ID)]),
            next: FUSE_ROOT_ID + 1,
        }
    }

    fn path(&self, ino: u64) -> Option<String> {
        self.by_ino.get(&ino).map(|(path, _)| path.clone())
    }

    /// The inode of `path`, held by one more lookup.
    fn remember(&mut self, path: &str) -> u64 {
        if let Some(&ino) = self.by_path.get(path) {
            if let Some((_, lookups)) = self.by_ino.get_mut(&ino) {
                *lookups += 1;
            }
            return ino;
        }
        let ino = self.next;
        self.next += 1;
        self.by_ino.insert(ino, (path.to_owned(), 1));
        self.by_path.insert(path.to_owned(), ino);
        ino
    }

    fn forget(&mut self, ino: u64, n: u64) {
        if ino == FUSE_ROOT_ID {
            return;
        }
        let Some((path, lookups)) = self.by_ino.get_mut(&ino) else {
            return;
        };
        *lookups = lookups.saturating_sub(n);
        if *lookups == 0 {
            let path = path.clone();
            self.by_ino.remove(&ino);
            self.by_path.remove(&path);
        }
    }
}

enum Source {
    /// The kernel reads it itself, from the file registered under this id.
    Passthrough(i32),
    Local(Arc<File>),
    Remote {
        key: Key,
        // Taken by the first read that fetches the file.
        staged: Mutex<Option<Staged>>,
        // The file once read.
        content: Mutex<Option<Content>>,
    },
}

struct OpenFile {
    size: u64,
    source: Source,
}

#[derive(Default)]
struct OpenFiles {
    files: HashMap<u64, Arc<OpenFile>>,
    next: u64,
}

fn timestamp(seconds: f64) -> Timestamp {
    let seconds = seconds.max(0.0);
    Timestamp::new(seconds.trunc() as u64, (seconds.fract() * 1e9) as u32)
}

fn attr(ino: u64, entry: &Entry) -> FileAttr {
    let (kind, perm, size, mtime, nlink) = match entry {
        Entry::Dir => (FileType::Directory, 0o555, 0, 0.0, 2),
        Entry::File(file) => (FileType::RegularFile, 0o444, file.size, file.mtime, 1),
    };
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: timestamp(mtime),
        mtime: timestamp(mtime),
        ctime: timestamp(mtime),
        mode: kind.to_mode() | perm,
        nlink,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 4096,
    }
}

fn io_errno(e: &io::Error) -> Errno {
    e.raw_os_error().unwrap_or(EIO)
}

impl RviewFs {
    /// With `passthrough`, files opened are registered for the kernel to read itself.
    pub fn new(
        state: Arc<State>,
        runtime: tokio::runtime::Handle,
        passthrough: Option<Arc<Passthrough>>,
    ) -> Self {
        Self {
            state,
            runtime,
            passthrough,
            inodes: Mutex::new(Inodes::new()),
            open: Mutex::default(),
        }
    }

    fn path(&self, ino: InodeId) -> FsResult<String> {
        self.inodes.lock().path(ino.0).ok_or(ENOENT)
    }

    fn entry(&self, path: &str) -> FsResult<Entry> {
        self.state.index.read().lookup(path).ok_or(ENOENT)
    }

    fn passing_through(&self) -> bool {
        self.passthrough.as_ref().is_some_and(|p| p.is_on())
    }

    fn pass_through(&self, file: &File) -> Option<i32> {
        self.passthrough.as_ref()?.register(file)
    }

    /// Where the bytes of `file` come from: this node's disk when it has the file, else
    /// another node, which stages it for us. Passed through, a file of another node is
    /// fetched now, into a memfd the kernel reads; otherwise on the first read.
    async fn open_file(&self, file: FileView) -> FsResult<OpenFile> {
        let size = file.size;
        if file.nodes.contains(&self.state.me) {
            let path = self.state.export.join(file.path.as_str());
            match self.runtime.spawn_blocking(move || File::open(path)).await {
                Ok(Ok(local)) => {
                    let source = match self.pass_through(&local) {
                        Some(id) => Source::Passthrough(id),
                        None => Source::Local(Arc::new(local)),
                    };
                    return Ok(OpenFile { size, source });
                }
                Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => {} // deleted since
                Ok(Err(e)) => return Err(io_errno(&e)),
                Err(_) => return Err(EIO),
            }
        }
        let mut failed = false;
        for node in file.nodes.iter().filter(|n| **n != self.state.me) {
            let key = Key {
                node: node.clone(),
                path: file.path.clone(),
            };
            let cached = self.state.cache.get(&key);
            let staged = match &cached {
                Some(_) => None,
                None => {
                    let state = Arc::clone(&self.state);
                    let stage_key = key.clone();
                    match self
                        .runtime
                        .spawn(async move { state.stage(&stage_key).await })
                        .await
                    {
                        Ok(Ok(Some(staged))) => Some(staged),
                        Ok(Ok(None)) => continue,
                        Ok(Err(e)) => {
                            tracing::warn!(node = node.as_str(), path = %file.path, "staging failed: {e:#}");
                            failed = true;
                            continue;
                        }
                        Err(_) => return Err(EIO),
                    }
                }
            };
            if !self.passing_through() {
                let source = Source::Remote {
                    key,
                    staged: Mutex::new(staged),
                    content: Mutex::new(cached),
                };
                return Ok(OpenFile { size, source });
            }
            let content = match cached {
                Some(content) => content,
                None => {
                    let (state, fetch_key) = (Arc::clone(&self.state), key.clone());
                    match self
                        .runtime
                        .spawn(async move { state.remote_content(&fetch_key, staged).await })
                        .await
                    {
                        Ok(Ok(content)) => content,
                        Ok(Err(e)) => {
                            tracing::warn!(node = node.as_str(), path = %file.path, "fetching failed: {e:#}");
                            failed = true;
                            continue;
                        }
                        Err(_) => return Err(EIO),
                    }
                }
            };
            let source = match self.pass_through(&content.file) {
                Some(id) => Source::Passthrough(id),
                None => Source::Remote {
                    key,
                    staged: Mutex::new(None),
                    content: Mutex::new(Some(content)),
                },
            };
            return Ok(OpenFile { size, source });
        }
        // Gone from every node that had it, unless one could not be asked.
        Err(if failed { EIO } else { ENOENT })
    }

    async fn read_file(&self, file: &OpenFile, from: u64, to: u64, dst: &mut [u8]) -> FsResult<()> {
        match &file.source {
            // The kernel reads a file passed through itself, and never asks.
            Source::Passthrough(_) => return Err(EIO),
            Source::Local(file) => {
                let file = Arc::clone(file);
                let len = (to - from) as usize;
                let data = self
                    .runtime
                    .spawn_blocking(move || {
                        let mut data = vec![0u8; len];
                        file.read_exact_at(&mut data, from).map(|()| data)
                    })
                    .await
                    .map_err(|_| EIO)?
                    .map_err(|e| io_errno(&e))?;
                dst.copy_from_slice(&data);
            }
            Source::Remote {
                key,
                staged,
                content,
            } => {
                let held = content.lock().clone();
                let data = match held {
                    Some(held) => held.bytes,
                    None => {
                        let staged = staged.lock().take();
                        let (state, key) = (Arc::clone(&self.state), key.clone());
                        let fetched = self
                            .runtime
                            .spawn(async move { state.remote_content(&key, staged).await })
                            .await
                            .map_err(|_| EIO)?
                            .map_err(|e| {
                                tracing::error!("reading a file of another node failed: {e:#}");
                                EIO
                            })?;
                        *content.lock() = Some(fetched.clone());
                        fetched.bytes
                    }
                };
                let src = data.get(from as usize..to as usize).ok_or(EIO)?;
                dst.copy_from_slice(src);
            }
        }
        Ok(())
    }
}

impl Filesystem for RviewFs {
    async fn lookup(&self, _req: Request, parent: InodeId, name: &OsStr) -> FsResult<ReplyEntry> {
        let name = name.to_str().ok_or(ENOENT)?;
        let path = join(&self.path(parent)?, name);
        let entry = self.entry(&path)?;
        let ino = self.inodes.lock().remember(&path);
        Ok(ReplyEntry {
            ttl: TTL,
            attr: attr(ino, &entry),
            generation: 0,
        })
    }

    fn forget(&self, _req: Request, inode: InodeId, nlookup: u64) {
        self.inodes.lock().forget(inode.0, nlookup);
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: InodeId,
        _fh: Option<FileHandleId>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        let entry = self.entry(&self.path(inode)?)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: attr(inode.0, &entry),
        })
    }

    async fn open(&self, _req: Request, inode: InodeId, flags: u32) -> FsResult<ReplyOpen> {
        if flags & libc::O_ACCMODE as u32 != libc::O_RDONLY as u32 {
            return Err(libc::EROFS);
        }
        let Entry::File(file) = self.entry(&self.path(inode)?)? else {
            return Err(EISDIR);
        };
        let file = Arc::new(self.open_file(file).await?);
        let (flags, backing_id) = match &file.source {
            Source::Passthrough(id) => (FOPEN_PASSTHROUGH, *id),
            // A file never changes once written, so the kernel may keep what it read.
            Source::Local(_) | Source::Remote { .. } => (FOPEN_KEEP_CACHE, 0),
        };
        let mut open = self.open.lock();
        open.next += 1;
        let fh = open.next;
        open.files.insert(fh, file);
        Ok(ReplyOpen {
            fh: FileHandleId(fh),
            flags,
            backing_id,
        })
    }

    async fn read(
        &self,
        _req: Request,
        _inode: InodeId,
        fh: FileHandleId,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        let file = self
            .open
            .lock()
            .files
            .get(&fh.0)
            .cloned()
            .ok_or(libc::EBADF)?;
        let end = file.size.min(offset + buf.len() as u64);
        if offset >= end {
            return Ok(0);
        }
        let len = (end - offset) as usize;
        self.read_file(&file, offset, end, &mut buf[..len]).await?;
        Ok(len)
    }

    async fn release(
        &self,
        _req: Request,
        _inode: InodeId,
        fh: FileHandleId,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        _flock_release: bool,
    ) -> FsResult<()> {
        let Some(file) = self.open.lock().files.remove(&fh.0) else {
            return Ok(());
        };
        match &file.source {
            Source::Passthrough(id) => {
                if let Some(passthrough) = &self.passthrough {
                    passthrough.unregister(*id);
                }
            }
            // A stage nobody read: its node keeps it in memory until told.
            Source::Remote { key, staged, .. } => {
                if let Some(staged) = staged.lock().take() {
                    let (state, key) = (Arc::clone(&self.state), key.clone());
                    self.runtime.spawn(async move {
                        if let Err(e) = state.unstage(&key, staged).await {
                            tracing::debug!(node = key.node.as_str(), path = %key.path, "releasing a stage failed: {e:#}");
                        }
                    });
                }
            }
            Source::Local(_) => {}
        }
        Ok(())
    }

    async fn opendir(&self, _req: Request, inode: InodeId, _flags: u32) -> FsResult<ReplyOpen> {
        match self.entry(&self.path(inode)?)? {
            Entry::Dir => Ok(ReplyOpen {
                fh: FileHandleId(0),
                flags: 0,
                backing_id: 0,
            }),
            Entry::File(_) => Err(libc::ENOTDIR),
        }
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: InodeId,
        _fh: FileHandleId,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        let path = self.path(inode)?;
        let entries = self.state.index.read().list(&path).ok_or(ENOENT)?;
        let mut used = 0;
        let mut out = Vec::new();
        for (i, (name, kind)) in with_dots(entries).into_iter().enumerate() {
            let next = i as u64 + 1;
            if next <= offset {
                continue;
            }
            used += fuse_dirent_size(name.len());
            if used > size as usize {
                break;
            }
            out.push(DirectoryEntry {
                // The kernel looks every name up before it uses it, so this only has to be
                // a number other than zero; the same one for the same path.
                ino: pseudo_ino(&join(&path, &name)),
                offset: next,
                kind: file_type(kind),
                name: name.into_bytes(),
            });
        }
        Ok(out)
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: InodeId,
        _fh: FileHandleId,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        let path = self.path(inode)?;
        let entries = self.state.index.read().list(&path).ok_or(ENOENT)?;
        let mut used = 0;
        let mut out = Vec::new();
        for (i, (name, kind)) in with_dots(entries).into_iter().enumerate() {
            let next = i as u64 + 1;
            if next <= offset {
                continue;
            }
            used += fuse_direntplus_size(name.len());
            // Every entry handed back counts as a lookup, so only those that fit.
            if used > size as usize {
                break;
            }
            let child = match name.as_str() {
                "." => path.clone(),
                ".." => path
                    .rsplit_once('/')
                    .map_or_else(String::new, |(p, _)| p.to_owned()),
                _ => join(&path, &name),
            };
            let Some(entry) = self.state.index.read().lookup(&child) else {
                continue; // gone since the listing
            };
            let ino = if name == "." || name == ".." {
                self.inodes
                    .lock()
                    .by_path
                    .get(&child)
                    .copied()
                    .unwrap_or(FUSE_ROOT_ID)
            } else {
                self.inodes.lock().remember(&child)
            };
            out.push(DirectoryEntryPlus {
                ino,
                offset: next,
                kind: file_type(kind),
                name: name.into_bytes(),
                entry_ttl: TTL,
                attr: attr(ino, &entry),
                generation: 0,
            });
        }
        Ok(out)
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: InodeId,
        _fh: FileHandleId,
        _flags: u32,
    ) -> FsResult<()> {
        Ok(())
    }

    async fn statfs(&self, _req: Request, _inode: InodeId) -> FsResult<ReplyStatfs> {
        let files = self.state.index.read().file_count() as u64;
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files,
            ffree: 0,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }

    async fn access(&self, _req: Request, _inode: InodeId, _mask: u32) -> FsResult<()> {
        Ok(())
    }
}

fn with_dots(entries: Vec<(String, EntryKind)>) -> Vec<(String, EntryKind)> {
    [
        (".".to_owned(), EntryKind::Dir),
        ("..".to_owned(), EntryKind::Dir),
    ]
    .into_iter()
    .chain(entries)
    .collect()
}

fn pseudo_ino(path: &str) -> u64 {
    use std::hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher};
    BuildHasherDefault::<DefaultHasher>::default().hash_one(path) | 1
}

fn file_type(kind: EntryKind) -> FileType {
    match kind {
        EntryKind::Dir => FileType::Directory,
        EntryKind::File => FileType::RegularFile,
    }
}
