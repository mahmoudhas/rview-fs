//! The union as a read-only FUSE file system: the folders and files of every node's
//! export (index.rs).
//!
//! A file of this node is read from the export: by the kernel itself with passthrough
//! (Linux 6.9), every open of a file sharing one backing file, and otherwise by rview-fs.
//!
//! A file of another node is opened there too, and held open until it is closed here
//! (held.rs, remote.rs): that node may delete it meanwhile and it is still read whole.
//! Nothing of it moves until it is read: the first read fetches all of it into memory,
//! a mapped file's first fault included, and rview-fs serves its reads from there. A file
//! opened and never read is never fetched. An open file keeps its bytes until it is
//! closed, which a mapping of it is only once unmapped: the kernel may drop the pages it
//! read and fault them in again, and that must not need the other node still up. A file
//! read before comes from the cache, without asking its node.
//!
//! The kernel serves an inode either through rview-fs, keeping what it read, or passed
//! through, and fails an open of the one kind while one of the other is open (EIO). A
//! file of this node is passed through and one of another served, but a file two nodes
//! have can come to this node, or go from it, while it is open: an open of an inode is
//! of the kind of those still open (Kinds), served from this node's disk, or passed
//! through to the backing file of the others even once deleted here.
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

use bytes::Bytes;

use crate::cache::Key;
use crate::index::{Entry, EntryKind, FileView};
use crate::passthrough::Passthrough;
use crate::path::join;
use crate::remote::RemoteFile;
use crate::state::State;

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
    /// A file of this node the kernel reads itself, from the backing file of inode `ino`
    /// registered under `id`.
    Passthrough { ino: u64, id: i32 },
    /// A file of this node.
    Local(Arc<File>),
    /// A file of another node, read before.
    Fetched(Bytes),
    /// A file of another node, which holds it open for us; fetched by the first read.
    Remote {
        file: RemoteFile,
        bytes: Mutex<Option<Bytes>>,
    },
}

struct OpenFile {
    ino: u64,
    size: u64,
    source: Source,
}

#[derive(Default)]
struct OpenFiles {
    files: HashMap<u64, Arc<OpenFile>>,
    next: u64,
    kinds: Kinds,
}

/// How the kernel reads an inode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    PassedThrough,
    Served,
}

/// The kind of every inode open, and how many opens of it are; counted from the start
/// of an open, so that two at once agree.
#[derive(Default)]
struct Kinds(HashMap<u64, (Kind, usize)>);

impl Kinds {
    /// One more open of `ino`: of kind `want`, unless opens of it are, whose kind it
    /// takes. The kind it is of.
    fn begin(&mut self, ino: u64, want: Kind) -> Kind {
        let (kind, opens) = self.0.entry(ino).or_insert((want, 0));
        *opens += 1;
        *kind
    }

    /// An open of `ino` that `begin` counted is closed, or failed.
    fn end(&mut self, ino: u64) {
        let Some((_, opens)) = self.0.get_mut(&ino) else {
            return;
        };
        *opens -= 1;
        if *opens == 0 {
            self.0.remove(&ino);
        }
    }

    /// Serve the open of `ino` begun, when it is the only one; whether it is.
    fn serve_alone(&mut self, ino: u64) -> bool {
        match self.0.get_mut(&ino) {
            Some((kind, 1)) => {
                *kind = Kind::Served;
                true
            }
            _ => false,
        }
    }
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

    /// Where the bytes of inode `ino`, `file`, come from: this node's disk when it has
    /// the file, else another node, which holds it open for us. The open is counted in
    /// `kinds` until it is released, or here when it fails.
    async fn open_file(&self, ino: u64, file: FileView) -> FsResult<OpenFile> {
        let ours = file.nodes.contains(&self.state.me);
        let want = match &self.passthrough {
            Some(passthrough) if ours && passthrough.is_on() => Kind::PassedThrough,
            _ => Kind::Served,
        };
        let kind = self.open.lock().kinds.begin(ino, want);
        let opened = match kind {
            Kind::PassedThrough => self.open_passed_through(ino, &file).await,
            Kind::Served => self.open_served(ino, &file, ours).await,
        };
        if opened.is_err() {
            self.open.lock().kinds.end(ino);
        }
        opened
    }

    /// An open passed through: to the backing file of the inode's other opens, or to this
    /// node's file. Served instead when neither can be and no other open is, or failed.
    async fn open_passed_through(&self, ino: u64, file: &FileView) -> FsResult<OpenFile> {
        let size = file.size;
        let Some(passthrough) = &self.passthrough else {
            return Err(EIO);
        };
        if let Some(id) = passthrough.reuse(ino) {
            let source = Source::Passthrough { ino, id };
            return Ok(OpenFile { ino, size, source });
        }
        let local = self.open_local(file).await?;
        if let Some(local) = &local
            && let Some(id) = passthrough.open(ino, local)
        {
            let source = Source::Passthrough { ino, id };
            return Ok(OpenFile { ino, size, source });
        }
        if !self.open.lock().kinds.serve_alone(ino) {
            // Opens passed through that have not registered their backing file yet, or
            // whose file went from here before they could.
            return Err(EIO);
        }
        match local {
            Some(local) => Ok(OpenFile {
                ino,
                size,
                source: Source::Local(Arc::new(local)),
            }),
            None => self.open_served(ino, file, false).await,
        }
    }

    /// This node's file, or None when it is gone.
    async fn open_local(&self, file: &FileView) -> FsResult<Option<File>> {
        let path = self.state.export.join(file.path.as_str());
        match self.runtime.spawn_blocking(move || File::open(path)).await {
            Ok(Ok(local)) => Ok(Some(local)),
            Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None), // deleted since
            Ok(Err(e)) => Err(io_errno(&e)),
            Err(_) => Err(EIO),
        }
    }

    /// An open rview-fs serves: from this node's disk when `ours`, else another node's.
    async fn open_served(&self, ino: u64, file: &FileView, ours: bool) -> FsResult<OpenFile> {
        let size = file.size;
        if ours && let Some(local) = self.open_local(file).await? {
            let source = Source::Local(Arc::new(local));
            return Ok(OpenFile { ino, size, source });
        }
        let mut failed = false;
        for node in file.nodes.iter().filter(|n| **n != self.state.me) {
            let key = Key {
                node: node.clone(),
                path: file.path.clone(),
            };
            if let Some(bytes) = self.state.cache.get(&key) {
                let size = bytes.len() as u64;
                return Ok(OpenFile {
                    ino,
                    size,
                    source: Source::Fetched(bytes),
                });
            }
            let (state, runtime) = (Arc::clone(&self.state), self.runtime.clone());
            match self
                .runtime
                .spawn(async move { RemoteFile::open(state, key, runtime).await })
                .await
            {
                Ok(Ok(Some(remote))) => {
                    let size = remote.remote.size;
                    let source = Source::Remote {
                        file: remote,
                        bytes: Mutex::new(None),
                    };
                    return Ok(OpenFile { ino, size, source });
                }
                Ok(Ok(None)) => continue, // deleted there since
                Ok(Err(e)) => {
                    tracing::warn!(node = node.as_str(), path = %file.path, "opening failed: {e:#}");
                    failed = true;
                }
                Err(_) => return Err(EIO),
            }
        }
        // Gone from every node that had it, unless one could not be asked.
        Err(if failed { EIO } else { ENOENT })
    }

    async fn read_file(&self, file: &OpenFile, from: u64, to: u64, dst: &mut [u8]) -> FsResult<()> {
        match &file.source {
            // The kernel reads a file passed through itself, and never asks.
            Source::Passthrough { .. } => return Err(EIO),
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
            Source::Fetched(bytes) => {
                let src = bytes.get(from as usize..to as usize).ok_or(EIO)?;
                dst.copy_from_slice(src);
            }
            Source::Remote { file, bytes } => {
                let held = bytes.lock().clone();
                let data = match held {
                    Some(held) => held,
                    None => {
                        let (state, remote) = (Arc::clone(&self.state), file.remote.clone());
                        let fetched = self
                            .runtime
                            .spawn(async move { state.remote_content(&remote).await })
                            .await
                            .map_err(|_| EIO)?
                            .map_err(|e| {
                                tracing::error!("reading a file of another node failed: {e:#}");
                                EIO
                            })?;
                        *bytes.lock() = Some(fetched.clone());
                        fetched
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
        let file = Arc::new(self.open_file(inode.0, file).await?);
        let (flags, backing_id) = match &file.source {
            Source::Passthrough { id, .. } => (FOPEN_PASSTHROUGH, *id),
            // A file never changes once written, so the kernel may keep what it read.
            Source::Local(_) | Source::Fetched(_) | Source::Remote { .. } => (FOPEN_KEEP_CACHE, 0),
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
        let Some(file) = ({
            let mut open = self.open.lock();
            let file = open.files.remove(&fh.0);
            if let Some(file) = &file {
                open.kinds.end(file.ino);
            }
            file
        }) else {
            return Ok(());
        };
        if let (Source::Passthrough { ino, .. }, Some(passthrough)) =
            (&file.source, &self.passthrough)
        {
            passthrough.release(*ino);
        }
        // A file of another node is closed there once the last read of it is done, when
        // the last reference goes (remote.rs).
        drop(file);
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

#[cfg(test)]
#[path = "fs_test.rs"]
mod tests;
