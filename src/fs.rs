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
//! A folder is read from one listing of it, taken when the reading begins: the kernel
//! reads a folder a page of entries a call, and each call returns the next entries of
//! that listing (OpenDirs), so reading a folder costs its size once.
//!
//! The kernel serves an inode either through rview-fs, keeping what it read, or passed
//! through, and fails an open of the one kind while one of the other is open (EIO). A
//! file of this node is passed through and one of another served, but a file two nodes
//! have can come to this node, or go from it, while it is open: an open of an inode is
//! of the kind of those still open (Kinds), served from this node's disk, or passed
//! through to the backing file of the others even once deleted here.
//!
//! Every open counts as a use of the file on the node whose export holds it (used.rs).
//!
//! FUSE requests run on fractal-fuse's threads. Anything that blocks or talks to velo
//! runs on the tokio runtime, and the request awaits it.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd as _, OwnedFd};
use std::os::unix::fs::FileExt as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fractal_fuse::abi::{
    FOPEN_KEEP_CACHE, FOPEN_PASSTHROUGH, FUSE_BATCH_FORGET, FUSE_FORGET, FUSE_ROOT_ID,
    fuse_batch_forget_in, fuse_dirent_size, fuse_direntplus_size, fuse_in_header,
};
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EIO, EISDIR, ENOENT, Errno, FileAttr, FileHandleId,
    FileType, Filesystem, FsResult, FuseNotifier, InodeId, ReplyAttr, ReplyEntry, ReplyInit,
    ReplyOpen, ReplyStatfs, Request, Timestamp,
};
use parking_lot::Mutex;
use prometheus::{IntGauge, Registry};
use tokio_util::sync::CancellationToken;

use bytes::Bytes;

use crate::cache::Key;
use crate::index::{Entry, EntryKind, FileView};
use crate::passthrough::Passthrough;
use crate::path::join;
use crate::remote::RemoteFile;
use crate::state::State;
use crate::used;

// Files never change once written, but they come and go.
const TTL: Duration = Duration::from_secs(1);
/// What a read from /dev/fuse is given room for at first. The kernel refuses a read with
/// less room than its largest write to the file system takes, 1 MiB and its headers as
/// fractal-fuse sets that, though a forget takes a page at the most.
const DEV_REQUEST_ROOM: usize = (1 << 20) + (1 << 16);
/// How often the kernel is told of the files and folders gone since.
const GONE_EVERY: Duration = Duration::from_millis(200);
/// The paths looked up in `Inodes` at a time, which every request takes for a moment.
const GONE_AT_A_TIME: usize = 64;
/// The reads the kernel has in flight on the mount at the most, unless set otherwise.
pub const READS_IN_FLIGHT: u16 = 16;

pub struct RviewFs {
    state: Arc<State>,
    runtime: tokio::runtime::Handle,
    passthrough: Option<Arc<Passthrough>>,
    reads_in_flight: u16,
    /// Whether the session has read the kernel's first request, INIT.
    initialized: Arc<AtomicBool>,
    // Taken by every request for a moment, on fractal-fuse's threads; never across an
    // await.
    inodes: Arc<Mutex<Inodes>>,
    open: Mutex<OpenFiles>,
    dirs: Mutex<OpenDirs>,
}

/// The path of every inode the kernel holds, with how many lookups it holds it by.
struct Inodes {
    by_ino: HashMap<u64, (String, u64)>,
    by_path: HashMap<String, u64>,
    next: u64,
    /// How many there are, as a metric.
    held: IntGauge,
}

impl Inodes {
    fn new() -> Self {
        let held = IntGauge::new(
            "rview_fs_inodes",
            "The inodes the kernel holds of the mount, each with its path here.",
        );
        Self {
            by_ino: HashMap::from([(FUSE_ROOT_ID, (String::new(), 1))]),
            by_path: HashMap::from([(String::new(), FUSE_ROOT_ID)]),
            next: FUSE_ROOT_ID + 1,
            held: held.expect("a valid metric"),
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
        self.held.inc();
        ino
    }

    /// The inode of `path`'s folder and `path`'s name in it, when the kernel holds an inode
    /// of `path`.
    fn entry_of(&self, path: &str) -> Option<(u64, String)> {
        self.by_path.get(path)?;
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        Some((*self.by_path.get(dir)?, name.to_owned()))
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
            self.held.dec();
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

/// A folder's entries, "." and ".." first, in the order they are read.
type Listing = Vec<(String, EntryKind)>;

/// The folders open, each with the listing its reading returns entries of.
///
/// The kernel reads a folder in calls of a page of entries, each from the offset the one
/// before reached. A listing taken at every call costs the folder's size a call, the
/// square of it a reading: seconds for tens of thousands of entries. So a reading takes
/// one listing, when it begins, and its calls read on in it. The folder may change
/// meanwhile, which that reading does not see; one begun again (rewinddir) does.
#[derive(Default)]
struct OpenDirs {
    listings: HashMap<u64, Option<Arc<Listing>>>,
    next: u64,
}

impl OpenDirs {
    /// A handle for a folder opened, which has no listing until it is read.
    fn open(&mut self) -> u64 {
        self.next += 1;
        self.listings.insert(self.next, None);
        self.next
    }

    /// The listing the reading of `fh` that goes on at `offset` began with. None when a
    /// reading begins (offset 0), which takes one, and for a handle with none to go on
    /// in: one read first from further in, or not open here.
    fn reading(&self, fh: u64, offset: u64) -> Option<Arc<Listing>> {
        if offset == 0 {
            return None;
        }
        self.listings.get(&fh)?.clone()
    }

    /// `listing` is what the reading of `fh` beginning now returns entries of. The
    /// listing of the reading before it, for the caller to drop.
    fn begin(&mut self, fh: u64, listing: &Arc<Listing>) -> Option<Arc<Listing>> {
        self.listings.get_mut(&fh)?.replace(listing.clone())
    }

    /// The listing `fh` was read from, for the caller to drop.
    fn close(&mut self, fh: u64) -> Option<Arc<Listing>> {
        self.listings.remove(&fh)?
    }
}

/// The entries of `listing` after its first `offset`, each with its own offset: the
/// number of entries up to and with it, which the kernel reads on from.
fn after(listing: &Listing, offset: u64) -> impl Iterator<Item = (u64, &(String, EntryKind))> {
    listing
        .iter()
        .enumerate()
        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        .map(|(i, entry)| (i as u64 + 1, entry))
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

/// The inodes a request read from /dev/fuse says to forget, each with the lookups it was
/// held by; none when it is another request (an interrupt of a call, which needs no
/// answer).
fn forgotten(request: &[u8]) -> Vec<(u64, u64)> {
    let word = |at: usize| {
        request
            .get(at..at + 8)
            .map(|b| u64::from_ne_bytes(b.try_into().unwrap()))
    };
    let header = size_of::<fuse_in_header>();
    let opcode = request
        .get(4..8)
        .map(|b| u32::from_ne_bytes(b.try_into().unwrap()));
    match opcode {
        // The inode is the header's, the lookups all there is after it.
        Some(FUSE_FORGET) => word(16).zip(word(header)).into_iter().collect(),
        Some(FUSE_BATCH_FORGET) => {
            let entries = header + size_of::<fuse_batch_forget_in>();
            let count = request
                .get(header..header + 4)
                .map(|b| u32::from_ne_bytes(b.try_into().unwrap()));
            (0..count.unwrap_or(0) as usize)
                .map_while(|i| word(entries + 16 * i).zip(word(entries + 16 * i + 8)))
                .collect()
        }
        _ => Vec::new(),
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
            reads_in_flight: READS_IN_FLIGHT,
            initialized: Arc::default(),
            inodes: Arc::new(Mutex::new(Inodes::new())),
            open: Mutex::default(),
            dirs: Mutex::default(),
        }
    }

    /// Have the kernel keep at most `most` reads in flight on the mount, for all who read
    /// through it together (FUSE's max_background). A read that meets a file of another
    /// node not fetched yet stays in flight until the file is there, so a few such files
    /// take the default up, and the readers of every other file wait behind them.
    #[must_use]
    pub fn with_reads_in_flight(mut self, most: u16) -> Self {
        self.reads_in_flight = most;
        self
    }

    /// Read what the kernel sends on /dev/fuse itself, `fuse`, and not through the ring,
    /// on a thread of its own until the mount is gone. As of Linux 6.14 it forgets inodes
    /// there, which nothing read: no inode was ever forgotten here, and the path of every
    /// file ever looked up stayed in memory.
    pub fn read_forgets(&self, fuse: Arc<OwnedFd>) -> io::Result<()> {
        let (inodes, initialized) = (Arc::clone(&self.inodes), Arc::clone(&self.initialized));
        let read = move || {
            // The first request on /dev/fuse, INIT, is the session's own to read. Not
            // learned by asking the mount: a call of our own that it has not answered
            // when rview-fs ends keeps its thread in the kernel, and the process never goes.
            while !initialized.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(100));
            }
            let mut request = vec![0u8; DEV_REQUEST_ROOM];
            loop {
                let (fd, buf) = (fuse.as_raw_fd(), request.as_mut_ptr().cast());
                // SAFETY: `buf` is `request`'s own memory, of the length given.
                let read = unsafe { libc::read(fd, buf, request.len()) };
                if read == 0 {
                    return; // closed: /dev/fuse never reads so, another file does
                }
                let Ok(len) = usize::try_from(read) else {
                    match io::Error::last_os_error() {
                        // A call given up before it was read, or a signal.
                        e if [libc::ENOENT, libc::EINTR, libc::EAGAIN]
                            .map(Some)
                            .contains(&e.raw_os_error()) =>
                        {
                            continue;
                        }
                        // Too little room for the kernel's largest write.
                        e if e.raw_os_error() == Some(libc::EINVAL)
                            && request.len() < 64 * DEV_REQUEST_ROOM =>
                        {
                            request = vec![0u8; 2 * request.len()];
                            continue;
                        }
                        // Unmounted.
                        e if e.raw_os_error() == Some(libc::ENODEV) => return,
                        e => {
                            tracing::error!(
                                "reading the kernel's forgets failed, so the path of every inode it forgets from now on stays in memory: {e}"
                            );
                            return;
                        }
                    }
                };
                let mut inodes = inodes.lock();
                for (ino, lookups) in forgotten(&request[..len]) {
                    inodes.forget(ino, lookups);
                }
            }
        };
        std::thread::Builder::new()
            .name("rview-forgets".into())
            .spawn(read)?;
        Ok(())
    }

    /// The count of the inodes the kernel holds, as a metric of `registry`.
    pub fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self.inodes.lock().held.clone()))
    }

    /// Tell the kernel, through `notifier`, of every file and folder gone from the index,
    /// until cancelled. The kernel keeps an inode for every name it remembers, and rview-fs
    /// that inode's path; a name nobody asks for again it remembers until it is short of
    /// memory, so both grew with every file ever opened. Told, it drops the name and what
    /// is below it, and forgets their inodes.
    pub fn forget_gone(
        &self,
        notifier: FuseNotifier,
        cancel: CancellationToken,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + 'static {
        let (state, inodes) = (Arc::clone(&self.state), Arc::clone(&self.inodes));
        async move {
            state.index.write().note_gone();
            let mut tick = tokio::time::interval(GONE_EVERY);
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    _ = tick.tick() => {}
                }
                let gone = state.index.write().take_gone();
                if gone.is_empty() {
                    continue;
                }
                let (inodes, notifier) = (Arc::clone(&inodes), notifier.clone());
                // The kernel takes the folder's lock for each: not on a thread of the runtime.
                tokio::task::spawn_blocking(move || {
                    for paths in gone.chunks(GONE_AT_A_TIME) {
                        let held: Vec<(u64, String)> = {
                            let inodes = inodes.lock();
                            paths.iter().filter_map(|p| inodes.entry_of(p)).collect()
                        };
                        for (folder, name) in held {
                            match notifier.inval_entry(folder, name.as_bytes()) {
                                Ok(()) => {}
                                // It dropped the name by itself since.
                                Err(e) if e.raw_os_error() == Some(libc::ENOENT) => {}
                                Err(e) => tracing::warn!(
                                    name,
                                    "telling the kernel of a file gone failed: {e}"
                                ),
                            }
                        }
                    }
                })
                .await?;
            }
        }
    }

    fn path(&self, ino: InodeId) -> FsResult<String> {
        self.inodes.lock().path(ino.0).ok_or(ENOENT)
    }

    fn entry(&self, path: &str) -> FsResult<Entry> {
        self.state.index.read().lookup(path).ok_or(ENOENT)
    }

    /// The listing a call reading the folder `path` through `fh` from `offset` returns
    /// entries of: the one its reading began with, or one taken now.
    fn listing(&self, path: &str, fh: FileHandleId, offset: u64) -> FsResult<Arc<Listing>> {
        if let Some(listing) = self.dirs.lock().reading(fh.0, offset) {
            return Ok(listing);
        }
        let entries = self.state.index.read().list(path).ok_or(ENOENT)?;
        let listing = Arc::new(with_dots(entries));
        // Freeing a big listing takes a while: not under the lock every folder read takes.
        let before = self.dirs.lock().begin(fh.0, &listing);
        drop(before);
        Ok(listing)
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
            if passthrough.mark_due(ino) {
                self.mark_local(file);
            }
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

    /// This node's file, or None when it is gone. Opened through the mount, it counts as
    /// used (used.rs).
    async fn open_local(&self, file: &FileView) -> FsResult<Option<File>> {
        let (export, path) = (self.state.export.clone(), file.path.clone());
        match self
            .runtime
            .spawn_blocking(move || -> io::Result<File> {
                let local = crate::path::open_file(&export, &path)?;
                if let Err(e) = used::mark(&local) {
                    used::cannot_mark(&e);
                }
                Ok(local)
            })
            .await
        {
            Ok(Ok(local)) => Ok(Some(local)),
            Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None), // deleted since
            Ok(Err(e)) => Err(io_errno(&e)),
            Err(_) => Err(EIO),
        }
    }

    /// Mark this node's file as used, for an open that opens nothing of the export. The
    /// open does not wait for it.
    fn mark_local(&self, file: &FileView) {
        let (export, path) = (self.state.export.clone(), file.path.clone());
        self.runtime.spawn_blocking(move || {
            if let Err(e) = used::mark_export_file(&export, &path) {
                used::cannot_mark(&e);
            }
        });
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
                // Its node is not asked, and is told of the open later (used.rs).
                self.state.used.note(&key);
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
    async fn init(&self, _req: Request) -> FsResult<ReplyInit> {
        self.initialized.store(true, Ordering::Release);
        Ok(ReplyInit {
            max_background: self.reads_in_flight,
            // From here on the kernel stops reading ahead of a reader: three quarters, as
            // its own defaults are to each other.
            congestion_threshold: (self.reads_in_flight / 4 * 3).max(1),
            ..ReplyInit::default()
        })
    }

    async fn lookup(&self, _req: Request, parent: InodeId, name: &OsStr) -> FsResult<ReplyEntry> {
        let _timed = self.state.ops.time("lookup");
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
        let _timed = self.state.ops.time("getattr");
        let entry = self.entry(&self.path(inode)?)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: attr(inode.0, &entry),
        })
    }

    async fn open(&self, _req: Request, inode: InodeId, flags: u32) -> FsResult<ReplyOpen> {
        let _timed = self.state.ops.time("open");
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
        let _timed = self.state.ops.time("read");
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
        let _timed = self.state.ops.time("opendir");
        match self.entry(&self.path(inode)?)? {
            Entry::Dir => Ok(ReplyOpen {
                fh: FileHandleId(self.dirs.lock().open()),
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
        fh: FileHandleId,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        let _timed = self.state.ops.time("readdir");
        let path = self.path(inode)?;
        let listing = self.listing(&path, fh, offset)?;
        let mut used = 0;
        Ok(after(&listing, offset)
            .take_while(|(_, (name, _))| {
                used += fuse_dirent_size(name.len());
                used <= size as usize
            })
            .map(|(next, (name, kind))| DirectoryEntry {
                // The kernel looks every name up before it uses it, so this only has to be
                // a number other than zero; the same one for the same path.
                ino: pseudo_ino(&join(&path, name)),
                offset: next,
                kind: file_type(*kind),
                name: name.clone().into_bytes(),
            })
            .collect())
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: InodeId,
        fh: FileHandleId,
        offset: u64,
        size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        let _timed = self.state.ops.time("readdirplus");
        let path = self.path(inode)?;
        let listing = self.listing(&path, fh, offset)?;
        let mut used = 0;
        let mut out = Vec::new();
        for (next, (name, _)) in after(&listing, offset) {
            let child = match name.as_str() {
                "." => path.clone(),
                ".." => path
                    .rsplit_once('/')
                    .map_or_else(String::new, |(p, _)| p.to_owned()),
                _ => join(&path, name),
            };
            // Gone since the listing: left out, and the call goes on to the entries after
            // it, since one that returns none ends the reading.
            let Some(entry) = self.state.index.read().lookup(&child) else {
                continue;
            };
            used += fuse_direntplus_size(name.len());
            // Every entry handed back counts as a lookup, so only those that fit.
            if used > size as usize {
                break;
            }
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
                kind: file_type(kind_of(&entry)),
                name: name.clone().into_bytes(),
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
        fh: FileHandleId,
        _flags: u32,
    ) -> FsResult<()> {
        let listing = self.dirs.lock().close(fh.0);
        drop(listing); // not under the lock, as in `listing`
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

fn kind_of(entry: &Entry) -> EntryKind {
    match entry {
        Entry::Dir => EntryKind::Dir,
        Entry::File(_) => EntryKind::File,
    }
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
