//! Moving files between the machines with velo.
//!
//! Each node serves its own export: a listing of its files, and a file staged for one
//! reader. Staged in RDMA-registered memory (with the `ucx` feature and `--rdma`),
//! the reader's NIC reads it straight into the reader's host memory; otherwise it is
//! pulled in chunks over TCP. The bytes staged at once are bounded; a stage is gone once
//! its reader took it, released when the reader closes the file without reading, and
//! dropped after a while if neither happens.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use velo::{DataHandle, Handler, InstanceId, TypedContext, Velo, WeakVelo};

use crate::content::{self, Content};
use crate::path::RelPath;

pub const LIST: &str = "rview_list";
pub const STAGE: &str = "rview_stage";
pub const UNSTAGE: &str = "rview_unstage";
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
// A stage nobody took by then is dropped; no transfer takes anywhere near as long.
const STAGED_FOR: Duration = Duration::from_secs(600);
const REAP_EVERY: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize)]
pub struct Listing {
    pub files: Vec<Listed>,
    /// When the walk started, in seconds since the epoch.
    pub listed_at: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Listed {
    pub path: String,
    pub size: u64,
    pub mtime: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {}

#[derive(Debug, Serialize, Deserialize)]
struct StageRequest {
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UnstageRequest {
    handle: DataHandle,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Staged {
    pub handle: DataHandle,
    pub size: u64,
}

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Every visible file below `root`. Blocks.
pub fn scan(root: &Path) -> io::Result<Listing> {
    let listed_at = now();
    let mut files = Vec::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(dir) = stack.pop() {
        let entries = match std::fs::read_dir(root.join(&dir)) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue, // removed meanwhile
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.starts_with('.') {
                continue; // being written, so not shared
            }
            let rel = dir.join(name);
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(rel);
            } else if meta.is_file() {
                let mtime = meta
                    .modified()?
                    .duration_since(UNIX_EPOCH)
                    .map_or(0.0, |d| d.as_secs_f64());
                files.push(Listed {
                    path: rel.to_string_lossy().into_owned(),
                    size: meta.len(),
                    mtime,
                });
            }
        }
    }
    Ok(Listing { files, listed_at })
}

/// The bytes of a file, mapped, or None when it is not there. Reading it counts as using
/// it: its time is renewed, which a cleanup that deletes the least recently used files
/// can go by. Blocks.
fn map_file(path: &Path) -> io::Result<Option<Bytes>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if let Err(e) = file.set_modified(SystemTime::now()) {
        tracing::debug!(?path, %e, "could not mark a staged file as used");
    }
    if file.metadata()?.len() == 0 {
        return Ok(Some(Bytes::new()));
    }
    // SAFETY: files are written under a hidden name and moved into place whole, and never
    // written again (README.md), so the mapping never changes under us. A file deleted
    // meanwhile stays mapped until the mapping is dropped.
    let mapped = unsafe { memmap2::Mmap::map(&file)? };
    Ok(Some(Bytes::from_owner(mapped)))
}

/// What this node staged for other nodes: within `budget` bytes, oldest first.
pub struct Stages {
    budget: u64,
    staged: Mutex<StageList>,
}

#[derive(Default)]
struct StageList {
    stages: VecDeque<(Instant, DataHandle, u64)>,
    /// Bytes of the stages, and of those being made.
    bytes: u64,
}

impl Stages {
    pub fn new(budget: u64) -> Self {
        Self {
            budget,
            staged: Mutex::default(),
        }
    }

    /// Room for `size` more bytes, or false when the budget has none. A file bigger than
    /// the whole budget is staged alone.
    fn reserve(&self, size: u64, store: &velo::rendezvous::store::DataStore) -> bool {
        let mut s = self.staged.lock();
        s.forget_gone(store);
        if s.bytes + size > self.budget && s.bytes > 0 {
            return false;
        }
        s.bytes += size;
        true
    }

    fn add(&self, handle: DataHandle, size: u64) {
        self.staged
            .lock()
            .stages
            .push_back((Instant::now(), handle, size));
    }

    /// Release a stage nobody will take.
    fn release(&self, handle: DataHandle, store: &velo::rendezvous::store::DataStore) {
        let mut s = self.staged.lock();
        if let Some(i) = s.stages.iter().position(|(_, h, _)| *h == handle) {
            let (_, _, size) = s.stages.remove(i).expect("the position is in range");
            s.bytes -= size;
            store.remove(handle.unpack().1);
        }
    }

    /// Drop the stages not taken in time, and count those taken.
    fn reap(&self, store: &velo::rendezvous::store::DataStore) {
        let mut s = self.staged.lock();
        while s
            .stages
            .front()
            .is_some_and(|(at, _, _)| at.elapsed() >= STAGED_FOR)
        {
            let (_, handle, size) = s.stages.pop_front().expect("front is some");
            s.bytes -= size;
            store.remove(handle.unpack().1);
        }
        s.forget_gone(store);
    }

    pub fn bytes(&self) -> u64 {
        self.staged.lock().bytes
    }
}

impl StageList {
    /// Stop counting the stages whose reader took them and let go.
    fn forget_gone(&mut self, store: &velo::rendezvous::store::DataStore) {
        let mut gone = 0;
        self.stages.retain(|(_, handle, size)| {
            let live = store.get_total_len(handle.unpack().1).is_some();
            if !live {
                gone += size;
            }
            live
        });
        self.bytes -= gone;
    }
}

/// Serve this node's export at `root` to the other nodes until cancelled. With
/// `pinned`, files are staged in RDMA-registered memory.
pub async fn serve(
    velo: Velo,
    root: PathBuf,
    pinned: bool,
    stages: Arc<Stages>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let list_root = root.clone();
    velo.register_handler(
        Handler::typed_unary_async(LIST, move |_ctx: TypedContext<ListRequest>| {
            let root = list_root.clone();
            async move {
                let listing = tokio::task::spawn_blocking(move || scan(&root)).await??;
                Ok(listing)
            }
        })
        .build(),
    )?;
    let weak: WeakVelo = velo.downgrade();
    let staged_by = Arc::clone(&stages);
    velo.register_handler(
        Handler::typed_unary_async(STAGE, move |ctx: TypedContext<StageRequest>| {
            let (weak, root, stages) = (weak.clone(), root.clone(), Arc::clone(&staged_by));
            async move {
                let path = RelPath::parse(&ctx.input.path)
                    .ok_or_else(|| anyhow!("not a path in the export: {:?}", ctx.input.path))?;
                let file = root.join(path.as_str());
                let Some(bytes) = tokio::task::spawn_blocking(move || map_file(&file)).await??
                else {
                    return Ok(None);
                };
                let velo = weak.upgrade().ok_or_else(|| anyhow!("shutting down"))?;
                let size = bytes.len() as u64;
                let store = velo.rendezvous_manager().data_store();
                anyhow::ensure!(
                    stages.reserve(size, store),
                    "no room to stage {path} ({size} bytes): {} staged",
                    stages.bytes()
                );
                let handle = if pinned {
                    velo.register_data_pinned(&bytes).await
                } else {
                    velo.register_data(bytes)
                };
                stages.add(handle, size);
                Ok(Some(Staged { handle, size }))
            }
        })
        .build(),
    )?;
    let (weak, released_by) = (velo.downgrade(), Arc::clone(&stages));
    velo.register_handler(
        Handler::typed_unary_async(UNSTAGE, move |ctx: TypedContext<UnstageRequest>| {
            let (weak, stages) = (weak.clone(), Arc::clone(&released_by));
            async move {
                let velo = weak.upgrade().ok_or_else(|| anyhow!("shutting down"))?;
                stages.release(ctx.input.handle, velo.rendezvous_manager().data_store());
                Ok(())
            }
        })
        .build(),
    )?;
    let mut tick = tokio::time::interval(REAP_EVERY);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        stages.reap(velo.rendezvous_manager().data_store());
    }
}

pub async fn list(velo: &Velo, instance: InstanceId) -> anyhow::Result<Listing> {
    velo.typed_unary::<Listing>(LIST)?
        .payload(&ListRequest {})?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .context("listing a peer's files")
}

/// Ask the node to stage a file for us. None when it does not have it (anymore).
pub async fn stage(
    velo: &Velo,
    instance: InstanceId,
    path: &RelPath,
) -> anyhow::Result<Option<Staged>> {
    velo.typed_unary::<Option<Staged>>(STAGE)?
        .payload(&StageRequest {
            path: path.as_str().to_owned(),
        })?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("staging {path}"))
}

/// Tell the node we will not read a file it staged for us.
pub async fn unstage(velo: &Velo, instance: InstanceId, staged: Staged) -> anyhow::Result<()> {
    velo.typed_unary::<()>(UNSTAGE)?
        .payload(&UnstageRequest {
            handle: staged.handle,
        })?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .context("releasing a stage")
}

/// Read a staged file into a new memfd named `name`.
pub async fn fetch(velo: &Velo, staged: Staged, name: &str) -> anyhow::Result<Content> {
    let (file, mut map) = content::memfd(name, staged.size).context("making a memfd")?;
    let mut dest: &mut [u8] = map.as_deref_mut().unwrap_or_default();
    // A node that hangs rather than goes would hold the open or the read, and the page
    // fault behind it, forever.
    let lease = tokio::time::timeout(CALL_TIMEOUT, velo.get_into(staged.handle, &mut dest))
        .await
        .with_context(|| format!("no reply in {CALL_TIMEOUT:?} reading a staged file"))?
        .context("reading a staged file")?;
    velo.release(staged.handle, lease).await?;
    Ok(Content::new(file, map)?)
}

#[cfg(test)]
#[path = "transfer_test.rs"]
mod tests;
