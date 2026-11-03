//! Moving files between the machines with velo.
//!
//! Each node serves its own export to the others: a listing of its files; a file held
//! open for one of them from its open until its close (held.rs); and such a file's bytes,
//! staged when it reads them. Staged in RDMA-registered memory (with the `ucx` feature),
//! the reader's NIC reads them straight into the reader's host memory; otherwise they are
//! pulled in chunks over TCP. The bytes staged at once are bounded; a stage is gone once
//! its reader took it, and dropped after a while if it never does.

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

use crate::content;
use crate::held::Held;
use crate::path::RelPath;

pub const LIST: &str = "rview_list";
pub const OPEN: &str = "rview_open";
pub const FETCH: &str = "rview_fetch";
pub const CLOSE: &str = "rview_close";
pub const RENEW: &str = "rview_renew";
/// How long a call waits for its answer: less than held::GRACE.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(60);
// A stage nobody took by then is dropped; no transfer takes anywhere near as long.
const STAGED_FOR: Duration = Duration::from_secs(600);
const REAP_EVERY: Duration = Duration::from_secs(10);

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
struct OpenRequest {
    client: InstanceId,
    path: String,
}

/// A file a node holds open for another, and the handle it does by.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Opened {
    pub handle: u64,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct HandleRequest {
    client: InstanceId,
    handle: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RenewRequest {
    client: InstanceId,
    open: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Staged {
    handle: DataHandle,
    size: u64,
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

/// The file of the export at `path`, open, and its size; None when it is not there.
/// Opening it for another node counts as using it: its time is renewed, which a cleanup
/// that deletes the least recently used files can go by. Blocks.
pub fn open_export_file(path: &Path) -> io::Result<Option<(File, u64)>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Err(io::Error::other(format!(
            "{} is not a file",
            path.display()
        )));
    }
    if let Err(e) = file.set_modified(SystemTime::now()) {
        tracing::debug!(?path, %e, "could not mark a file another node opened as used");
    }
    Ok(Some((file, meta.len())))
}

/// The bytes of a held file, mapped. Blocks.
fn map_held(file: &File) -> io::Result<Bytes> {
    if file.metadata()?.len() == 0 {
        return Ok(Bytes::new());
    }
    // SAFETY: files are written under a hidden name and moved into place whole, and never
    // written again (README.md), so the mapping never changes under us. It keeps the file
    // after the file is closed and deleted, until the mapping is dropped.
    let mapped = unsafe { memmap2::Mmap::map(file)? };
    Ok(Bytes::from_owner(mapped))
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

/// Serve this node's export at `root` to the other nodes: its handlers, registered when
/// this returns. With `pinned`, files are staged in RDMA-registered memory.
pub fn serve(
    velo: &Velo,
    root: PathBuf,
    pinned: bool,
    stages: Arc<Stages>,
    held: Arc<Held<InstanceId>>,
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
    let opened_by = Arc::clone(&held);
    velo.register_handler(
        Handler::typed_unary_async(OPEN, move |ctx: TypedContext<OpenRequest>| {
            let (root, held) = (root.clone(), Arc::clone(&opened_by));
            async move {
                let path = RelPath::parse(&ctx.input.path)
                    .ok_or_else(|| anyhow!("not a path in the export: {:?}", ctx.input.path))?;
                let file = root.join(path.as_str());
                let Some((file, size)) =
                    tokio::task::spawn_blocking(move || open_export_file(&file)).await??
                else {
                    return Ok(None);
                };
                match held.open(ctx.input.client, file, Instant::now()) {
                    Ok(handle) => Ok(Some(Opened { handle, size })),
                    Err(_refused) => Err(anyhow!(
                        "{} files are held open for other nodes already, the most this node holds",
                        held.count()
                    )),
                }
            }
        })
        .build(),
    )?;
    let (weak, fetched_from, staged_by): (WeakVelo, _, _) =
        (velo.downgrade(), Arc::clone(&held), Arc::clone(&stages));
    velo.register_handler(
        Handler::typed_unary_async(FETCH, move |ctx: TypedContext<HandleRequest>| {
            let (weak, held, stages) = (
                weak.clone(),
                Arc::clone(&fetched_from),
                Arc::clone(&staged_by),
            );
            async move {
                let HandleRequest { client, handle } = ctx.input;
                let file = held.get(client, handle, Instant::now()).ok_or_else(|| {
                    anyhow!(
                        "no file {handle} is open for that node: closed, or this node restarted"
                    )
                })?;
                let bytes = tokio::task::spawn_blocking(move || map_held(&file)).await??;
                let velo = weak.upgrade().ok_or_else(|| anyhow!("shutting down"))?;
                let size = bytes.len() as u64;
                let store = velo.rendezvous_manager().data_store();
                anyhow::ensure!(
                    stages.reserve(size, store),
                    "no room to stage {size} bytes: {} staged",
                    stages.bytes()
                );
                let handle = if pinned {
                    velo.register_data_pinned(&bytes).await
                } else {
                    velo.register_data(bytes)
                };
                stages.add(handle, size);
                Ok(Staged { handle, size })
            }
        })
        .build(),
    )?;
    let closed_by = Arc::clone(&held);
    velo.register_handler(
        Handler::typed_unary_async(CLOSE, move |ctx: TypedContext<HandleRequest>| {
            let held = Arc::clone(&closed_by);
            async move {
                held.close(ctx.input.client, ctx.input.handle);
                Ok(())
            }
        })
        .build(),
    )?;
    let renewed_by = Arc::clone(&held);
    velo.register_handler(
        Handler::typed_unary_async(RENEW, move |ctx: TypedContext<RenewRequest>| {
            let held = Arc::clone(&renewed_by);
            async move {
                let RenewRequest { client, open } = ctx.input;
                let closed = held.renew(client, &open, Instant::now());
                if closed > 0 {
                    tracing::warn!(
                        ?client,
                        closed,
                        "closed files a node no longer has open: their closes never arrived"
                    );
                }
                Ok(())
            }
        })
        .build(),
    )?;
    Ok(())
}

/// Drop the stages nobody took in time, and close the files of the nodes gone silent,
/// until cancelled.
pub async fn reap(
    velo: Velo,
    stages: Arc<Stages>,
    held: Arc<Held<InstanceId>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(REAP_EVERY);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        stages.reap(velo.rendezvous_manager().data_store());
        let closed = held.expire(Instant::now());
        if closed > 0 {
            tracing::warn!(closed, "closed the files of nodes gone silent");
        }
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

/// Have the node open a file of its export, and hold it open for us until we close it.
/// None when it does not have it (anymore).
pub async fn open(
    velo: &Velo,
    instance: InstanceId,
    path: &RelPath,
) -> anyhow::Result<Option<Opened>> {
    velo.typed_unary::<Option<Opened>>(OPEN)?
        .payload(&OpenRequest {
            client: velo.instance_id(),
            path: path.as_str().to_owned(),
        })?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .with_context(|| format!("opening {path}"))
}

/// The bytes of a file the node holds open for us as `handle`.
pub async fn fetch(velo: &Velo, instance: InstanceId, handle: u64) -> anyhow::Result<Bytes> {
    let staged = velo
        .typed_unary::<Staged>(FETCH)?
        .payload(&HandleRequest {
            client: velo.instance_id(),
            handle,
        })?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .context("staging an open file")?;
    let mut map = content::buffer(staged.size).context("making room for a file")?;
    let mut dest: &mut [u8] = map.as_deref_mut().unwrap_or_default();
    // A node that hangs rather than goes would hold the read, and the page fault behind
    // it, forever.
    let lease = tokio::time::timeout(CALL_TIMEOUT, velo.get_into(staged.handle, &mut dest))
        .await
        .with_context(|| format!("no reply in {CALL_TIMEOUT:?} reading a staged file"))?
        .context("reading a staged file")?;
    velo.release(staged.handle, lease).await?;
    Ok(content::freeze(map)?)
}

/// Tell the node we closed the file it holds open for us as `handle`.
pub async fn close(velo: &Velo, instance: InstanceId, handle: u64) -> anyhow::Result<()> {
    velo.typed_unary::<()>(CLOSE)?
        .payload(&HandleRequest {
            client: velo.instance_id(),
            handle,
        })?
        .instance(instance)
        .timeout(CALL_TIMEOUT)
        .send()
        .await
        .context("closing a file another node holds")
}

/// Tell the node which of the files it holds open for us we still have open: all.
pub async fn renew(velo: &Velo, instance: InstanceId, open: Vec<u64>) -> anyhow::Result<()> {
    velo.typed_unary::<()>(RENEW)?
        .payload(&RenewRequest {
            client: velo.instance_id(),
            open,
        })?
        .instance(instance)
        // A list late by this much is overtaken by the next.
        .timeout(crate::held::RENEW_EVERY)
        .send()
        .await
        .context("listing the files we have open")
}

#[cfg(test)]
#[path = "transfer_test.rs"]
mod tests;
