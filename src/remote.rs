//! The files of other nodes this node has open. The node a file is on holds it open from
//! the open until this node closes it (held.rs), and this node lists the ones it has open
//! to it every RENEW_EVERY, so that one it no longer has open is closed there even when
//! its close never arrived.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use parking_lot::Mutex;
use prometheus::{IntCounterVec, Opts, Registry};
use tokio_util::sync::CancellationToken;
use velo::InstanceId;

use crate::cache::Key;
use crate::held::RENEW_EVERY;
use crate::index::NodeId;
use crate::state::State;
use crate::transfer::{self, Opened};

/// The handles this node has open, by the node's rview-fs that holds them.
#[derive(Default)]
pub struct RemoteFiles {
    // Taken for a moment by every open and close, and every RENEW_EVERY.
    open: Mutex<HashMap<InstanceId, BTreeSet<u64>>>,
}

impl RemoteFiles {
    fn add(&self, instance: InstanceId, handle: u64) {
        self.open.lock().entry(instance).or_default().insert(handle);
    }

    fn remove(&self, instance: InstanceId, handle: u64) {
        let mut open = self.open.lock();
        if let Some(handles) = open.get_mut(&instance) {
            handles.remove(&handle);
            if handles.is_empty() {
                open.remove(&instance);
            }
        }
    }

    /// The handles open on each node that holds some.
    pub fn lists(&self) -> Vec<(InstanceId, Vec<u64>)> {
        self.open
            .lock()
            .iter()
            .map(|(instance, handles)| (*instance, handles.iter().copied().collect()))
            .collect()
    }
}

/// How long a node's files are read over TCP after reading one over RDMA stalled, before
/// a read tries RDMA again.
pub const TCP_AFTER_STALL: Duration = Duration::from_secs(300);

/// How a file of another node is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Via {
    Rdma,
    /// RDMA, once the node's files were read over TCP for TCP_AFTER_STALL: whether it
    /// works again.
    RdmaAgain,
    Tcp,
}

/// The nodes whose files are read over TCP, because reading one over RDMA stalled: RDMA
/// from a node has stalled, with no error from UCX, until rview-fs restarted, while TCP
/// went on working.
#[derive(Default)]
pub struct Stalls {
    /// When reading a file of each, up as the instance, last stalled or tried RDMA again.
    since: Mutex<HashMap<NodeId, (InstanceId, Instant)>>,
    counted: StallCount,
}

/// The reads of each node's files over RDMA that stalled.
struct StallCount(IntCounterVec);

impl Default for StallCount {
    fn default() -> Self {
        let opts = Opts::new(
            "rview_fs_rdma_stalls_total",
            "Reads of a machine's files over RDMA that stalled, by machine.",
        );
        Self(IntCounterVec::new(opts, &["node"]).expect("a valid metric"))
    }
}

impl Stalls {
    /// How to read a file of `node`, up as `instance`.
    pub fn via(&self, node: &NodeId, instance: InstanceId) -> Via {
        let mut since = self.since.lock();
        let Some((stalled, at)) = since.get_mut(node) else {
            return Via::Rdma;
        };
        if *stalled != instance {
            since.remove(node); // it restarted, and its UCX with it
            return Via::Rdma;
        }
        if at.elapsed() < TCP_AFTER_STALL {
            return Via::Tcp;
        }
        // This read tries RDMA; the others go on over TCP until it worked.
        *at = Instant::now();
        Via::RdmaAgain
    }

    /// Reading a file of `node`, up as `instance`, over RDMA stalled: whether that is
    /// news, which it is not when its files were read over TCP already.
    pub fn stalled(&self, node: &NodeId, instance: InstanceId) -> bool {
        self.counted.0.with_label_values(&[node.as_str()]).inc();
        let was = self
            .since
            .lock()
            .insert(node.clone(), (instance, Instant::now()));
        was.is_none_or(|(stalled, _)| stalled != instance)
    }

    /// Reading a file of `node` over RDMA worked again.
    pub fn worked(&self, node: &NodeId) {
        self.since.lock().remove(node);
    }

    pub fn register(&self, registry: &Registry) -> prometheus::Result<()> {
        registry.register(Box::new(self.counted.0.clone()))
    }

    /// The reads of `node`'s files over RDMA that stalled.
    pub fn count(&self, node: &NodeId) -> u64 {
        self.counted.0.with_label_values(&[node.as_str()]).get()
    }
}

/// A file of another node, and the handle it holds it open for us by.
#[derive(Clone, Debug)]
pub struct Remote {
    pub key: Key,
    pub instance: InstanceId,
    pub handle: u64,
    pub size: u64,
}

/// A file another node holds open for this one, until this is dropped.
pub struct RemoteFile {
    pub remote: Remote,
    state: Arc<State>,
    runtime: tokio::runtime::Handle,
}

impl RemoteFile {
    /// Have the node `key` names open its file for us. None when it does not have it
    /// (anymore).
    pub async fn open(
        state: Arc<State>,
        key: Key,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Option<Self>> {
        let instance = state
            .nodes
            .read()
            .instance(&key.node)
            .ok_or_else(|| anyhow!("node {} is not up", key.node.as_str()))?;
        let Some(Opened { handle, size }) =
            transfer::open(&state.velo, instance, &key.path).await?
        else {
            return Ok(None);
        };
        state.remote.add(instance, handle);
        Ok(Some(Self {
            remote: Remote {
                key,
                instance,
                handle,
                size,
            },
            state,
            runtime,
        }))
    }
}

impl Drop for RemoteFile {
    fn drop(&mut self) {
        let (instance, handle) = (self.remote.instance, self.remote.handle);
        self.state.remote.remove(instance, handle);
        let state = Arc::clone(&self.state);
        // A close that never arrives is caught by the next list (held.rs).
        self.runtime.spawn(async move {
            if let Err(e) = transfer::close(&state.velo, instance, handle).await {
                tracing::debug!(?instance, "closing a file another node holds failed: {e:#}");
            }
        });
    }
}

/// List the files this node has open to each node that holds some, every RENEW_EVERY,
/// until cancelled.
pub async fn renew(state: Arc<State>, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(RENEW_EVERY);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        let mut sent = tokio::task::JoinSet::new();
        for (instance, open) in state.remote.lists() {
            let velo = state.velo.clone();
            sent.spawn(async move { (instance, transfer::renew(&velo, instance, open).await) });
        }
        while let Some(done) = sent.join_next().await {
            match done {
                Ok((_, Ok(()))) => {}
                Ok((instance, Err(e))) => {
                    tracing::warn!(
                        ?instance,
                        "listing the files we have open to a node failed: {e:#}"
                    );
                }
                Err(e) => tracing::warn!(%e, "listing the files we have open to a node panicked"),
            }
        }
    }
}

#[cfg(test)]
#[path = "remote_test.rs"]
mod tests;
