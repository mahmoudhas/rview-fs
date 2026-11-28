//! What the FUSE file system and the NATS tasks share.

use std::path::PathBuf;

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use parking_lot::RwLock;
use velo::Velo;

use crate::cache::{ContentCache, Key};
use crate::index::{Index, NodeId};
use crate::remote::{Remote, RemoteFiles, Stalls, TCP_AFTER_STALL, Via};
use crate::sync::Nodes;
use crate::transfer::{self, Opened};
use crate::used::Used;

pub struct State {
    pub me: NodeId,
    pub export: PathBuf,
    // Read by every FUSE lookup, on many threads, and changed by the NATS tasks; each
    // holds it for one lookup or one batch of updates, and never across an await.
    pub index: RwLock<Index>,
    // Held while a listing goes into `index` (sync.rs): taken before it, never after.
    pub nodes: RwLock<Nodes>,
    /// Files of other nodes, once read.
    pub cache: ContentCache<Bytes>,
    /// The files of other nodes this node has open.
    pub remote: RemoteFiles,
    /// The files of other nodes opened from `cache`, which their nodes are yet to be told.
    pub used: Used,
    pub velo: Velo,
    /// Files move between nodes over RDMA.
    pub rdma: bool,
    /// The nodes whose files are read over TCP for now.
    pub stalls: Stalls,
    pub read_bytes: transfer::ReadBytes,
    pub ops: transfer::OpTimes,
}

impl State {
    /// A file another node holds open for us, fetched whole, or from the cache when it
    /// was read before.
    pub async fn remote_content(&self, remote: &Remote) -> anyhow::Result<Bytes> {
        self.cache
            .get_or_fetch(&remote.key, || self.fetch_held(remote))
            .await
            .with_context(|| {
                format!(
                    "reading {} of {}",
                    remote.key.path,
                    remote.key.node.as_str()
                )
            })
    }

    /// Have the node `key` names open its file for us, and hold it open until we close
    /// it. None when it does not have it (anymore). A node that restarts with the call in
    /// hand never answers it: the node as it is up then is asked, at once.
    pub async fn open_remote(&self, key: &Key) -> anyhow::Result<Option<Remote>> {
        loop {
            let up = self.nodes.read().up(&key.node);
            let (instance, gone) =
                up.ok_or_else(|| anyhow!("node {} is not up", key.node.as_str()))?;
            let opened = tokio::select! {
                opened = transfer::open(&self.velo, instance, &key.path) => opened?,
                () = gone.cancelled() => continue,
            };
            return Ok(opened.map(|Opened { handle, size }| Remote {
                key: key.clone(),
                instance,
                handle,
                size,
            }));
        }
    }

    /// The file `remote`, from the node that holds it open for us. When that node
    /// restarted, before the fetch or under it, what it held went with it and it never
    /// answers: the node as it is up now opens the file anew, for this fetch. A node that
    /// only went silent may still answer, and is waited for as long as a call is.
    async fn fetch_held(&self, remote: &Remote) -> anyhow::Result<Bytes> {
        let node = &remote.key.node;
        let up = self.nodes.read().up(node);
        match up {
            Some((instance, gone)) if instance == remote.instance => {
                let fetching = self.fetch(remote);
                tokio::pin!(fetching);
                tokio::select! {
                    fetched = &mut fetching => return fetched,
                    () = gone.cancelled() => {}
                }
                if self.nodes.read().up(node).is_none() {
                    return fetching.await;
                }
            }
            // Up as another instance: it restarted since it opened the file.
            Some(_) => {}
            None => return self.fetch(remote).await,
        }
        let again = self
            .open_remote(&remote.key)
            .await?
            .ok_or_else(|| anyhow!("its node restarted and has it no more"))?;
        let fetched = self.fetch(&again).await;
        let velo = self.velo.clone();
        tokio::spawn(async move {
            if let Err(e) = transfer::close(&velo, again.instance, again.handle).await {
                tracing::debug!("closing a file opened anew for a fetch failed: {e:#}");
            }
        });
        fetched
    }

    /// A file another node holds open for us: over RDMA, unless reading one of its files
    /// so stalled; then over TCP.
    async fn fetch(&self, remote: &Remote) -> anyhow::Result<Bytes> {
        let (node, instance, handle) = (&remote.key.node, remote.instance, remote.handle);
        let via = if self.rdma {
            self.stalls.via(node, instance)
        } else {
            Via::Tcp
        };
        if via == Via::Tcp {
            return transfer::fetch(&self.velo, instance, handle, false, &self.read_bytes).await;
        }
        match transfer::fetch(&self.velo, instance, handle, true, &self.read_bytes).await {
            Err(e) if e.chain().any(|c| c.is::<tokio::time::error::Elapsed>()) => {
                if self.stalls.stalled(node, instance) || via == Via::RdmaAgain {
                    tracing::error!(
                        node = node.as_str(),
                        "{e:#}: reading the node's files over RDMA stalled, so they are read \
                         over TCP, and over RDMA again in {TCP_AFTER_STALL:?}. UCX on the wrong \
                         device stalls every one (set UCX_NET_DEVICES); UCX_LOG_LEVEL=diag says \
                         what UCX saw"
                    );
                }
                transfer::fetch(&self.velo, instance, handle, false, &self.read_bytes).await
            }
            fetched => {
                if via == Via::RdmaAgain && fetched.is_ok() {
                    self.stalls.worked(node);
                    tracing::info!(
                        node = node.as_str(),
                        "reading the node's files over RDMA works again"
                    );
                }
                fetched
            }
        }
    }
}

/// A node as the tests of more than one module build it.
#[cfg(test)]
pub mod testing {
    use std::path::PathBuf;
    use std::sync::Arc;

    use parking_lot::RwLock;
    use velo::Velo;
    use velo::transports::tcp::TcpTransportBuilder;

    use super::State;
    use crate::cache::ContentCache;
    use crate::index::{Index, NodeId};
    use crate::remote::{RemoteFiles, Stalls};
    use crate::transfer;
    use crate::used::Used;

    /// A node's velo, over TCP on this host.
    pub async fn velo() -> Velo {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp = TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap();
        Velo::builder()
            .add_transport(Arc::new(tcp))
            .build()
            .await
            .unwrap()
    }

    /// What this node knows, which reaches the others with `velo`.
    pub fn state(velo: Velo) -> Arc<State> {
        state_of(velo, PathBuf::from("/nonexistent"), 0)
    }

    /// The same, of a node that exports `export` and keeps `cache` bytes of other nodes'
    /// files.
    pub fn state_of(velo: Velo, export: PathBuf, cache: u64) -> Arc<State> {
        Arc::new(State {
            me: NodeId::parse("a").unwrap(),
            export,
            index: RwLock::new(Index::new()),
            nodes: RwLock::default(),
            cache: ContentCache::new(cache),
            remote: RemoteFiles::default(),
            used: Used::default(),
            velo,
            rdma: false,
            stalls: Stalls::default(),
            read_bytes: transfer::ReadBytes::default(),
            ops: transfer::OpTimes::default(),
        })
    }
}
