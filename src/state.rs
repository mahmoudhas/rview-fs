//! What the FUSE file system and the NATS tasks share.

use std::path::PathBuf;

use anyhow::Context as _;
use bytes::Bytes;
use parking_lot::RwLock;
use velo::Velo;

use crate::cache::ContentCache;
use crate::index::{Index, NodeId};
use crate::remote::{Remote, RemoteFiles, Stalls, TCP_AFTER_STALL, Via};
use crate::sync::Nodes;
use crate::transfer;

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
    pub velo: Velo,
    /// Files move between nodes over RDMA.
    pub rdma: bool,
    /// The nodes whose files are read over TCP for now.
    pub stalls: Stalls,
    pub read_bytes: transfer::ReadBytes,
}

impl State {
    /// A file another node holds open for us, fetched whole, or from the cache when it
    /// was read before.
    pub async fn remote_content(&self, remote: &Remote) -> anyhow::Result<Bytes> {
        self.cache
            .get_or_fetch(&remote.key, || self.fetch(remote))
            .await
            .with_context(|| {
                format!(
                    "reading {} of {}",
                    remote.key.path,
                    remote.key.node.as_str()
                )
            })
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
