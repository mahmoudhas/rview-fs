//! What the FUSE file system and the NATS tasks share.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use velo::Velo;

use crate::cache::ContentCache;
use crate::health::Degraded;
use crate::index::{Index, NodeId};
use crate::remote::{Remote, RemoteFiles};
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
    pub degraded: Arc<Degraded>,
}

impl State {
    /// A file another node holds open for us, fetched whole, or from the cache when it
    /// was read before.
    pub async fn remote_content(&self, remote: &Remote) -> anyhow::Result<Bytes> {
        self.cache
            .get_or_fetch(&remote.key, || {
                transfer::fetch(&self.velo, remote.instance, remote.handle)
            })
            .await
            .inspect_err(|e| {
                if self.rdma && e.chain().any(|c| c.is::<tokio::time::error::Elapsed>()) {
                    self.degraded.report(
                        "working RDMA",
                        format!(
                            "{e:#}: a transfer never completed, which is often UCX on the wrong \
                             device: set UCX_NET_DEVICES to one port (mlx5_0:1, for one), or run \
                             with --no-rdma"
                        ),
                    );
                }
            })
    }
}
