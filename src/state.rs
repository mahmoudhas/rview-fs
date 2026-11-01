//! What the FUSE file system and the NATS tasks share.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::RwLock;
use velo::Velo;

use crate::cache::{ContentCache, Key};
use crate::content::Content;
use crate::health::Degraded;
use crate::index::{Index, NodeId};
use crate::sync::Nodes;
use crate::transfer::{self, Staged};

pub struct State {
    pub me: NodeId,
    pub export: PathBuf,
    // Read by every FUSE lookup, on many threads, and changed by the NATS tasks; each
    // holds it for one lookup or one batch of updates, and never across an await.
    pub index: RwLock<Index>,
    pub nodes: RwLock<Nodes>,
    pub cache: ContentCache<Content>,
    pub velo: Velo,
    /// Files move between nodes over RDMA.
    pub rdma: bool,
    pub degraded: Arc<Degraded>,
}

impl State {
    /// Ask the node `key` names to stage the file for us. None when it does not have
    /// it anymore.
    pub async fn stage(&self, key: &Key) -> anyhow::Result<Option<Staged>> {
        let instance = self
            .nodes
            .read()
            .instance(&key.node)
            .ok_or_else(|| anyhow!("node {} is not up", key.node.as_str()))?;
        transfer::stage(&self.velo, instance, &key.path).await
    }

    /// Tell the node `key` names we will not read the file it staged for us.
    pub async fn unstage(&self, key: &Key, staged: Staged) -> anyhow::Result<()> {
        let instance = self
            .nodes
            .read()
            .instance(&key.node)
            .ok_or_else(|| anyhow!("node {} is not up", key.node.as_str()))?;
        transfer::unstage(&self.velo, instance, staged).await
    }

    /// A file another node holds, fetched into memory. `staged` is a stage taken for it
    /// already, when the file was opened.
    pub async fn remote_content(
        &self,
        key: &Key,
        staged: Option<Staged>,
    ) -> anyhow::Result<Content> {
        self.cache
            .get_or_fetch(key, || async {
                let staged = match staged {
                    Some(staged) => staged,
                    None => self.stage(key).await?.ok_or_else(|| {
                        anyhow!("node {} no longer has {}", key.node.as_str(), key.path)
                    })?,
                };
                transfer::fetch(&self.velo, staged, key.path.as_str()).await
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
