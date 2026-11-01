//! What the FUSE file system and the NATS tasks share.

use std::path::PathBuf;

use anyhow::anyhow;
use bytes::Bytes;
use parking_lot::RwLock;
use velo::Velo;

use crate::cache::{ContentCache, Key};
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
    pub cache: ContentCache,
    pub velo: Velo,
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

    /// The bytes of a file another node holds, in memory once read. `staged` is a stage
    /// taken for it already, when the file was opened.
    pub async fn remote_bytes(&self, key: &Key, staged: Option<Staged>) -> anyhow::Result<Bytes> {
        self.cache
            .get_or_fetch(key, || async {
                let staged = match staged {
                    Some(staged) => staged,
                    None => self.stage(key).await?.ok_or_else(|| {
                        anyhow!("node {} no longer has {}", key.node.as_str(), key.path)
                    })?,
                };
                transfer::fetch(&self.velo, staged).await
            })
            .await
    }
}
