//! What rview-fs knows of the other machines, over NATS.
//!
//!   `{subject}.files`   whatever writes into an export announces its files that come
//!                       and go
//!   `{subject}.nodes`   each rview-fs heartbeats its node and how velo reaches it
//!
//! A node that shows up is listed whole, and every node again every few minutes, so an
//! announcement lost on the way is only late. A node that stops heartbeating is gone,
//! and so are its files.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerInfo};

use crate::index::{Index, NodeId, Stat};
use crate::path::RelPath;
use crate::state::State;
use crate::transfer;

const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);
const NODE_TTL: Duration = Duration::from_secs(30);
const RESCAN_EVERY: Duration = Duration::from_secs(600);

#[derive(Debug, Serialize, Deserialize)]
struct Heartbeat {
    node: String,
    peer: PeerInfo,
}

#[derive(Debug, Deserialize)]
struct Announcement {
    node: String,
    updates: Vec<Update>,
}

#[derive(Debug, Deserialize)]
struct Update {
    op: Op,
    path: String,
    size: u64,
    mtime: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Op {
    Add,
    Del,
}

struct Peer {
    instance: InstanceId,
    seen: Instant,
}

/// The other nodes' rview-fs that heartbeat.
#[derive(Default)]
pub struct Nodes {
    peers: HashMap<NodeId, Peer>,
}

impl Nodes {
    pub fn instance(&self, node: &NodeId) -> Option<InstanceId> {
        self.peers.get(node).map(|p| p.instance)
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Note a heartbeat. True when it is a node we did not know, or that restarted.
    fn heard(&mut self, node: NodeId, instance: InstanceId) -> bool {
        let fresh = self.instance(&node) != Some(instance);
        self.peers.insert(
            node,
            Peer {
                instance,
                seen: Instant::now(),
            },
        );
        fresh
    }

    fn expire(&mut self) -> Vec<(NodeId, InstanceId)> {
        let gone: Vec<(NodeId, InstanceId)> = self
            .peers
            .iter()
            .filter(|(_, p)| p.seen.elapsed() > NODE_TTL)
            .map(|(n, p)| (n.clone(), p.instance))
            .collect();
        for (node, _) in &gone {
            self.peers.remove(node);
        }
        gone
    }
}

pub fn files_subject(subject: &str) -> String {
    format!("{subject}.files")
}

pub fn nodes_subject(subject: &str) -> String {
    format!("{subject}.nodes")
}

/// Apply the announcements, from a subscription to `files_subject`, to the index until
/// cancelled.
pub async fn watch_files(
    mut messages: async_nats::Subscriber,
    state: Arc<State>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        let message = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            message = messages.next() => message.context("NATS subscription ended")?,
        };
        let announcement: Announcement = match serde_json::from_slice(&message.payload) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(%e, "skipping an announcement that does not parse");
                continue;
            }
        };
        let Some(node) = NodeId::parse(&announcement.node) else {
            continue;
        };
        apply(&mut state.index.write(), &node, announcement.updates);
    }
}

/// Apply `node`'s announced updates to the index. Hidden paths, of files still being
/// written, are not paths of an export, and are skipped like any other.
fn apply(index: &mut Index, node: &NodeId, updates: Vec<Update>) {
    for update in updates {
        let Some(path) = RelPath::parse(&update.path) else {
            tracing::warn!(path = update.path, "skipping a path outside the export");
            continue;
        };
        match update.op {
            Op::Add => index.add(
                node,
                &path,
                Stat {
                    size: update.size,
                    mtime: update.mtime,
                },
            ),
            Op::Del => index.delete(node, &path),
        }
    }
}

/// Heartbeat our node, keep track of the others from a subscription to `nodes_subject`,
/// and list each one that shows up, until cancelled.
pub async fn watch_nodes(
    nats: async_nats::Client,
    subject: String,
    mut messages: async_nats::Subscriber,
    state: Arc<State>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let beat = serde_json::to_vec(&Heartbeat {
        node: state.me.as_str().to_owned(),
        peer: state.velo.peer_info(),
    })?;
    let mut tick = tokio::time::interval(HEARTBEAT_EVERY);
    let mut listings = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {
                nats.publish(nodes_subject(&subject), beat.clone().into()).await?;
                let gone = state.nodes.write().expire();
                for (node, instance) in gone {
                    tracing::warn!(node = node.as_str(), "node stopped heartbeating; dropping its files");
                    state.index.write().remove_node(&node);
                    state.velo.unregister_peer(instance);
                }
            }
            message = messages.next() => {
                let message = message.context("NATS subscription ended")?;
                let Ok(heartbeat) = serde_json::from_slice::<Heartbeat>(&message.payload) else {
                    tracing::warn!("skipping a heartbeat that does not parse");
                    continue;
                };
                let Some(node) = NodeId::parse(&heartbeat.node) else { continue };
                if node == state.me {
                    continue;
                }
                let instance = heartbeat.peer.instance_id;
                if state.nodes.write().heard(node.clone(), instance) {
                    tracing::info!(node = node.as_str(), "node up; listing its files");
                    state.velo.register_peer(heartbeat.peer)?;
                    listings.spawn(list_node(Arc::clone(&state), node, instance));
                    // So that it knows us now, and not only at our next heartbeat.
                    nats.publish(nodes_subject(&subject), beat.clone().into()).await?;
                }
            }
            Some(done) = listings.join_next() => done?,
        }
    }
}

/// Our own export and every node's, again every few minutes, until cancelled.
pub async fn rescan(state: Arc<State>, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(RESCAN_EVERY);
    tick.tick().await; // the first tick is now, and startup scanned already
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        scan_local(&state).await?;
        let peers: Vec<(NodeId, InstanceId)> = {
            let nodes = state.nodes.read();
            nodes
                .peers
                .iter()
                .map(|(n, p)| (n.clone(), p.instance))
                .collect()
        };
        for (node, instance) in peers {
            list_node(Arc::clone(&state), node, instance).await;
        }
    }
}

/// Our export as it is on disk.
pub async fn scan_local(state: &State) -> anyhow::Result<()> {
    let root = state.export.clone();
    let started = Instant::now();
    let requested = state.index.read().mark();
    let listing = tokio::task::spawn_blocking(move || transfer::scan(&root)).await??;
    let files = parse_listing(listing.files);
    let count = files.len();
    state
        .index
        .write()
        .replace_node(&state.me, files, listing.listed_at, requested);
    tracing::info!(count, elapsed = ?started.elapsed(), "scanned the export");
    Ok(())
}

/// A peer's export, through its rview-fs. A peer that does not answer keeps the files we
/// knew, and is listed again later.
async fn list_node(state: Arc<State>, node: NodeId, instance: InstanceId) {
    let requested = state.index.read().mark();
    match transfer::list(&state.velo, instance).await {
        Ok(listing) => {
            let files = parse_listing(listing.files);
            let count = files.len();
            state
                .index
                .write()
                .replace_node(&node, files, listing.listed_at, requested);
            tracing::info!(node = node.as_str(), count, "listed a node's files");
        }
        Err(e) => tracing::warn!(node = node.as_str(), "listing a node failed: {e:#}"),
    }
}

fn parse_listing(files: Vec<transfer::Listed>) -> Vec<(RelPath, Stat)> {
    files
        .into_iter()
        .filter_map(|f| {
            Some((
                RelPath::parse(&f.path)?,
                Stat {
                    size: f.size,
                    mtime: f.mtime,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
#[path = "sync_test.rs"]
mod tests;
