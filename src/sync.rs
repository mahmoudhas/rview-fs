//! What rview-fs knows of the other machines, over NATS.
//!
//!   `{subject}.files`   whatever writes into an export announces its files that come
//!                       and go
//!   `{subject}.nodes`   each rview-fs heartbeats its node and how velo reaches it
//!
//! A node that shows up, new or restarted, is listed whole, and again until it answers;
//! every node is listed again every few minutes, and when NATS is back after it was
//! away, so an announcement lost on the way is only late. A node that stops heartbeating
//! is gone, and so are its files.

use std::collections::HashMap;
use std::future::{Ready, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use futures::StreamExt as _;
use parking_lot::RwLockWriteGuard;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerInfo};

use crate::index::{Index, NodeId, Replacement, Stat};
use crate::path::RelPath;
use crate::state::State;
use crate::transfer;

const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);
const NODE_TTL: Duration = Duration::from_secs(30);
/// A node that showed up and did not answer its listing is listed again this much later,
/// and twice as long after each time since, up to LIST_AGAIN_MOST.
const LIST_AGAIN_AFTER: Duration = Duration::from_secs(1);
const LIST_AGAIN_MOST: Duration = Duration::from_secs(30);
/// How long the index is held at a time to put a listing in, and how long it is then left
/// to the lookups, which all wait while it is held. The files a part takes in that time
/// go with their paths' length and the index's size: 2000 at a time, a millisecond's worth
/// with 170,000 files of short paths, were 8 ms with a million files of 1 kB paths.
const LISTING_HOLD: Duration = Duration::from_micros(250);
const LISTING_PAUSE: Duration = Duration::from_millis(1);
/// The files of a listing looked at between two looks at the clock.
const LISTING_STEP: usize = 64;

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

/// What a heartbeat tells of its node.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Heard {
    /// It is up as the instance we know.
    Again,
    /// We did not know it: it is new, or back after it went.
    New,
    /// It is up as another instance: it restarted. The instance it was.
    Restarted(InstanceId),
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

    /// Note a heartbeat.
    pub(crate) fn heard(&mut self, node: NodeId, instance: InstanceId) -> Heard {
        let seen = Instant::now();
        match self.peers.insert(node, Peer { instance, seen }) {
            None => Heard::New,
            Some(was) if was.instance == instance => Heard::Again,
            Some(was) => Heard::Restarted(was.instance),
        }
    }

    /// Forget `node` until its next heartbeat, as though it went.
    fn forget(&mut self, node: &NodeId) {
        self.peers.remove(node);
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
/// and list each one that shows up, new or restarted, until cancelled.
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
                let heard = state.nodes.write().heard(node.clone(), instance);
                match heard {
                    Heard::Again => continue,
                    Heard::New => tracing::info!(node = node.as_str(), "node up; listing its files"),
                    Heard::Restarted(was) => {
                        tracing::info!(node = node.as_str(), "node restarted; listing its files");
                        // Velo keeps what it has of an instance until told it is gone.
                        state.velo.unregister_peer(was);
                    }
                }
                if let Err(e) = state.velo.register_peer(heartbeat.peer) {
                    // Left out, as though gone, until it heartbeats an address velo can use.
                    tracing::warn!(node = node.as_str(), "a node came up at an address velo cannot use: {e:#}");
                    state.nodes.write().forget(&node);
                    state.index.write().remove_node(&node);
                    continue;
                }
                listings.spawn(list_until_listed(Arc::clone(&state), node, instance));
                // So that it knows us now, and not only at our next heartbeat.
                nats.publish(nodes_subject(&subject), beat.clone().into()).await?;
            }
            Some(done) = listings.join_next() => done?,
        }
    }
}

/// What the NATS client calls at each of its events: `back` is notified when the
/// connection is made again after it was lost, and not when it is first made.
pub fn reconnects(back: Arc<Notify>) -> impl Fn(async_nats::Event) -> Ready<()> + Send + Sync {
    let connected_before = AtomicBool::new(false);
    move |event| {
        if matches!(event, async_nats::Event::Connected)
            && connected_before.swap(true, Ordering::Relaxed)
        {
            back.notify_one();
        }
        ready(())
    }
}

/// Our own export and every node's, again every `every` and whenever `nats_back` is
/// notified, until cancelled.
pub async fn rescan(
    state: Arc<State>,
    every: Duration,
    nats_back: Arc<Notify>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(every);
    tick.tick().await; // the first tick is now, and startup scanned already
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
            () = nats_back.notified() => {
                // What was announced while it was away was never heard.
                tracing::info!("NATS is back; listing every node again");
                tick.reset();
            }
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
            // It keeps the files we knew, until the next time.
            if let Err(e) = list_node(&state, &node, instance).await {
                tracing::warn!(node = node.as_str(), "listing a node failed: {e:#}");
            }
        }
    }
}

/// Our export as it is on disk.
pub async fn scan_local(state: &State) -> anyhow::Result<()> {
    let (root, me) = (state.export.clone(), state.me.clone());
    let started = Instant::now();
    let requested = state.index.read().mark();
    let mut replacement = tokio::task::spawn_blocking(move || {
        transfer::scan(&root).map(|listing| replacement(&me, listing, requested))
    })
    .await??;
    let count = replacement.count();
    while replace_part(state.index.write(), &mut replacement) {
        tokio::time::sleep(LISTING_PAUSE).await;
    }
    tracing::info!(count, elapsed = ?started.elapsed(), "scanned the export");
    Ok(())
}

/// `node`'s listing, requested at `requested`, ready to go into the index. Blocks: it
/// sorts the listing.
fn replacement(node: &NodeId, listing: transfer::Listing, requested: u64) -> Replacement {
    Replacement::new(node, listing.files, listing.listed_at, requested)
}

/// The next part of a listing into the index, which is held for LISTING_HOLD and no
/// longer: every lookup waits for whoever holds it. Whoever waited has it next, and the
/// caller leaves it alone for LISTING_PAUSE. False once the listing is in.
fn replace_part(mut index: RwLockWriteGuard<'_, Index>, replacement: &mut Replacement) -> bool {
    let held = Instant::now();
    let mut more = true;
    while more && held.elapsed() < LISTING_HOLD {
        more = index.replace_part(replacement, LISTING_STEP);
    }
    RwLockWriteGuard::unlock_fair(index);
    more
}

/// List a node that came up, as `instance`, until it answers: until then, of the files it
/// had when it came up, we show only those we knew of it before. Not once it is gone, or
/// restarted: its next instance is listed on its own.
async fn list_until_listed(state: Arc<State>, node: NodeId, instance: InstanceId) {
    let mut wait = LIST_AGAIN_AFTER;
    while let Err(e) = list_node(&state, &node, instance).await {
        tracing::warn!(
            node = node.as_str(),
            "listing a node that came up failed; again in {wait:?}: {e:#}"
        );
        tokio::time::sleep(wait).await;
        if state.nodes.read().instance(&node) != Some(instance) {
            return;
        }
        wait = (wait * 2).min(LIST_AGAIN_MOST);
    }
}

/// A peer's export, through its rview-fs as `instance`. A peer that does not answer keeps
/// the files we knew. The listing of a peer that went, or restarted, while it listed is
/// dropped: it would bring back the files of one gone.
async fn list_node(state: &State, node: &NodeId, instance: InstanceId) -> anyhow::Result<()> {
    let requested = state.index.read().mark();
    let listing = transfer::list(&state.velo, instance).await?;
    let of = node.clone();
    let mut replacement =
        tokio::task::spawn_blocking(move || replacement(&of, listing, requested)).await?;
    let count = replacement.count();
    loop {
        {
            // Held while a part of the files goes in, so that the node does not expire
            // in between and leave them behind; one that expired between two parts took
            // the parts before with it.
            let nodes = state.nodes.read();
            if nodes.instance(node) != Some(instance) {
                tracing::info!(
                    node = node.as_str(),
                    "dropped the listing of a node that went or restarted since"
                );
                return Ok(());
            }
            if !replace_part(state.index.write(), &mut replacement) {
                break;
            }
        }
        tokio::time::sleep(LISTING_PAUSE).await;
    }
    tracing::info!(node = node.as_str(), count, "listed a node's files");
    Ok(())
}

#[cfg(test)]
#[path = "sync_test.rs"]
mod tests;
