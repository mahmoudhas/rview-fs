//! A file that is read counts as used on the machine whose export holds it: its access
//! time is renewed there, and its modification time left alone. A cleanup of an export
//! that deletes the least recently read files then keeps the ones any machine still
//! reads. The time is set outright, so this holds on a file system mounted `noatime` or
//! `relatime`, where a read alone leaves it.
//!
//! A file is marked when it is opened through a mount:
//!
//! - this machine's own, here (fs.rs); opens that share a backing file passed through
//!   open nothing of the export, and mark it at most every MARK_EVERY;
//! - another machine's, there, as it opens the file for us (transfer.rs);
//! - another machine's that was read before and is kept in memory (cache.rs), which opens
//!   without asking its machine: there too, once that machine is told, within TELL_EVERY.

use std::collections::{HashMap, HashSet};
use std::fs::{File, FileTimes};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::cache::Key;
use crate::index::NodeId;
use crate::path::{self, RelPath};
use crate::state::State;
use crate::transfer;

/// How often a file whose opens share one backing file is marked.
pub const MARK_EVERY: Duration = Duration::from_secs(1);
/// How often a machine is told which of its files were opened from memory here.
pub const TELL_EVERY: Duration = Duration::from_secs(5);
/// The paths one message tells of: a path deep in folders is a few KiB, and a message
/// carries 16 MiB.
const PATHS_A_MESSAGE: usize = 1000;
/// A machine that cannot be told, and files that cannot be marked, are said so this often.
const WARN_EVERY: Duration = Duration::from_secs(600);

/// Renew the access time of `file`, and leave its modification time. Blocks.
pub fn mark(file: &File) -> io::Result<()> {
    file.set_times(FileTimes::new().set_accessed(SystemTime::now()))
}

/// Mark the file at `path` in the export at `root`. False when it is not there (anymore).
/// Blocks.
pub fn mark_export_file(root: &Path, path: &RelPath) -> io::Result<bool> {
    match path::open_file(root, path) {
        Ok(file) => mark(&file).map(|()| true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Say that a file could not be marked, for `e`: loudly, as what rview-fs runs without
/// (health.rs), and not again for WARN_EVERY, as every file fails alike: an export
/// mounted read-only, or files that are not ours to set the times of.
pub fn cannot_mark(e: &io::Error) {
    static SAID: Mutex<Option<Instant>> = Mutex::new(None);
    let mut said = SAID.lock();
    if said.is_some_and(|at| at.elapsed() < WARN_EVERY) {
        return;
    }
    *said = Some(Instant::now());
    tracing::error!(
        "DEGRADED: marking the files read as used is not available: {e}: a cleanup of the \
         export that keeps the files last read deletes files still read. rview-fs sets \
         their access times, which takes the export mounted read-write and its files \
         owned by rview-fs's user, or CAP_FOWNER"
    );
}

/// The files of other nodes opened from memory here that their nodes were not told of yet.
#[derive(Default)]
pub struct Used {
    // Taken for a moment by every such open, and every TELL_EVERY.
    opened: Mutex<HashMap<NodeId, HashSet<RelPath>>>,
    warned: Mutex<HashMap<NodeId, Instant>>,
}

impl Used {
    /// A file of another node was opened from memory.
    pub fn note(&self, key: &Key) {
        let mut opened = self.opened.lock();
        match opened.get_mut(&key.node) {
            Some(paths) => {
                paths.insert(key.path.clone());
            }
            None => {
                opened.insert(key.node.clone(), HashSet::from([key.path.clone()]));
            }
        }
    }

    /// What was noted since this was last called, by node.
    pub(crate) fn take(&self) -> HashMap<NodeId, HashSet<RelPath>> {
        std::mem::take(&mut *self.opened.lock())
    }

    /// Whether to say that `node` could not be told: not more than every WARN_EVERY.
    fn warns(&self, node: &NodeId) -> bool {
        let mut warned = self.warned.lock();
        let due = warned.get(node).is_none_or(|at| at.elapsed() >= WARN_EVERY);
        if due {
            warned.insert(node.clone(), Instant::now());
        }
        due
    }
}

/// Tell each node which of its files were opened from memory here, every TELL_EVERY,
/// until cancelled. A node that went meanwhile is not told: its files went with it.
pub async fn tell(state: Arc<State>, cancel: CancellationToken) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(TELL_EVERY);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        tell_once(&state).await;
    }
}

/// One round of `tell`.
pub async fn tell_once(state: &Arc<State>) {
    let mut sent = tokio::task::JoinSet::new();
    for (node, paths) in state.used.take() {
        let Some(instance) = state.nodes.read().instance(&node) else {
            continue;
        };
        let paths: Vec<String> = paths.iter().map(|p| p.as_str().to_owned()).collect();
        let velo = state.velo.clone();
        sent.spawn(async move {
            for part in paths.chunks(PATHS_A_MESSAGE) {
                if let Err(e) = transfer::used(&velo, instance, part.to_vec()).await {
                    return (node, Err(e));
                }
            }
            (node, Ok(()))
        });
    }
    while let Some(done) = sent.join_next().await {
        match done {
            Ok((_, Ok(()))) => {}
            Ok((node, Err(e))) => {
                if state.used.warns(&node) {
                    tracing::warn!(
                        node = node.as_str(),
                        "telling a node which of its files were read from memory here failed, \
                         so its cleanup may take them for unused (an rview-fs from before it \
                         took such notes?): {e:#}"
                    );
                }
            }
            Err(e) => tracing::warn!(%e, "telling a node of its files read here panicked"),
        }
    }
}

#[cfg(test)]
#[path = "used_test.rs"]
pub(crate) mod tests;
