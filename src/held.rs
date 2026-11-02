//! The files this node holds open for other nodes. Another node that opens a file of this
//! one has it opened here, and held open until it closes it: the file stays readable to
//! it if this node deletes it meanwhile, and none of it moves until it is read.
//!
//! Each held file is an `Arc<File>`. Closing it takes it out of the table, and its fd goes
//! with the last reference: a read still going keeps it until the read is done. Files
//! leave the table under its lock and are dropped, which closes them, after the lock is
//! released.
//!
//! Nothing a node fails to say leaves a file open here. Every RENEW_EVERY, each node lists
//! the files it still has open on this one (remote.rs); a file not on the list is closed,
//! unless it was opened within GRACE, when its open may not have answered yet. A node that
//! stops listing has all of its files closed once LEASE passes.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// How often a node lists the files it has open on another.
pub const RENEW_EVERY: Duration = Duration::from_secs(10);
/// A node silent this long has all of its files closed.
pub const LEASE: Duration = Duration::from_secs(60);
/// A file opened this recently is kept though its node did not list it: longer than an
/// open waits for its answer (transfer::CALL_TIMEOUT), so a list made while an open was on
/// its way never closes that file.
pub const GRACE: Duration = Duration::from_secs(120);

/// Held files, by the node that has them open (`K`) and the handle it has them by.
pub struct Held<K> {
    limit: usize,
    table: Mutex<Table<K>>,
}

struct Table<K> {
    next: u64,
    count: usize,
    clients: HashMap<K, Client>,
}

struct Client {
    /// Each file, and when it was opened.
    files: HashMap<u64, (Arc<File>, Instant)>,
    heard: Instant,
}

impl<K: Copy + Eq + Hash> Held<K> {
    /// At most `limit` files held at once.
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            table: Mutex::new(Table {
                next: 1,
                count: 0,
                clients: HashMap::new(),
            }),
        }
    }

    /// Hold `file` open for `client`, and the handle it reads and closes it by. The file
    /// comes back when `limit` files are held already.
    pub fn open(&self, client: K, file: File, now: Instant) -> Result<u64, File> {
        let mut table = self.table.lock();
        if table.count >= self.limit {
            return Err(file);
        }
        let handle = table.next;
        table.next += 1;
        table.count += 1;
        let held = table.clients.entry(client).or_insert_with(|| Client {
            files: HashMap::new(),
            heard: now,
        });
        held.heard = now;
        held.files.insert(handle, (Arc::new(file), now));
        Ok(handle)
    }

    /// The file `client` has open as `handle`. Whoever reads it keeps it open until done,
    /// though it is closed meanwhile.
    pub fn get(&self, client: K, handle: u64, now: Instant) -> Option<Arc<File>> {
        let mut table = self.table.lock();
        let held = table.clients.get_mut(&client)?;
        held.heard = now;
        held.files.get(&handle).map(|(file, _)| Arc::clone(file))
    }

    /// Close `client`'s `handle`. False when it was not open.
    pub fn close(&self, client: K, handle: u64) -> bool {
        let closed = {
            let mut guard = self.table.lock();
            let table = &mut *guard;
            let Some(held) = table.clients.get_mut(&client) else {
                return false;
            };
            let closed = held.files.remove(&handle);
            if held.files.is_empty() {
                table.clients.remove(&client);
            }
            if closed.is_some() {
                table.count -= 1;
            }
            closed
        };
        closed.is_some()
    }

    /// `client` has `open` open here, and nothing else: close its other files, but those
    /// opened within GRACE. How many it closed.
    pub fn renew(&self, client: K, open: &[u64], now: Instant) -> usize {
        let listed: HashSet<u64> = open.iter().copied().collect();
        let closed: Vec<Arc<File>> = {
            let mut guard = self.table.lock();
            let table = &mut *guard;
            let Some(held) = table.clients.get_mut(&client) else {
                return 0;
            };
            held.heard = now;
            let stale: Vec<u64> = held
                .files
                .iter()
                .filter(|(handle, (_, opened))| {
                    !listed.contains(handle) && now.saturating_duration_since(*opened) > GRACE
                })
                .map(|(handle, _)| *handle)
                .collect();
            let closed: Vec<Arc<File>> = stale
                .iter()
                .filter_map(|handle| held.files.remove(handle))
                .map(|(file, _)| file)
                .collect();
            if held.files.is_empty() {
                table.clients.remove(&client);
            }
            table.count -= closed.len();
            closed
        };
        closed.len()
    }

    /// Close every file of the nodes silent for LEASE. How many it closed.
    pub fn expire(&self, now: Instant) -> usize {
        let closed: Vec<Client> = {
            let mut guard = self.table.lock();
            let table = &mut *guard;
            let silent: Vec<K> = table
                .clients
                .iter()
                .filter(|(_, held)| now.saturating_duration_since(held.heard) > LEASE)
                .map(|(client, _)| *client)
                .collect();
            let closed: Vec<Client> = silent
                .iter()
                .filter_map(|client| table.clients.remove(client))
                .collect();
            table.count -= closed.iter().map(|held| held.files.len()).sum::<usize>();
            closed
        };
        closed.iter().map(|held| held.files.len()).sum()
    }

    /// Files held now.
    pub fn count(&self) -> usize {
        self.table.lock().count
    }
}

#[cfg(test)]
#[path = "held_test.rs"]
mod tests;
