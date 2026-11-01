//! Files of other nodes, in host memory once fetched: whole, by one reader while the
//! others wait, and dropped least recently used first past a budget. Files never change
//! once written, so a cached file is never stale.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

use crate::content::Content;
use crate::index::NodeId;
use crate::path::RelPath;

/// What the cache holds, and the bytes of memory each takes.
pub trait Weighed: Clone {
    fn weight(&self) -> u64;
}

impl Weighed for Bytes {
    fn weight(&self) -> u64 {
        self.len() as u64
    }
}

impl Weighed for Content {
    fn weight(&self) -> u64 {
        self.bytes.len() as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    pub node: NodeId,
    pub path: RelPath,
}

struct Slot<V> {
    bytes: Arc<OnceCell<V>>,
    last_used: u64,
    counted: bool,
}

struct Slots<V> {
    slots: HashMap<Key, Slot<V>>,
    clock: u64,
    bytes: u64,
}

pub struct ContentCache<V: Weighed> {
    budget: u64,
    // Every read takes it for a moment to find its file; never across an await.
    slots: Mutex<Slots<V>>,
}

impl<V: Weighed> ContentCache<V> {
    pub fn new(budget: u64) -> Self {
        Self {
            budget,
            slots: Mutex::new(Slots {
                slots: HashMap::new(),
                clock: 0,
                bytes: 0,
            }),
        }
    }

    pub fn get(&self, key: &Key) -> Option<V> {
        let mut s = self.slots.lock();
        s.clock += 1;
        let clock = s.clock;
        let slot = s.slots.get_mut(key)?;
        slot.last_used = clock;
        slot.bytes.get().cloned()
    }

    /// The file's bytes, fetched with `fetch` unless they are here or on their way.
    /// A failed fetch is not kept: the next read tries again.
    pub async fn get_or_fetch<F, Fut>(&self, key: &Key, fetch: F) -> anyhow::Result<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<V>>,
    {
        let cell = {
            let mut s = self.slots.lock();
            s.clock += 1;
            let clock = s.clock;
            let slot = s.slots.entry(key.clone()).or_insert_with(|| Slot {
                bytes: Arc::new(OnceCell::new()),
                last_used: clock,
                counted: false,
            });
            slot.last_used = clock;
            Arc::clone(&slot.bytes)
        };
        match cell.get_or_try_init(fetch).await {
            Ok(bytes) => {
                let bytes = bytes.clone();
                self.account(key);
                Ok(bytes)
            }
            Err(e) => {
                self.forget_unfetched(key, &cell);
                Err(e)
            }
        }
    }

    /// Drop the slot of a fetch that failed, unless another reader filled it since or it
    /// is another slot by now: an empty slot is never evicted.
    fn forget_unfetched(&self, key: &Key, cell: &Arc<OnceCell<V>>) {
        let mut s = self.slots.lock();
        if let Some(slot) = s.slots.get(key)
            && Arc::ptr_eq(&slot.bytes, cell)
            && slot.bytes.get().is_none()
        {
            s.slots.remove(key);
        }
    }

    pub fn len(&self) -> usize {
        self.slots.lock().slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> u64 {
        self.slots.lock().bytes
    }

    /// Count a newly fetched file, and drop the least recently used others past the
    /// budget. Readers that hold a file's bytes keep them until they are done.
    fn account(&self, key: &Key) {
        let mut s = self.slots.lock();
        let Some(slot) = s.slots.get_mut(key) else {
            return;
        };
        let Some(len) = slot.bytes.get().map(Weighed::weight) else {
            return;
        };
        if slot.counted {
            return;
        }
        slot.counted = true;
        s.bytes += len;
        while s.bytes > self.budget {
            let victim = s
                .slots
                .iter()
                .filter(|(k, slot)| slot.counted && *k != key)
                .min_by_key(|(_, slot)| slot.last_used)
                .map(|(k, _)| k.clone());
            let Some(victim) = victim else {
                break;
            };
            if let Some(gone) = s.slots.remove(&victim) {
                s.bytes -= gone.bytes.get().map_or(0, Weighed::weight);
            }
        }
    }
}

#[cfg(test)]
#[path = "cache_test.rs"]
mod tests;
