//! The union of every node's exported files: which nodes hold each file.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::path::{RelPath, join};

/// The machine an export is on, which its files are announced under and which its
/// rview-fs heartbeats with.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(String);

impl NodeId {
    pub fn parse(name: &str) -> Option<Self> {
        (!name.is_empty() && !name.contains(char::is_whitespace)).then(|| Self(name.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Stat {
    pub size: u64,
    pub mtime: f64,
}

/// A file of the union, and the nodes that hold it.
#[derive(Clone, Debug, PartialEq)]
pub struct FileView {
    pub path: RelPath,
    pub size: u64,
    pub mtime: f64,
    pub nodes: Vec<NodeId>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    Dir,
    File(FileView),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
}

#[derive(Default)]
struct Dir {
    files: BTreeMap<String, BTreeMap<NodeId, Stat>>,
    dirs: BTreeSet<String>,
}

impl Dir {
    fn is_empty(&self) -> bool {
        self.files.is_empty() && self.dirs.is_empty()
    }
}

pub struct Index {
    dirs: HashMap<String, Dir>,
    /// Counts the deletions announced, which `mark` reads.
    clock: u64,
    /// The files whose deletion was announced, and when on `clock`: a listing requested
    /// before then may still have them.
    deleted: HashMap<(NodeId, RelPath), u64>,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            dirs: HashMap::from([(String::new(), Dir::default())]),
            clock: 0,
            deleted: HashMap::new(),
        }
    }

    /// Where we are, to request a listing at: see `replace_node`.
    pub fn mark(&self) -> u64 {
        self.clock
    }

    /// A file `node` announced it deleted.
    pub fn delete(&mut self, node: &NodeId, path: &RelPath) {
        self.clock += 1;
        self.deleted
            .insert((node.clone(), path.clone()), self.clock);
        self.remove(node, path);
    }

    pub fn add(&mut self, node: &NodeId, path: &RelPath, stat: Stat) {
        self.deleted.remove(&(node.clone(), path.clone()));
        let (dir, name) = path.split();
        self.ensure_dir(dir);
        self.dir_mut(dir)
            .files
            .entry(name.to_owned())
            .or_default()
            .insert(node.clone(), stat);
    }

    pub fn remove(&mut self, node: &NodeId, path: &RelPath) {
        let (dir, name) = path.split();
        let Some(d) = self.dirs.get_mut(dir) else {
            return;
        };
        let Some(sources) = d.files.get_mut(name) else {
            return;
        };
        if sources.remove(node).is_none() {
            return;
        }
        if sources.is_empty() {
            d.files.remove(name);
        }
        self.prune(dir);
    }

    /// Forget every file of `node`, when it went away.
    pub fn remove_node(&mut self, node: &NodeId) {
        for path in self.paths_of(node) {
            self.remove(node, &path);
        }
        self.deleted.retain(|(n, _), _| n != node);
    }

    /// Make `node`'s files those of a listing it took at `listed_at`, which we requested
    /// at `requested` (a `mark`), keeping the files written since, which it announced
    /// after the listing was taken, and not taking back the ones it announced deleted
    /// since we requested it.
    pub fn replace_node(
        &mut self,
        node: &NodeId,
        files: Vec<(RelPath, Stat)>,
        listed_at: f64,
        requested: u64,
    ) {
        let files: Vec<(RelPath, Stat)> = files
            .into_iter()
            .filter(|(path, _)| {
                self.deleted
                    .get(&(node.clone(), path.clone()))
                    .is_none_or(|&at| at <= requested)
            })
            .collect();
        // The listing has every deletion announced before it was requested.
        self.deleted
            .retain(|(n, _), &mut at| n != node || at > requested);
        let listed: HashSet<&RelPath> = files.iter().map(|(path, _)| path).collect();
        let stale: Vec<RelPath> = self
            .paths_of(node)
            .into_iter()
            .filter(|path| !listed.contains(path))
            .filter(|path| {
                let (dir, name) = path.split();
                self.dirs[dir].files[name][node].mtime < listed_at
            })
            .collect();
        for path in &stale {
            self.remove(node, path);
        }
        for (path, stat) in &files {
            self.add(node, path, *stat);
        }
    }

    pub fn lookup(&self, path: &str) -> Option<Entry> {
        if self.dirs.contains_key(path) {
            return Some(Entry::Dir);
        }
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        let sources = self.dirs.get(dir)?.files.get(name)?;
        Some(Entry::File(FileView {
            path: RelPath::parse(path)?,
            size: sources.values().next()?.size,
            mtime: sources.values().map(|s| s.mtime).reduce(f64::max)?,
            nodes: sources.keys().cloned().collect(),
        }))
    }

    /// The entries of a folder, folders first.
    pub fn list(&self, dir: &str) -> Option<Vec<(String, EntryKind)>> {
        let d = self.dirs.get(dir)?;
        let dirs = d.dirs.iter().map(|n| (n.clone(), EntryKind::Dir));
        let files = d.files.keys().map(|n| (n.clone(), EntryKind::File));
        Some(dirs.chain(files).collect())
    }

    pub fn file_count(&self) -> usize {
        self.dirs.values().map(|d| d.files.len()).sum()
    }

    fn paths_of(&self, node: &NodeId) -> Vec<RelPath> {
        self.dirs
            .iter()
            .flat_map(|(dir, d)| {
                d.files
                    .iter()
                    .filter(|(_, sources)| sources.contains_key(node))
                    .filter_map(move |(name, _)| RelPath::parse(&join(dir, name)))
            })
            .collect()
    }

    fn dir_mut(&mut self, dir: &str) -> &mut Dir {
        self.dirs
            .get_mut(dir)
            .expect("ensure_dir made every folder of the path")
    }

    fn ensure_dir(&mut self, dir: &str) {
        if self.dirs.contains_key(dir) {
            return;
        }
        let (parent, name) = dir.rsplit_once('/').unwrap_or(("", dir));
        self.ensure_dir(parent);
        self.dir_mut(parent).dirs.insert(name.to_owned());
        self.dirs.insert(dir.to_owned(), Dir::default());
    }

    /// Remove `dir` and the folders above it that hold nothing anymore.
    fn prune(&mut self, dir: &str) {
        let mut dir = dir.to_owned();
        while !dir.is_empty() && self.dirs.get(&dir).is_some_and(Dir::is_empty) {
            self.dirs.remove(&dir);
            let (parent, name) = dir.rsplit_once('/').unwrap_or(("", &dir));
            let parent = parent.to_owned();
            self.dir_mut(&parent).dirs.remove(name);
            dir = parent;
        }
    }
}

#[cfg(test)]
#[path = "index_test.rs"]
mod tests;
