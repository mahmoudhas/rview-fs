//! The union of every node's exported files: which nodes hold each file.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

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
    /// In order, so that a listing's sweep reads on from the folder it stopped at.
    dirs: BTreeMap<String, Dir>,
    /// Counts the deletions announced, which `mark` reads.
    clock: u64,
    /// The files whose deletion was announced, by node, and when on `clock`: a listing
    /// requested before then may still have them.
    deleted: HashMap<NodeId, HashMap<RelPath, u64>>,
}

/// A listing of a node's files on its way into the index, a part at a time
/// (`Index::replace_part`): whole, the index is held for all of it, which every lookup
/// then waits for (0.6 s for a node of 170,000 files).
pub struct Replacement {
    node: NodeId,
    listed_at: f64,
    requested: u64,
    /// The listing, in path order.
    files: Vec<(RelPath, Stat)>,
    /// How many of `files` are in.
    added: usize,
    swept: Swept,
    begun: bool,
}

/// How far the sweep for the files a node no longer has is through the index's folders.
enum Swept {
    Nothing,
    /// The folder, up to and with the name.
    Part(String, String),
    /// Up to and with the folder.
    Folder(String),
    All,
}

impl Replacement {
    /// `node`'s files as it listed them at `listed_at`, which we requested at `requested`
    /// (a `mark`).
    pub fn new(
        node: &NodeId,
        mut files: Vec<(RelPath, Stat)>,
        listed_at: f64,
        requested: u64,
    ) -> Self {
        files.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        Self {
            node: node.clone(),
            listed_at,
            requested,
            files,
            added: 0,
            swept: Swept::Nothing,
            begun: false,
        }
    }

    fn lists(&self, dir: &str, name: &str) -> bool {
        self.files
            .binary_search_by(|(path, _)| cmp_joined(path.as_str(), dir, name))
            .is_ok()
    }
}

/// What comes after `name`, of names in order.
fn after(name: &str) -> (Bound<&str>, Bound<&str>) {
    (Bound::Excluded(name), Bound::Unbounded)
}

/// `path` against the child `name` of `dir`, as `join` spells it, without spelling it.
fn cmp_joined(path: &str, dir: &str, name: &str) -> Ordering {
    let slash = (!dir.is_empty()).then_some(b'/');
    path.bytes()
        .cmp(dir.bytes().chain(slash).chain(name.bytes()))
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            dirs: BTreeMap::from([(String::new(), Dir::default())]),
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
            .entry(node.clone())
            .or_default()
            .insert(path.clone(), self.clock);
        self.remove(node, path);
    }

    pub fn add(&mut self, node: &NodeId, path: &RelPath, stat: Stat) {
        if let Some(deleted) = self.deleted.get_mut(node) {
            deleted.remove(path);
        }
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
        self.deleted.remove(node);
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
        let mut replacement = Replacement::new(node, files, listed_at, requested);
        while self.replace_part(&mut replacement, usize::MAX) {}
    }

    /// The next part of what `replace_node` does, of at most `most` files looked at;
    /// false once nothing is left. Between the parts the node goes on announcing, and
    /// the index is anyone's: what the node announces then stands, as what it announced
    /// since the listing was requested does.
    pub fn replace_part(&mut self, replacement: &mut Replacement, most: usize) -> bool {
        let r = replacement;
        if !r.begun {
            // The listing has every deletion announced before it was requested.
            if let Some(deleted) = self.deleted.get_mut(&r.node) {
                deleted.retain(|_, at| *at > r.requested);
            }
            r.begun = true;
        }
        let mut left = most;
        while left > 0 && self.sweep(r, &mut left) {}
        while left > 0 && r.added < r.files.len() {
            let (path, stat) = &r.files[r.added];
            r.added += 1;
            left -= 1;
            let deleted_since = self
                .deleted
                .get(&r.node)
                .and_then(|deleted| deleted.get(path))
                .is_some_and(|&at| at > r.requested);
            if !deleted_since {
                self.add_listed(&r.node, path, *stat, r.listed_at);
            }
        }
        !(matches!(r.swept, Swept::All) && r.added == r.files.len())
    }

    /// Remove the files `r`'s node no longer has, of the next folder or of what is left
    /// of the one begun, looking at no more than `left` files; false once every folder
    /// is swept. A file written since the listing was taken stays.
    fn sweep(&mut self, r: &mut Replacement, left: &mut usize) -> bool {
        let (dir, from) = match &r.swept {
            Swept::All => return false,
            Swept::Part(dir, name) if self.dirs.contains_key(dir.as_str()) => {
                (Some(dir.clone()), Some(name.clone()))
            }
            Swept::Part(dir, _) | Swept::Folder(dir) => {
                let next = self.dirs.range::<str, _>(after(dir)).next();
                (next.map(|(dir, _)| dir.clone()), None)
            }
            Swept::Nothing => (self.dirs.keys().next().cloned(), None),
        };
        let Some(dir) = dir else {
            r.swept = Swept::All;
            return false;
        };
        let files = &self.dirs[dir.as_str()].files;
        let mut names = match &from {
            Some(name) => files.range::<str, _>(after(name)),
            None => files.range::<str, _>(..),
        };
        let mut stale = Vec::new();
        // A folder with no file to look at is looked at all the same.
        *left -= 1;
        r.swept = loop {
            let Some((name, sources)) = names.next() else {
                break Swept::Folder(dir.clone());
            };
            if sources
                .get(&r.node)
                .is_some_and(|stat| stat.mtime < r.listed_at)
                && !r.lists(&dir, name)
            {
                stale.push(name.clone());
            }
            if *left == 0 {
                break Swept::Part(dir.clone(), name.clone());
            }
            *left -= 1;
        };
        for name in stale {
            if let Some(path) = RelPath::parse(&join(&dir, &name)) {
                self.remove(&r.node, &path);
            }
        }
        true
    }

    /// A file of a listing taken at `listed_at`: unless the node announced it since.
    fn add_listed(&mut self, node: &NodeId, path: &RelPath, stat: Stat, listed_at: f64) {
        let (dir, name) = path.split();
        let known = self
            .dirs
            .get(dir)
            .and_then(|d| d.files.get(name))
            .and_then(|sources| sources.get(node));
        if known.is_some_and(|known| *known == stat || known.mtime >= listed_at) {
            return;
        }
        self.ensure_dir(dir);
        self.dir_mut(dir)
            .files
            .entry(name.to_owned())
            .or_default()
            .insert(node.clone(), stat);
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
