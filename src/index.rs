//! The union of every node's exported files: which nodes hold each file.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use crate::path::{self, RelPath, join};

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

/// The files of a listing, their paths in one piece of memory. A path allocated for each
/// file, 400,000 of 1 kB, is freed with the listing into the allocator's heaps, which
/// keep it: a process grew by 1.5 GiB at every listing of three such machines.
#[derive(Debug, Default)]
pub struct Files {
    /// Every path, one after the other.
    paths: String,
    files: Vec<Place>,
}

/// The least a listing asks the allocator for at a time, for its paths, for its files and
/// for its text on the way to another machine: more than the allocator ever gives from
/// the heaps it keeps (32 MiB), so that each is memory of its own, which goes back to the
/// kernel when it is freed. Until it is written to it takes no memory.
pub const PIECE: usize = 64 << 20;

/// Where a file's path begins in `Files::paths`, and how long its folder (nothing at the
/// root) and its name are.
#[derive(Clone, Copy, Debug)]
struct Place {
    at: usize,
    dir: u32,
    name: u32,
    stat: Stat,
}

/// A file of `Files`: its path, which is its folder (empty at the root) and its name.
#[derive(Clone, Copy)]
struct Listed<'a> {
    path: &'a str,
    dir: &'a str,
    name: &'a str,
    stat: Stat,
}

impl Files {
    /// Add the file `name` of the folder `dir`, which is empty at the root.
    pub fn push(&mut self, dir: &str, name: &str, stat: Stat) {
        let at = self.paths.len();
        if self.paths.capacity() - at < dir.len() + 1 + name.len() {
            self.paths.reserve(PIECE);
        }
        if self.files.capacity() == self.files.len() {
            self.files.reserve(PIECE / size_of::<Place>());
        }
        if !dir.is_empty() {
            self.paths.push_str(dir);
            self.paths.push('/');
        }
        self.paths.push_str(name);
        self.files.push(Place {
            at,
            dir: u32::try_from(dir.len()).expect("a path is shorter than 4 GiB"),
            name: u32::try_from(name.len()).expect("a path is shorter than 4 GiB"),
            stat,
        });
    }

    /// Add the file at `path`, unless that is no path of an export (`RelPath` says).
    pub fn push_path(&mut self, path: &str, stat: Stat) -> bool {
        let valid = path::is_valid(path);
        if valid {
            let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
            self.push(dir, name, stat);
        }
        valid
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Each file's path and stat, in the order they are in.
    pub fn iter(&self) -> impl Iterator<Item = (&str, Stat)> {
        let listed = self.files.iter().map(|place| place.listed(&self.paths));
        listed.map(|file| (file.path, file.stat))
    }

    fn get(&self, i: usize) -> Option<Listed<'_>> {
        Some(self.files.get(i)?.listed(&self.paths))
    }

    /// Into the index's order: by folder, then by name.
    fn sort(&mut self) {
        let key = |place: &Place| {
            let file = place.listed(&self.paths);
            (file.dir, file.name)
        };
        self.files.sort_unstable_by(|a, b| key(a).cmp(&key(b)));
    }
}

impl Place {
    fn listed<'a>(&self, paths: &'a str) -> Listed<'a> {
        let (dir, name) = (self.dir as usize, self.name as usize);
        let named = self.at + dir + usize::from(dir > 0);
        Listed {
            path: &paths[self.at..named + name],
            dir: &paths[self.at..self.at + dir],
            name: &paths[named..named + name],
            stat: self.stat,
        }
    }
}

impl FromIterator<(RelPath, Stat)> for Files {
    fn from_iter<I: IntoIterator<Item = (RelPath, Stat)>>(files: I) -> Self {
        let mut all = Self::default();
        for (path, stat) in files {
            let (dir, name) = path.split();
            all.push(dir, name, stat);
        }
        all
    }
}

impl From<Vec<(RelPath, Stat)>> for Files {
    fn from(files: Vec<(RelPath, Stat)>) -> Self {
        files.into_iter().collect()
    }
}

/// A listing of a node's files on its way into the index, a part at a time
/// (`Index::replace_part`): whole, the index is held for all of it, which every lookup
/// then waits for (0.6 s for a node of 170,000 files).
pub struct Replacement {
    node: NodeId,
    listed_at: f64,
    requested: u64,
    /// The listing, in the index's order: by folder, then by name.
    files: Files,
    /// How many of `files` are in.
    added: usize,
    /// How many of `files` come before where the sweep stands.
    passed: usize,
    swept: Swept,
    begun: bool,
    /// The deletions the listing has, taken out of the index as it began: freed with the
    /// replacement, since freeing their paths takes too long to hold the index for.
    forgotten: Vec<(RelPath, u64)>,
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
    /// (a `mark`). Sorts them: not with the index held.
    pub fn new(node: &NodeId, mut files: Files, listed_at: f64, requested: u64) -> Self {
        files.sort();
        Self {
            node: node.clone(),
            listed_at,
            requested,
            files,
            added: 0,
            passed: 0,
            swept: Swept::Nothing,
            begun: false,
            forgotten: Vec::new(),
        }
    }

    /// How many files the listing has.
    pub fn count(&self) -> usize {
        self.files.len()
    }

    /// Whether the listing has the file `name` of the folder `dir`. For the sweep, which
    /// asks in the index's order: it reads on in the listing from where it last stood,
    /// and never searches it.
    fn lists(&mut self, dir: &str, name: &str) -> bool {
        while let Some(listed) = self.files.get(self.passed) {
            match (listed.dir, listed.name).cmp(&(dir, name)) {
                Ordering::Less => self.passed += 1,
                Ordering::Equal => return true,
                Ordering::Greater => return false,
            }
        }
        false
    }
}

/// The folders `add_listed` reads on through for the next file's before it searches for
/// it instead: reading one on costs about a tenth of a search.
const FOLDERS_READ_ON: usize = 8;

/// What comes after `name`, of names in order.
fn after(name: &str) -> (Bound<&str>, Bound<&str>) {
    (Bound::Excluded(name), Bound::Unbounded)
}

/// `name` and what comes after it, of names in order.
fn from(name: &str) -> (Bound<&str>, Bound<&str>) {
    (Bound::Included(name), Bound::Unbounded)
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
        self.insert(node, dir, name, stat);
    }

    fn insert(&mut self, node: &NodeId, dir: &str, name: &str, stat: Stat) {
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
    pub fn replace_node(&mut self, node: &NodeId, files: Files, listed_at: f64, requested: u64) {
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
                r.forgotten.reserve_exact(deleted.len());
                r.forgotten
                    .extend(deleted.extract_if(|_, at| *at <= r.requested));
            }
            r.begun = true;
        }
        let mut left = most;
        while left > 0 && self.sweep(r, &mut left) {}
        self.add_listed(r, &mut left);
        !(matches!(r.swept, Swept::All) && r.added == r.files.len())
    }

    /// Remove the files `r`'s node no longer has, from where the sweep stands on, looking
    /// at no more than `left` files and folders; false once every folder is swept. A file
    /// written since the listing was taken stays.
    fn sweep(&mut self, r: &mut Replacement, left: &mut usize) -> bool {
        let stood = std::mem::replace(&mut r.swept, Swept::All);
        let from = match &stood {
            Swept::All => return false,
            Swept::Nothing => Bound::Unbounded,
            // A folder gone since is none of the range's, which begins after it.
            Swept::Part(dir, _) => Bound::Included(dir.as_str()),
            Swept::Folder(dir) => Bound::Excluded(dir.as_str()),
        };
        let mut stale = Vec::new();
        // The folder reached, and the name in it when it is not done.
        let mut reached: Option<(&str, Option<&str>)> = None;
        let mut all = true;
        for (dir, folder) in self.dirs.range::<str, _>((from, Bound::Unbounded)) {
            if *left == 0 {
                all = false;
                break;
            }
            // A folder with no file to look at is looked at all the same.
            *left -= 1;
            let names = match &stood {
                Swept::Part(part, name) if part == dir => folder.files.range::<str, _>(after(name)),
                _ => folder.files.range::<str, _>(..),
            };
            let mut stopped = None;
            for (name, sources) in names {
                if sources
                    .get(&r.node)
                    .is_some_and(|stat| stat.mtime < r.listed_at)
                    && !r.lists(dir, name)
                {
                    stale.push(join(dir, name));
                }
                if *left == 0 {
                    stopped = Some(name.as_str());
                    break;
                }
                *left -= 1;
            }
            reached = Some((dir, stopped));
            if stopped.is_some() {
                all = false;
                break;
            }
        }
        r.swept = match reached {
            _ if all => Swept::All,
            Some((dir, Some(name))) => Swept::Part(dir.to_owned(), name.to_owned()),
            Some((dir, None)) => Swept::Folder(dir.to_owned()),
            None => stood,
        };
        for path in stale.iter().filter_map(|path| RelPath::parse(path)) {
            self.remove(&r.node, &path);
        }
        !all
    }

    /// Put the next files of `r`'s listing in, no more than `left` of them, once the sweep
    /// is done: those the index does not have as listed, unless the node announced them
    /// since. The listing and the folders are both in order, so the folders are read on
    /// beside the listing, and one is searched for only when it is far on.
    fn add_listed(&mut self, r: &mut Replacement, left: &mut usize) {
        if !matches!(r.swept, Swept::All) {
            return;
        }
        let Some(first) = r.files.get(r.added) else {
            return;
        };
        let mut unknown = Vec::new();
        let mut sought = first.dir;
        let mut folders = self.dirs.range::<str, _>(from(sought)).peekable();
        while *left > 0 {
            let Some(listed) = r.files.get(r.added) else {
                break;
            };
            let (dir, name) = (listed.dir, listed.name);
            if sought != dir {
                let mut read = 0;
                while folders
                    .next_if(|(folder, _)| folder.as_str() < dir)
                    .is_some()
                {
                    read += 1;
                    if read == FOLDERS_READ_ON {
                        folders = self.dirs.range::<str, _>(from(dir)).peekable();
                    }
                }
                sought = dir;
            }
            let known = folders
                .peek()
                .filter(|(folder, _)| folder.as_str() == dir)
                .and_then(|(_, folder)| folder.files.get(name))
                .and_then(|sources| sources.get(&r.node));
            if !known.is_some_and(|known| *known == listed.stat || known.mtime >= r.listed_at) {
                unknown.push(listed);
            }
            r.added += 1;
            *left -= 1;
        }
        for listed in unknown {
            let deleted_since = self
                .deleted
                .get(&r.node)
                .and_then(|deleted| deleted.get(listed.path))
                .is_some_and(|&at| at > r.requested);
            if !deleted_since {
                self.insert(&r.node, listed.dir, listed.name, listed.stat);
            }
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
