//! Paths inside an exported directory, and opening them.

use std::fmt;
use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;

use rustix::fs::{Mode, OFlags};

/// How a folder is opened: to look in, not to read.
pub const FOLDER: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::CLOEXEC);

/// A path inside an export: relative, with no empty or hidden components. Hidden names
/// (`.` and `..` among them) are for files that are still being written.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelPath(String);

impl RelPath {
    pub fn parse(path: &str) -> Option<Self> {
        let valid = |c: &str| !c.is_empty() && !c.starts_with('.') && !c.contains('\0');
        path.split('/').all(valid).then(|| Self(path.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The folder (empty at the root) and the name.
    pub fn split(&self) -> (&str, &str) {
        self.0.rsplit_once('/').unwrap_or(("", &self.0))
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The child `name` of the folder `dir` (empty at the root).
pub fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// The folder `dir` (empty: the export itself) of the export at `root`, open. A path in an
/// export can be longer than any the kernel takes (PATH_MAX, 4096 bytes): a token tree puts
/// each page's folder in the previous page's. So the folders are opened one name at a time,
/// each in the one before, and no call is given the path whole. Blocks.
pub fn open_dir(root: &Path, dir: &str) -> io::Result<OwnedFd> {
    let mut fd = rustix::fs::open(root, FOLDER, Mode::empty())?;
    for name in dir.split('/').filter(|name| !name.is_empty()) {
        fd = rustix::fs::openat(&fd, name, FOLDER, Mode::empty())?;
    }
    Ok(fd)
}

/// The file at `path` in the export at `root`, open to read, reached as `open_dir` reaches
/// its folder. Blocks.
pub fn open_file(root: &Path, path: &RelPath) -> io::Result<File> {
    let (dir, name) = path.split();
    let dir = open_dir(root, dir)?;
    let flags = OFlags::RDONLY | OFlags::CLOEXEC;
    Ok(rustix::fs::openat(&dir, name, flags, Mode::empty())?.into())
}

#[cfg(test)]
#[path = "path_test.rs"]
pub(crate) mod tests;
