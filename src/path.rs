//! Paths inside an exported directory.

use std::fmt;

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

#[cfg(test)]
#[path = "path_test.rs"]
mod tests;
