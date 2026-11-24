use std::time::UNIX_EPOCH;

use super::*;
use crate::held::Held;
use crate::state::testing::{state, velo};

/// When the files of these tests were last read and written, before they are marked.
pub(crate) const LONG_AGO: SystemTime = SystemTime::UNIX_EPOCH;

/// Make the file at `path` last read and written `LONG_AGO`.
pub(crate) fn age(path: &Path) {
    let times = FileTimes::new()
        .set_accessed(LONG_AGO)
        .set_modified(LONG_AGO);
    let file = File::options().write(true).open(path).unwrap();
    file.set_times(times).unwrap();
}

/// When the file at `path` was last read and last written.
pub(crate) fn times(path: &Path) -> (SystemTime, SystemTime) {
    let meta = std::fs::metadata(path).unwrap();
    (meta.accessed().unwrap(), meta.modified().unwrap())
}

/// Whether the file at `path` is marked as used since it was aged: read since, and
/// written no later than before.
pub(crate) fn is_marked(path: &Path) -> bool {
    let (read, written) = times(path);
    assert_eq!(
        written, LONG_AGO,
        "marking a file leaves when it was written"
    );
    read > UNIX_EPOCH + Duration::from_secs(1_000_000_000)
}

fn key(node: &NodeId, path: &str) -> Key {
    Key {
        node: node.clone(),
        path: RelPath::parse(path).unwrap(),
    }
}

#[test]
fn marking_a_file_renews_when_it_was_read_and_not_when_it_was_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.bin");
    std::fs::write(&path, b"1234").unwrap();
    age(&path);
    assert!(!is_marked(&path));
    // Opened to read only, as a file of an export is.
    mark(&File::open(&path).unwrap()).unwrap();
    assert!(is_marked(&path));
    age(&path);
    let rel = |path: &str| RelPath::parse(path).unwrap();
    assert!(mark_export_file(dir.path(), &rel("x.bin")).unwrap());
    assert!(is_marked(&path));
    assert!(!mark_export_file(dir.path(), &rel("gone.bin")).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_is_told_of_its_files_opened_from_memory_here_and_marks_them() {
    let export = tempfile::tempdir().unwrap();
    let deep = crate::path::tests::deep_file(export.path(), 150, b"deep");
    for name in ["x.bin", "y.bin"] {
        std::fs::write(export.path().join(name), b"1234").unwrap();
        age(&export.path().join(name));
    }
    let (ours, theirs) = (velo().await, velo().await);
    let stages = Arc::new(transfer::Stages::new(1 << 20));
    let held = Arc::new(Held::new(8));
    transfer::serve(&theirs, export.path().to_owned(), false, stages, held).unwrap();
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let (b, c) = (NodeId::parse("b").unwrap(), NodeId::parse("c").unwrap());
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    state.used.note(&key(&b, "x.bin"));
    state.used.note(&key(&b, "x.bin")); // opened twice: told once
    state.used.note(&key(&b, "gone.bin")); // deleted there meanwhile
    state.used.note(&key(&b, deep.as_str())); // past what a path takes
    state.used.note(&key(&c, "x.bin")); // its node went: nobody to tell
    tell_once(&state).await;
    assert!(is_marked(&export.path().join("x.bin")));
    assert!(!is_marked(&export.path().join("y.bin")));
    // Told: the next round has nothing to tell.
    assert!(state.used.take().is_empty());
    assert!(state.used.warns(&b), "nothing failed, so nothing was said");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_cannot_be_told_is_said_so_now_and_then() {
    // An rview-fs from before it took such notes: up, with no handler for them.
    let (ours, theirs) = (velo().await, velo().await);
    ours.register_peer(theirs.peer_info()).unwrap();
    let state = state(ours);
    let b = NodeId::parse("b").unwrap();
    state.nodes.write().heard(b.clone(), theirs.instance_id());
    state.used.note(&key(&b, "x.bin"));
    tell_once(&state).await;
    assert!(!state.used.warns(&b), "said just now");
    let long_ago = Instant::now().checked_sub(WARN_EVERY).unwrap();
    state.used.warned.lock().insert(b.clone(), long_ago);
    assert!(state.used.warns(&b));
}
