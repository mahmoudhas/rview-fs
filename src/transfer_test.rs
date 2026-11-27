use super::*;
use crate::used::tests::{age, is_marked};

#[test]
fn a_scan_lists_only_the_visible_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("a/b")).unwrap();
    std::fs::write(root.join("a/x.bin"), b"1234").unwrap();
    std::fs::write(root.join("a/b/t.bin"), b"56").unwrap();
    std::fs::write(root.join("a/.x.bin.k3j.tmp"), b"half").unwrap(); // being written
    std::fs::create_dir_all(root.join(".hidden")).unwrap();
    std::fs::write(root.join(".hidden/y.bin"), b"y").unwrap();
    let mut listed: Vec<(String, u64)> = scan(root)
        .unwrap()
        .files
        .iter()
        .map(|(path, stat)| (path.to_owned(), stat.size))
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        vec![("a/b/t.bin".to_owned(), 2), ("a/x.bin".to_owned(), 4)]
    );
}

#[test]
fn staging_stays_within_its_budget() {
    let store = velo::rendezvous::store::DataStore::new();
    let stage = |size: usize| {
        let id = store.register(Bytes::from(vec![0u8; size]), None);
        DataHandle::pack(velo::WorkerId::from_u64(1), id)
    };
    let stages = Stages::new(10);
    assert!(stages.reserve(6, &store));
    let first = stage(6);
    stages.add(first, 6);
    assert!(!stages.reserve(6, &store)); // 12 bytes would be over 10
    // A reader that took it, and let go, leaves nothing staged.
    store.remove(first.unpack().1);
    assert!(stages.reserve(6, &store));
    assert_eq!(stages.bytes(), 6);
    // A file bigger than the whole budget is staged alone.
    let stages = Stages::new(10);
    assert!(stages.reserve(20, &store) && !stages.reserve(1, &store));
}

#[test]
fn opening_a_file_for_another_node_marks_it_used() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("x.bin");
    std::fs::write(&path, b"1234").unwrap();
    age(&path);
    let rel = |path: &str| RelPath::parse(path).unwrap();
    let (_, size) = open_export_file(dir.path(), &rel("x.bin"))
        .unwrap()
        .unwrap();
    assert_eq!(size, 4);
    assert!(is_marked(&path));
    assert!(
        open_export_file(dir.path(), &rel("gone.bin"))
            .unwrap()
            .is_none()
    );
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    assert!(open_export_file(dir.path(), &rel("sub")).is_err()); // a folder
}

#[test]
fn the_files_another_node_read_from_its_memory_are_marked_used() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["x.bin", "y.bin", "z.bin"] {
        std::fs::write(dir.path().join(name), b"1234").unwrap();
        age(&dir.path().join(name));
    }
    // One gone since, and what is no path in an export, are passed over.
    let read = ["x.bin", "gone.bin", "z.bin", "../z.bin", ".hidden.tmp"].map(str::to_owned);
    assert_eq!(mark_export_files(dir.path(), &read), 2);
    assert!(is_marked(&dir.path().join("x.bin")) && is_marked(&dir.path().join("z.bin")));
    assert!(!is_marked(&dir.path().join("y.bin")));
}

#[test]
fn files_deeper_than_a_path_takes_are_listed_and_opened() {
    let dir = tempfile::tempdir().unwrap();
    let path = crate::path::tests::deep_file(dir.path(), 150, b"deep");
    let listed: Vec<(String, u64)> = scan(dir.path())
        .unwrap()
        .files
        .iter()
        .map(|(path, stat)| (path.to_owned(), stat.size))
        .collect();
    assert_eq!(listed, vec![(path.as_str().to_owned(), 4)]);
    let (_, size) = open_export_file(dir.path(), &path).unwrap().unwrap();
    assert_eq!(size, 4);
}

#[test]
fn a_listing_is_sent_and_read_as_one_with_a_path_allocated_for_each_file_was() {
    let mut files = Files::default();
    let stat = |size, mtime| Stat { size, mtime };
    files.push("a/b", "t.bin", stat(2, 1.5));
    files.push("", "top.bin", stat(4, 2.0));
    let listing = Listing {
        files,
        listed_at: 10.0,
    };
    assert_eq!(
        serde_json::to_string(&listing).unwrap(),
        r#"{"files":[{"path":"a/b/t.bin","size":2,"mtime":1.5},{"path":"top.bin","size":4,"mtime":2.0}],"listed_at":10.0}"#
    );
    // A name that is escaped on the way, two paths that are none of an export, and a
    // field a later version might add.
    let sent = r#"{"files":[{"path":"a/q\"x.bin","size":1,"mtime":1.0},{"path":".tmp/x","size":1,"mtime":1.0},{"path":"a//b","size":1,"mtime":1.0},{"path":"d/e.bin","size":3,"mtime":2.5,"more":true}],"listed_at":7.0}"#;
    let got: Listing = serde_json::from_slice(sent.as_bytes()).unwrap();
    let files: Vec<(&str, Stat)> = got.files.iter().collect();
    assert_eq!(
        files,
        [("a/q\"x.bin", stat(1, 1.0)), ("d/e.bin", stat(3, 2.5))]
    );
    assert_eq!(got.listed_at, 7.0);
}
