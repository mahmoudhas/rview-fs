use super::*;

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
        .into_iter()
        .map(|f| (f.path, f.size))
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
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_modified(old)
        .unwrap();
    let (_, size) = open_export_file(&path).unwrap().unwrap();
    assert_eq!(size, 4);
    assert!(std::fs::metadata(&path).unwrap().modified().unwrap() > old);
    assert!(
        open_export_file(&dir.path().join("gone.bin"))
            .unwrap()
            .is_none()
    );
    assert!(open_export_file(dir.path()).is_err()); // a folder
}
