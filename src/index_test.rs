use super::*;

fn node(name: &str) -> NodeId {
    NodeId::parse(name).unwrap()
}

fn path(p: &str) -> RelPath {
    RelPath::parse(p).unwrap()
}

fn stat(size: u64) -> Stat {
    Stat { size, mtime: 1.0 }
}

fn names(index: &Index, dir: &str) -> Vec<String> {
    index
        .list(dir)
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect()
}

#[test]
fn files_of_every_node_make_one_union() {
    let mut ix = Index::new();
    ix.add(&node("a"), &path("x.bin"), stat(4));
    ix.add(&node("b"), &path("x.bin"), stat(4));
    ix.add(&node("b"), &path("d/e/y.bin"), stat(1));
    assert_eq!(ix.lookup(""), Some(Entry::Dir));
    assert_eq!(ix.lookup("d"), Some(Entry::Dir));
    let Some(Entry::File(file)) = ix.lookup("x.bin") else {
        panic!("a file")
    };
    assert_eq!((file.path, file.size), (path("x.bin"), 4));
    assert_eq!(file.nodes, vec![node("a"), node("b")]);
    assert_eq!(names(&ix, ""), vec!["d".to_owned(), "x.bin".to_owned()]);
    ix.remove(&node("b"), &path("d/e/y.bin"));
    assert_eq!(ix.lookup("d"), None); // emptied, so gone
    ix.remove_node(&node("a"));
    assert!(ix.lookup("x.bin").is_some()); // b still has it
    ix.remove_node(&node("b"));
    assert_eq!(ix.list(""), Some(vec![]));
}

#[test]
fn a_listing_replaces_what_a_node_had_but_not_what_it_announced_since() {
    let mut ix = Index::new();
    ix.add(&node("a"), &path("old.bin"), stat(1));
    ix.add(&node("a"), &path("kept.bin"), stat(1));
    let new = Stat {
        size: 1,
        mtime: 20.0,
    };
    ix.add(&node("a"), &path("new.bin"), new);
    ix.replace_node(
        &node("a"),
        vec![(path("kept.bin"), stat(1))],
        10.0,
        ix.mark(),
    );
    assert_eq!(names(&ix, ""), ["kept.bin", "new.bin"].map(str::to_owned));
}

#[test]
fn a_listing_does_not_bring_back_a_file_deleted_since_it_was_requested() {
    let mut ix = Index::new();
    ix.add(&node("a"), &path("x.bin"), stat(1));
    let requested = ix.mark();
    ix.delete(&node("a"), &path("x.bin")); // announced while the listing was on its way
    let listing = vec![(path("x.bin"), stat(1)), (path("y.bin"), stat(1))];
    ix.replace_node(&node("a"), listing.clone(), 10.0, requested);
    assert_eq!(names(&ix, ""), vec!["y.bin".to_owned()]);
    // A listing requested after the deletion has it, if the file is back.
    ix.replace_node(&node("a"), listing, 10.0, ix.mark());
    assert_eq!(names(&ix, ""), ["x.bin", "y.bin"].map(str::to_owned));
}
