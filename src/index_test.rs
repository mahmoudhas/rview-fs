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

/// A file of the union: its path, and the nodes that hold it with the size and time each
/// says.
type Held = (String, Vec<(NodeId, u64, f64)>);

fn files(index: &Index) -> Vec<Held> {
    let mut found = Vec::new();
    let mut folders = vec![String::new()];
    while let Some(dir) = folders.pop() {
        for (name, kind) in index.list(&dir).unwrap() {
            let child = join(&dir, &name);
            match kind {
                EntryKind::Dir => folders.push(child),
                EntryKind::File => {
                    let sources = &index.dirs[dir.as_str()].files[&name];
                    let nodes = sources.iter().map(|(n, s)| (n.clone(), s.size, s.mtime));
                    found.push((child, nodes.collect()));
                }
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

/// Two nodes' files over a few folders, one of them big, and a listing of `a`'s that
/// drops some, keeps some, changes one and brings new ones.
fn index_and_listing() -> (Index, Vec<(RelPath, Stat)>) {
    let mut ix = Index::new();
    let mut listing = Vec::new();
    for i in 0..40 {
        let p = path(&format!("big/f{i:02}.bin"));
        ix.add(&node("a"), &p, stat(i));
        if i % 3 == 0 {
            ix.add(&node("b"), &p, stat(i));
        }
        if i % 4 != 0 {
            listing.push((p, stat(i))); // every fourth is gone
        }
    }
    for dir in ["d1", "d1/e", "d2", "only-b"] {
        for i in 0..3 {
            let p = path(&format!("{dir}/g{i}.bin"));
            let owner = if dir == "only-b" { "b" } else { "a" };
            ix.add(&node(owner), &p, stat(i));
        }
    }
    listing.push((path("d1/g0.bin"), stat(0)));
    listing.push((path("d1/e/g1.bin"), stat(7))); // another size than we knew
    listing.push((path("new/h.bin"), stat(1)));
    listing.push((path("top.bin"), stat(1)));
    // Written after the listing was taken, and announced: it stays though unlisted.
    let since = Stat {
        size: 1,
        mtime: 20.0,
    };
    ix.add(&node("a"), &path("d2/since.bin"), since);
    (ix, listing)
}

#[test]
fn a_listing_put_in_a_part_at_a_time_ends_as_one_put_in_whole() {
    let (mut whole, listing) = index_and_listing();
    whole.replace_node(&node("a"), listing.clone(), 10.0, whole.mark());
    let kept = files(&whole);
    assert!(kept.iter().any(|(p, _)| p == "d2/since.bin"));
    assert!(kept.iter().any(|(p, _)| p == "only-b/g0.bin"));
    assert!(!kept.iter().any(|(p, _)| p == "d2/g0.bin")); // unlisted, and older
    for most in [1, 2, 7, 1000] {
        let (mut ix, listing) = index_and_listing();
        let mut replacement = Replacement::new(&node("a"), listing, 10.0, ix.mark());
        let mut parts = 0;
        while ix.replace_part(&mut replacement, most) {
            parts += 1;
            assert!(parts < 10_000, "it never ends");
        }
        assert_eq!(files(&ix), kept, "in parts of {most}");
        // A folder of 40 files is not swept in one part of fewer.
        assert!(most >= 40 || parts > 40 / most);
    }
}

#[test]
fn what_a_node_announces_between_the_parts_of_its_listing_stands() {
    let (mut ix, listing) = index_and_listing();
    let a = node("a");
    let mut replacement = Replacement::new(&a, listing, 10.0, ix.mark());
    // After every part: the node deletes a listed file not in yet, one in already and an
    // unlisted one, writes a new file, and writes one of the listing's again.
    let newer = Stat {
        size: 9,
        mtime: 30.0,
    };
    let mut announced = false;
    while ix.replace_part(&mut replacement, 5) {
        if !announced {
            ix.delete(&a, &path("top.bin")); // the last of the listing: not in yet
            ix.delete(&a, &path("big/f01.bin"));
            ix.delete(&a, &path("d2/since.bin"));
            ix.add(&a, &path("d2/after.bin"), newer);
            ix.add(&a, &path("new/h.bin"), newer);
            announced = true;
        }
    }
    let found = files(&ix);
    let of = |p: &str| {
        found
            .iter()
            .find(|(path, _)| path == p)
            .map(|(_, n)| n.clone())
    };
    assert_eq!(of("top.bin"), None);
    assert_eq!(of("big/f01.bin"), None);
    assert_eq!(of("d2/since.bin"), None);
    assert_eq!(of("d2/after.bin"), Some(vec![(a.clone(), 9, 30.0)]));
    assert_eq!(of("new/h.bin"), Some(vec![(a.clone(), 9, 30.0)])); // not the listing's
    assert_eq!(of("big/f02.bin"), Some(vec![(a.clone(), 2, 1.0)]));
    assert_eq!(of("big/f04.bin"), None); // the listing did not have it
    assert_eq!(of("d1/e/g1.bin"), Some(vec![(a, 7, 1.0)]));
}
