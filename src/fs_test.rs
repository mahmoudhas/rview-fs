use super::*;
use crate::index::Stat;
use crate::path::RelPath;
use crate::state::testing::{state, velo};

const REQUEST: Request = Request {
    unique: 0,
    uid: 0,
    gid: 0,
    pid: 0,
};
const ROOT: InodeId = InodeId(FUSE_ROOT_ID);

#[test]
fn an_open_is_of_the_kind_of_the_opens_of_its_inode() {
    let mut kinds = Kinds::default();
    assert_eq!(kinds.begin(7, Kind::Served), Kind::Served);
    // The file came to this node meanwhile: passed through, the kernel would fail it.
    assert_eq!(kinds.begin(7, Kind::PassedThrough), Kind::Served);
    assert_eq!(kinds.begin(8, Kind::PassedThrough), Kind::PassedThrough);
    kinds.end(7);
    assert_eq!(kinds.begin(7, Kind::PassedThrough), Kind::Served);
    kinds.end(7);
    kinds.end(7);
    // Once the last is closed, the next open is of its own kind.
    assert_eq!(kinds.begin(7, Kind::PassedThrough), Kind::PassedThrough);
}

#[test]
fn an_open_that_cannot_be_passed_through_is_served_only_alone() {
    let mut kinds = Kinds::default();
    assert_eq!(kinds.begin(7, Kind::PassedThrough), Kind::PassedThrough);
    assert!(kinds.serve_alone(7));
    assert_eq!(kinds.begin(7, Kind::PassedThrough), Kind::Served);
    assert!(!kinds.serve_alone(7));
    kinds.end(7);
    kinds.end(7);
    assert!(!kinds.serve_alone(7));
    assert!(kinds.0.is_empty());
}

/// The union of this node's `files`, mounted nowhere: its requests are called.
async fn union_of(files: &[String]) -> (Arc<State>, RviewFs) {
    let state = state(velo().await);
    for file in files {
        add(&state, file);
    }
    let fs = RviewFs::new(Arc::clone(&state), tokio::runtime::Handle::current(), None);
    (state, fs)
}

fn add(state: &State, file: &str) {
    let stat = Stat {
        size: 4,
        mtime: 1.0,
    };
    let path = RelPath::parse(file).unwrap();
    state.index.write().add(&state.me, &path, stat);
}

fn remove(state: &State, file: &str) {
    let path = RelPath::parse(file).unwrap();
    state.index.write().remove(&state.me, &path);
}

fn numbered(prefix: &str, range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("{prefix}{i:03}.bin")).collect()
}

/// How the kernel reads a folder: its names, or each with what a lookup of it says.
#[derive(Clone, Copy, Debug)]
enum Read {
    Names,
    Plus,
}

async fn open_root(fs: &RviewFs) -> FileHandleId {
    fs.opendir(REQUEST, ROOT, 0).await.unwrap().fh
}

/// The names one call returns, each with its offset.
async fn call(fs: &RviewFs, fh: FileHandleId, read: Read, offset: u64) -> Vec<(String, u64)> {
    let name = |bytes| String::from_utf8(bytes).unwrap();
    // The kernel reads a page a call (4096 bytes); less here, for many calls a folder.
    match read {
        Read::Names => fs
            .readdir(REQUEST, ROOT, fh, offset, 256)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (name(e.name), e.offset))
            .collect(),
        Read::Plus => fs
            .readdirplus(REQUEST, ROOT, fh, offset, 1024)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (name(e.name), e.offset))
            .collect(),
    }
}

/// The names a reading of the root through `fh` returns from its start, after "." and
/// "..": call after call, each from the offset the one before reached, until one
/// returns nothing, with `between` run after the call of each number.
async fn read_root(
    fs: &RviewFs,
    fh: FileHandleId,
    read: Read,
    mut between: impl FnMut(usize),
) -> Vec<String> {
    let (mut names, mut offset) = (Vec::new(), 0);
    for calls in 0.. {
        let page = call(fs, fh, read, offset).await;
        let Some((_, last)) = page.last() else {
            break;
        };
        offset = *last;
        names.extend(page.into_iter().map(|(name, _)| name));
        between(calls);
    }
    assert_eq!(names[..2], [".", ".."]);
    names.split_off(2)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_folder_read_while_files_come_and_go_returns_each_file_there_all_along_once() {
    for read in [Read::Names, Read::Plus] {
        let kept = numbered("m", 0..300);
        let (state, fs) = union_of(&kept).await;
        for file in numbered("a", 0..40) {
            add(&state, &file);
        }
        let fh = open_root(&fs).await;
        // Files whose names sort before those the reading has reached come, which moves
        // every later file one place on, and go, which moves them one back. Not for ever:
        // a reading that takes a listing a call never ends while a file a call comes.
        let names = read_root(&fs, fh, read, |calls| {
            if calls < 50 {
                add(&state, &format!("b{calls:03}.bin"));
                remove(&state, &format!("a{:03}.bin", calls % 40));
            }
        })
        .await;
        let there_all_along: Vec<&String> = names.iter().filter(|n| n.starts_with('m')).collect();
        assert_eq!(there_all_along, kept.iter().collect::<Vec<_>>(), "{read:?}");
        let mut unique = names.clone();
        unique.dedup();
        assert_eq!(unique, names, "{read:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_folder_is_read_from_one_listing_until_its_reading_begins_again() {
    let (state, fs) = union_of(&numbered("m", 0..100)).await;
    let fh = open_root(&fs).await;
    let taken = |fs: &RviewFs| fs.dirs.lock().listings[&fh.0].clone();
    assert!(taken(&fs).is_none()); // opened, and not read yet
    let mut listings = Vec::new();
    let names = read_root(&fs, fh, Read::Names, |_| listings.push(taken(&fs).unwrap())).await;
    assert_eq!(names, numbered("m", 0..100));
    assert!(listings.len() > 10);
    assert!(listings.iter().all(|l| Arc::ptr_eq(l, &listings[0])));
    drop(listings);

    // Another reading, of another open, takes its own; the first still reads on in its.
    add(&state, "n000.bin");
    remove(&state, "m099.bin");
    let other = open_root(&fs).await;
    let mut now = numbered("m", 0..99);
    now.push("n000.bin".to_owned());
    assert_eq!(read_root(&fs, other, Read::Names, |_| ()).await, now);
    let (last, _) = call(&fs, fh, Read::Names, 101).await.pop().unwrap();
    assert_eq!(last, "m099.bin");
    // Begun again (rewinddir reads from offset 0), it is of the folder as it is then.
    assert_eq!(read_root(&fs, fh, Read::Names, |_| ()).await, now);

    // A closed folder keeps no listing.
    for fh in [fh, other] {
        fs.releasedir(REQUEST, ROOT, fh, 0).await.unwrap();
    }
    assert!(fs.dirs.lock().listings.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_gone_since_the_listing_are_left_out_and_the_reading_goes_on() {
    let files = numbered("m", 0..60);
    let (state, fs) = union_of(&files).await;
    let fh = open_root(&fs).await;
    // More files in a row than a call returns go once the reading has begun: a call that
    // returned none of them, and nothing else, would end the reading before the rest.
    let names = read_root(&fs, fh, Read::Plus, |calls| {
        if calls == 0 {
            for file in &files[10..40] {
                remove(&state, file);
            }
        }
    })
    .await;
    let left: Vec<&String> = files[..10].iter().chain(&files[40..]).collect();
    assert_eq!(names.iter().collect::<Vec<_>>(), left);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_folder_first_read_from_further_in_is_read_from_a_listing_taken_then() {
    let (_state, fs) = union_of(&numbered("m", 0..20)).await;
    // seekdir before any read; and a handle that is not open here reads all the same.
    for fh in [open_root(&fs).await, FileHandleId(77)] {
        let page = call(&fs, fh, Read::Names, 12).await;
        assert_eq!(page[0], ("m010.bin".to_owned(), 13));
        // Past the end, nothing.
        assert!(call(&fs, fh, Read::Names, 22).await.is_empty());
    }
    // The open handle reads on in that listing; the other is of no folder open, and
    // nothing is kept for it.
    assert_eq!(fs.dirs.lock().listings.len(), 1);
    assert!(fs.dirs.lock().listings.values().all(Option::is_some));
}
