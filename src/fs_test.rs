use super::*;
use crate::cache::Key;
use crate::index::NodeId;
use crate::index::Stat;
use crate::path::RelPath;
use crate::state::testing::{state, state_of, velo};
use crate::used::tests::{age, is_marked};

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

#[tokio::test]
async fn each_call_of_the_kernel_is_timed_by_its_name() {
    let (state, fs) = union_of(&["a.bin".to_owned()]).await;
    assert_eq!(state.ops.count("lookup"), 0);
    fs.lookup(REQUEST, ROOT, OsStr::new("a.bin")).await.unwrap();
    assert_eq!(
        fs.lookup(REQUEST, ROOT, OsStr::new("b.bin")).await.err(),
        Some(ENOENT)
    );
    let fh = open_root(&fs).await;
    fs.readdir(REQUEST, ROOT, fh, 0, 4096).await.unwrap();
    // One that fails is timed too: a lookup of a name that is not there waits as long.
    assert_eq!(state.ops.count("lookup"), 2);
    assert_eq!(state.ops.count("opendir"), 1);
    assert_eq!(state.ops.count("readdir"), 1);
    assert_eq!(state.ops.count("read"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_a_file_of_this_node_marks_it_used() {
    let export = tempfile::tempdir().unwrap();
    let path = export.path().join("a.bin");
    std::fs::write(&path, b"1234").unwrap();
    age(&path);
    let state = state_of(velo().await, export.path().to_owned(), 0);
    add(&state, "a.bin");
    let fs = RviewFs::new(Arc::clone(&state), tokio::runtime::Handle::current(), None);
    let found = fs.lookup(REQUEST, ROOT, OsStr::new("a.bin")).await.unwrap();
    let inode = InodeId(found.attr.ino);
    assert!(!is_marked(&path), "looked up, not opened");
    let opened = fs.open(REQUEST, inode, libc::O_RDONLY as u32).await;
    assert!(is_marked(&path));
    let fh = opened.unwrap().fh;
    fs.release(REQUEST, inode, fh, 0, 0, false, false)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_a_file_of_another_node_from_memory_notes_it_for_its_node() {
    let state = state_of(velo().await, "/nonexistent".into(), 1 << 20);
    let key = Key {
        node: NodeId::parse("b").unwrap(),
        path: RelPath::parse("x.bin").unwrap(),
    };
    let stat = Stat {
        size: 4,
        mtime: 1.0,
    };
    state.index.write().add(&key.node, &key.path, stat);
    // Read before, and kept.
    let read = async { Ok(Bytes::from_static(b"1234")) };
    state.cache.get_or_fetch(&key, || read).await.unwrap();
    let fs = RviewFs::new(Arc::clone(&state), tokio::runtime::Handle::current(), None);
    let found = fs.lookup(REQUEST, ROOT, OsStr::new("x.bin")).await.unwrap();
    assert!(state.used.take().is_empty());
    let inode = InodeId(found.attr.ino);
    let opened = fs.open(REQUEST, inode, libc::O_RDONLY as u32).await;
    let mut buf = [0u8; 4];
    let fh = opened.unwrap().fh;
    assert_eq!(fs.read(REQUEST, inode, fh, 0, &mut buf).await.unwrap(), 4);
    assert_eq!(&buf, b"1234");
    let noted = state.used.take();
    assert_eq!(noted.len(), 1);
    assert!(noted[&key.node].contains(&key.path));
}

/// The next message `forget_gone` wrote for the kernel: the inode of the folder and the
/// name in it that it is to drop.
fn told(kernel: &mut impl io::Read) -> (u64, String) {
    use fractal_fuse::abi::{
        FUSE_NOTIFY_INVAL_ENTRY, fuse_notify_inval_entry_out, fuse_out_header,
    };
    let word = |bytes: &[u8]| u64::from_ne_bytes(bytes.try_into().unwrap());
    let mut header = [0; size_of::<fuse_out_header>()];
    kernel.read_exact(&mut header).unwrap();
    let len = u32::from_ne_bytes(header[..4].try_into().unwrap()) as usize;
    assert_eq!(
        i32::from_ne_bytes(header[4..8].try_into().unwrap()),
        FUSE_NOTIFY_INVAL_ENTRY
    );
    let mut rest = vec![0; len - header.len()];
    kernel.read_exact(&mut rest).unwrap();
    let (entry, name) = rest.split_at(size_of::<fuse_notify_inval_entry_out>());
    let name = name.strip_suffix(&[0]).expect("a name ends with a NUL");
    (word(&entry[..8]), String::from_utf8(name.to_vec()).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_kernel_is_told_of_a_file_gone_and_of_its_folder_when_it_holds_their_inodes() {
    let files = ["d/held.bin".to_owned(), "d/never.bin".to_owned()];
    let (state, fs) = union_of(&files).await;
    // The kernel looked d up, and one of its two files.
    let d = fs.lookup(REQUEST, ROOT, OsStr::new("d")).await.unwrap();
    let d = InodeId(d.attr.ino);
    fs.lookup(REQUEST, d, OsStr::new("held.bin")).await.unwrap();
    let (mut kernel, ours) = io::pipe().unwrap();
    let cancel = CancellationToken::new();
    let notifier = FuseNotifier::from(Arc::new(std::os::fd::OwnedFd::from(ours)));
    let telling = tokio::spawn(fs.forget_gone(notifier, cancel.clone()));
    // Until it has asked the index to note what goes.
    tokio::time::sleep(Duration::from_millis(100)).await;
    remove(&state, "d/never.bin");
    remove(&state, "d/held.bin");
    let heard = tokio::task::spawn_blocking(move || [told(&mut kernel), told(&mut kernel)]);
    let heard = tokio::time::timeout(Duration::from_secs(10), heard)
        .await
        .expect("told within its interval")
        .unwrap();
    assert_eq!(
        heard,
        [(d.0, "held.bin".to_owned()), (ROOT.0, "d".to_owned())]
    );
    cancel.cancel();
    telling.await.unwrap().unwrap();
}

/// A request as the kernel writes it to /dev/fuse: its header, then `body`.
fn request(opcode: u32, nodeid: u64, body: &[u64]) -> Vec<u8> {
    let len = (size_of::<fuse_in_header>() + 8 * body.len()) as u32;
    let mut request = [len.to_ne_bytes(), opcode.to_ne_bytes()].concat();
    request.extend(
        [7, nodeid, 0, 0]
            .iter()
            .flat_map(|word: &u64| word.to_ne_bytes()),
    );
    request.extend(body.iter().flat_map(|word| word.to_ne_bytes()));
    request
}

#[test]
fn the_inodes_the_kernel_forgets_are_read_from_its_request_one_or_many() {
    assert_eq!(forgotten(&request(FUSE_FORGET, 5, &[3])), [(5, 3)]);
    // Two of them, counted in the first half of the word after the header.
    let many = request(FUSE_BATCH_FORGET, 0, &[2, 5, 3, 9, 1]);
    assert_eq!(forgotten(&many), [(5, 3), (9, 1)]);
    // Cut short, it forgets what is whole.
    assert_eq!(forgotten(&many[..many.len() - 8]), [(5, 3)]);
    // An interrupt of a call is nothing to forget.
    assert!(forgotten(&request(36, 0, &[7])).is_empty());
    assert!(forgotten(&[]).is_empty());
}

#[test]
fn an_inode_is_counted_until_every_lookup_of_it_is_forgotten() {
    let mut inodes = Inodes::new();
    let ino = inodes.remember("d/x.bin");
    assert_eq!(inodes.remember("d/x.bin"), ino);
    assert_eq!(inodes.held.get(), 1);
    inodes.forget(ino, 1);
    assert_eq!(
        (inodes.held.get(), inodes.path(ino).as_deref()),
        (1, Some("d/x.bin"))
    );
    inodes.forget(ino, 1);
    assert_eq!((inodes.held.get(), inodes.path(ino)), (0, None));
}
