use std::io::Write as _;
use std::os::unix::fs::FileExt as _;
use std::path::Path;

use super::*;

const A: u8 = 1;
const B: u8 = 2;

fn file(dir: &Path, name: &str, bytes: &[u8]) -> File {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
    File::open(path).unwrap()
}

/// The fds of this process open on files below `dir`.
fn fds_below(dir: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|fd| std::fs::read_link(fd.ok()?.path()).ok())
        .filter(|target| target.starts_with(dir))
        .count()
}

#[test]
fn a_held_file_stays_readable_after_it_is_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let held = Held::new(8);
    let now = Instant::now();
    let handle = held
        .open(A, file(dir.path(), "x.bin", b"page"), now)
        .unwrap();
    std::fs::remove_file(dir.path().join("x.bin")).unwrap();
    let mut read = [0u8; 4];
    let open = held.get(A, handle, now).unwrap();
    open.read_exact_at(&mut read, 0).unwrap();
    assert_eq!(&read, b"page");
    // Another node's handle is not this one's.
    assert!(held.get(B, handle, now).is_none());
}

#[test]
fn a_file_closed_during_a_read_closes_once_the_read_is_done() {
    let dir = tempfile::tempdir().unwrap();
    let held = Held::new(8);
    let now = Instant::now();
    let handle = held
        .open(A, file(dir.path(), "x.bin", b"page"), now)
        .unwrap();
    assert_eq!(fds_below(dir.path()), 1);
    let reading = held.get(A, handle, now).unwrap();
    assert!(held.close(A, handle));
    assert!(!held.close(A, handle)); // closed already
    assert_eq!(held.count(), 0);
    let mut read = [0u8; 4];
    reading.read_exact_at(&mut read, 0).unwrap();
    assert_eq!(fds_below(dir.path()), 1);
    drop(reading);
    assert_eq!(fds_below(dir.path()), 0);
}

#[test]
fn a_file_its_node_no_longer_lists_is_closed_once_its_open_has_answered() {
    let dir = tempfile::tempdir().unwrap();
    let held = Held::new(8);
    let t0 = Instant::now();
    let kept = held
        .open(A, file(dir.path(), "kept.bin", b"1"), t0)
        .unwrap();
    let dropped = held
        .open(A, file(dir.path(), "dropped.bin", b"2"), t0)
        .unwrap();
    let other = held
        .open(B, file(dir.path(), "other.bin", b"3"), t0)
        .unwrap();
    // Unlisted, but its open may not have answered yet.
    assert_eq!(held.renew(A, &[kept], t0 + Duration::from_secs(1)), 0);
    let later = t0 + GRACE + Duration::from_secs(1);
    assert_eq!(held.renew(A, &[kept], later), 1);
    assert!(held.get(A, dropped, later).is_none());
    assert!(held.get(A, kept, later).is_some());
    // A list is of its own node's files only.
    assert!(held.get(B, other, later).is_some());
    assert_eq!(held.count(), 2);
    assert_eq!(fds_below(dir.path()), 2);
}

#[test]
fn a_silent_node_has_its_files_closed() {
    let dir = tempfile::tempdir().unwrap();
    let held = Held::new(8);
    let t0 = Instant::now();
    held.open(A, file(dir.path(), "a.bin", b"1"), t0).unwrap();
    let b = held.open(B, file(dir.path(), "b.bin", b"2"), t0).unwrap();
    // B is heard from later, when it reads.
    assert!(held.get(B, b, t0 + Duration::from_secs(30)).is_some());
    assert_eq!(held.expire(t0 + LEASE), 0);
    assert_eq!(held.expire(t0 + LEASE + Duration::from_secs(1)), 1);
    assert_eq!(held.count(), 1);
    assert_eq!(fds_below(dir.path()), 1);
    assert_eq!(held.expire(t0 + LEASE + Duration::from_secs(31)), 1);
    assert_eq!((held.count(), fds_below(dir.path())), (0, 0));
}

#[test]
fn no_more_than_the_limit_are_held() {
    let dir = tempfile::tempdir().unwrap();
    let held = Held::new(1);
    let now = Instant::now();
    let first = held.open(A, file(dir.path(), "a.bin", b"1"), now).unwrap();
    let refused = held.open(B, file(dir.path(), "b.bin", b"2"), now);
    assert!(refused.is_err());
    drop(refused); // the file came back, and is closed with it
    assert_eq!(fds_below(dir.path()), 1);
    assert!(held.close(A, first));
    assert!(held.open(B, file(dir.path(), "b.bin", b"2"), now).is_ok());
}
