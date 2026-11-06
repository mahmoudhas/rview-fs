use super::*;

#[test]
fn only_paths_inside_an_export_parse() {
    let path = RelPath::parse("a/b c/x.bin").unwrap();
    assert_eq!(path.split(), ("a/b c", "x.bin"));
    assert_eq!(RelPath::parse("x").unwrap().split(), ("", "x"));
    for bad in [
        "", "/x", "x/", "a//b", "../x", "a/../b", "./x", ".hidden", "a/.tmp", "a\0b",
    ] {
        assert_eq!(RelPath::parse(bad), None, "{bad}");
    }
    assert_eq!(join("", "x"), "x");
    assert_eq!(join("a/b", "x"), "a/b/x");
}

/// A file `depth` folders down below `root`, holding `data`, and its path from `root`.
/// Each folder's name is 32 characters, as a token tree's page digests are.
pub(crate) fn deep_file(root: &Path, depth: usize, data: &[u8]) -> RelPath {
    use std::io::Write as _;
    let mut fd = rustix::fs::open(root, FOLDER, Mode::empty()).unwrap();
    let mut dir = String::new();
    for level in 0..depth {
        let name = format!("{level:032}");
        rustix::fs::mkdirat(&fd, name.as_str(), Mode::from_raw_mode(0o755)).unwrap();
        fd = rustix::fs::openat(&fd, name.as_str(), FOLDER, Mode::empty()).unwrap();
        dir = join(&dir, &name);
    }
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::CLOEXEC;
    let file = rustix::fs::openat(&fd, "x.bin", flags, Mode::from_raw_mode(0o644)).unwrap();
    File::from(file).write_all(data).unwrap();
    RelPath::parse(&join(&dir, "x.bin")).unwrap()
}

#[test]
fn a_path_longer_than_the_kernel_takes_opens_one_folder_at_a_time() {
    use std::io::Read as _;
    let dir = tempfile::tempdir().unwrap();
    let path = deep_file(dir.path(), 150, b"deep");
    let whole = dir.path().join(path.as_str());
    assert!(whole.as_os_str().len() > 4096);
    // Given whole, the path is too long for any call.
    let refused = File::open(&whole).unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(libc::ENAMETOOLONG));
    let mut data = Vec::new();
    let mut file = open_file(dir.path(), &path).unwrap();
    file.read_to_end(&mut data).unwrap();
    assert_eq!(data, b"deep");
    assert!(open_dir(dir.path(), path.split().0).is_ok());
}
