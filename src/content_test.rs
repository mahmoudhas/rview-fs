use std::io::Read as _;
use std::os::unix::fs::FileExt as _;

use super::*;

#[test]
fn a_memfd_holds_what_is_written_into_its_mapping() {
    let (file, map) = memfd("x.bin", 5).unwrap();
    let mut map = map.unwrap();
    map.copy_from_slice(b"hello");
    let content = Content::new(file, Some(map)).unwrap();
    assert_eq!(content.bytes, Bytes::from_static(b"hello"));
    // What the kernel reads when it passes the file through.
    let mut read = [0u8; 5];
    content.file.read_exact_at(&mut read, 0).unwrap();
    assert_eq!(&read, b"hello");
}

#[test]
fn an_empty_file_has_nothing_mapped() {
    let (file, map) = memfd("empty.bin", 0).unwrap();
    assert!(map.is_none());
    let content = Content::new(file, None).unwrap();
    assert!(content.bytes.is_empty());
    let mut rest = Vec::new();
    (&*content.file).read_to_end(&mut rest).unwrap();
    assert!(rest.is_empty());
}
