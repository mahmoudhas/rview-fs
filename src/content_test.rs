use super::*;

#[test]
fn what_is_fetched_into_a_buffer_is_its_bytes() {
    let mut map = buffer(5).unwrap().unwrap();
    map.copy_from_slice(b"hello");
    assert_eq!(freeze(Some(map)).unwrap(), Bytes::from_static(b"hello"));
}

#[test]
fn an_empty_file_needs_no_buffer() {
    assert!(buffer(0).unwrap().is_none());
    assert!(freeze(None).unwrap().is_empty());
}
