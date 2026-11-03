use super::*;

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
