use super::*;
use crate::index::EntryKind;

#[test]
fn hidden_files_being_written_are_not_taken() {
    let announcement: Announcement = serde_json::from_str(
        r#"{"node": "a", "updates": [
            {"op": "add", "path": "d/x.bin", "size": 4, "mtime": 1.0},
            {"op": "add", "path": "d/.x.bin.k3j.tmp", "size": 2, "mtime": 1.0},
            {"op": "add", "path": ".writing/y.bin", "size": 2, "mtime": 1.0},
            {"op": "add", "path": "/etc/passwd", "size": 2, "mtime": 1.0},
            {"op": "add", "path": "d/../z.bin", "size": 2, "mtime": 1.0}
        ]}"#,
    )
    .unwrap();
    let node = NodeId::parse(&announcement.node).unwrap();
    let mut index = Index::new();
    apply(&mut index, &node, announcement.updates);
    assert_eq!(index.list(""), Some(vec![("d".to_owned(), EntryKind::Dir)]));
    assert_eq!(
        index.list("d"),
        Some(vec![("x.bin".to_owned(), EntryKind::File)])
    );
}
