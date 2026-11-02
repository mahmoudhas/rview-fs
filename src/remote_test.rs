use super::*;

#[test]
fn each_node_is_listed_the_handles_open_on_it() {
    let files = RemoteFiles::default();
    let (a, b) = (InstanceId::new_v4(), InstanceId::new_v4());
    files.add(a, 3);
    files.add(a, 1);
    files.add(b, 2);
    files.remove(a, 3);
    let mut lists = files.lists();
    lists.sort_by_key(|(_, open)| open.clone());
    assert_eq!(lists, vec![(a, vec![1]), (b, vec![2])]);
    // A node with nothing open is listed no more: what it still holds expires.
    files.remove(a, 1);
    files.remove(b, 2);
    assert!(files.lists().is_empty());
}
