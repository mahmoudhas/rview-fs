use super::*;

#[test]
fn a_node_whose_rdma_stalled_is_read_over_tcp_and_later_over_rdma_by_one_read() {
    let stalls = Stalls::default();
    let b = NodeId::parse("b").unwrap();
    let (first, second) = (InstanceId::new_v4(), InstanceId::new_v4());
    assert_eq!(stalls.via(&b, first), Via::Rdma);
    assert!(stalls.stalled(&b, first));
    assert!(!stalls.stalled(&b, first)); // another read that was under way
    assert_eq!(stalls.via(&b, first), Via::Tcp);
    // A while later, one read tries RDMA again, and the others wait for it over TCP.
    let long_ago = Instant::now().checked_sub(TCP_AFTER_STALL).unwrap();
    stalls.since.lock().get_mut(&b).unwrap().1 = long_ago;
    assert_eq!(stalls.via(&b, first), Via::RdmaAgain);
    assert_eq!(stalls.via(&b, first), Via::Tcp);
    stalls.worked(&b);
    assert_eq!(stalls.via(&b, first), Via::Rdma);
    // A node that restarted is read over RDMA at once: its UCX is new.
    stalls.stalled(&b, first);
    assert_eq!(stalls.via(&b, second), Via::Rdma);
    assert!(stalls.stalled(&b, second));
}

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
