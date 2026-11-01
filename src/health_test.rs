use super::*;

fn paths(of: &[(&str, u64)]) -> Vec<(String, u64)> {
    of.iter().map(|(p, n)| ((*p).to_owned(), *n)).collect()
}

#[test]
fn only_files_rdma_could_have_moved_count_as_falling_back() {
    let seen = paths(&[
        ("rdma/ok", 40),
        ("chunked/below_min", 3),
        ("chunked/budget", 2),
        ("chunked/no_offer", 0),
        ("chunked/not_pinned", 1),
    ]);
    assert_eq!(
        tcp_fallbacks(&seen),
        paths(&[("chunked/budget", 2), ("chunked/not_pinned", 1)])
    );
    assert!(tcp_fallbacks(&paths(&[("rdma/ok", 9), ("chunked/below_min", 1)])).is_empty());
}

#[test]
fn a_reason_is_said_once_until_it_changes() {
    let degraded = Degraded::default();
    degraded.report("RDMA", "no device".into());
    degraded.report("RDMA", "no device".into());
    assert_eq!(degraded.missing.lock().len(), 1);
    degraded.report("RDMA", "the pool is spent".into());
    assert_eq!(degraded.missing.lock()["RDMA"], "the pool is spent");
}
