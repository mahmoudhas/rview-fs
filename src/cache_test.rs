use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn key(name: &str) -> Key {
    Key {
        node: NodeId::parse("b").unwrap(),
        path: RelPath::parse(name).unwrap(),
    }
}

#[tokio::test]
async fn a_file_is_fetched_once_for_all_its_readers() {
    let cache = ContentCache::new(1 << 20);
    let fetches = AtomicUsize::new(0);
    let fetch = || async {
        fetches.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(Bytes::from_static(b"page"))
    };
    let x = key("x");
    let (a, b) = tokio::join!(cache.get_or_fetch(&x, fetch), cache.get_or_fetch(&x, fetch));
    assert_eq!(
        (a.unwrap(), b.unwrap()),
        (Bytes::from_static(b"page"), Bytes::from_static(b"page"))
    );
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert_eq!(cache.get(&key("x")), Some(Bytes::from_static(b"page")));
    assert_eq!(cache.bytes(), 4);
}

#[tokio::test]
async fn a_failed_fetch_is_tried_again() {
    let cache = ContentCache::new(1 << 20);
    let failed = cache
        .get_or_fetch(&key("x"), || async { anyhow::bail!("peer gone") })
        .await;
    assert!(failed.is_err() && cache.get(&key("x")).is_none());
    assert!(cache.is_empty()); // a failed fetch leaves nothing behind
    let got = cache
        .get_or_fetch(&key("x"), || async { Ok(Bytes::from_static(b"ok")) })
        .await;
    assert_eq!(got.unwrap(), Bytes::from_static(b"ok"));
}

#[tokio::test]
async fn the_least_recently_used_go_past_the_budget() {
    let cache = ContentCache::new(10);
    for name in ["a", "b"] {
        let fetched = cache
            .get_or_fetch(&key(name), || async { Ok(Bytes::from(vec![0u8; 4])) })
            .await;
        assert!(fetched.is_ok());
    }
    assert!(cache.get(&key("a")).is_some()); // a was read after b
    let fetched = cache
        .get_or_fetch(&key("c"), || async { Ok(Bytes::from(vec![0u8; 4])) })
        .await;
    assert!(fetched.is_ok());
    assert!(cache.get(&key("b")).is_none());
    assert!(cache.get(&key("a")).is_some() && cache.get(&key("c")).is_some());
    assert_eq!(cache.bytes(), 8);
}
