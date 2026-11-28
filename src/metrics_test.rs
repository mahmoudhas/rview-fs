use std::sync::Arc;
use std::time::Duration;

use prometheus::{IntCounter, Registry};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use super::{serve, serve_at};
use crate::health::Degraded;

async fn scrape(stream: &mut TcpStream) -> String {
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await.unwrap();
    reply
}

#[tokio::test]
async fn an_address_that_is_taken_is_served_once_it_is_free() {
    let registry = Registry::new();
    let counter = IntCounter::new("rview_fs_test_total", "A test counter.").unwrap();
    registry.register(Box::new(counter)).unwrap();
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = holder.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let degraded = Arc::new(Degraded::default());
    let server = {
        let (degraded, cancel) = (Arc::clone(&degraded), cancel.clone());
        tokio::spawn(async move { serve_at(addr, registry, &degraded, cancel).await })
    };
    // Two tries at least, and it still waits for the address.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!server.is_finished());
    drop(holder);
    let reply = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(mut stream) = TcpStream::connect(addr).await {
                break scrape(&mut stream).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the address was free and nothing served on it");
    assert!(reply.ends_with("\nrview_fs_test_total 0\n"), "{reply}");
    cancel.cancel();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn waiting_for_a_taken_address_ends_when_cancelled() {
    let holder = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = holder.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let degraded = Arc::new(Degraded::default());
    let server = {
        let (degraded, cancel) = (Arc::clone(&degraded), cancel.clone());
        tokio::spawn(async move { serve_at(addr, Registry::new(), &degraded, cancel).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("it went on waiting for the address")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_scrape_gets_every_metric_of_the_registry() {
    let registry = Registry::new();
    let counter = IntCounter::new("rview_fs_test_total", "A test counter.").unwrap();
    registry.register(Box::new(counter.clone())).unwrap();
    counter.inc_by(7);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let server = tokio::spawn(serve(listener, registry, cancel.clone()));
    for _ in 0..2 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let reply = scrape(&mut stream).await;
        assert!(reply.starts_with("HTTP/1.1 200 OK\r\n"), "{reply}");
        assert!(
            reply.contains("\r\n\r\n# HELP rview_fs_test_total A test counter.\n"),
            "{reply}"
        );
        assert!(reply.ends_with("\nrview_fs_test_total 7\n"), "{reply}");
    }
    cancel.cancel();
    server.await.unwrap().unwrap();
}
