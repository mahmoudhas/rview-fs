use prometheus::{IntCounter, Registry};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use super::serve;

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
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
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
