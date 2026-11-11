//! Prometheus metrics over HTTP: velo's, and rview-fs's own (transfer.rs).

use std::time::Duration;

use anyhow::Context as _;
use prometheus::{Encoder as _, Registry, TextEncoder};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// A scraper that sends no request in this long is dropped.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REQUEST_HEAD: usize = 8192;

/// Answer every request on `listener` with all of `registry`'s metrics, whatever its
/// path, until cancelled.
pub async fn serve(
    listener: TcpListener,
    registry: Registry,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        let accepted = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let mut stream = match accepted {
            Ok((stream, _)) => stream,
            // Out of file descriptors, say: the metrics wait, and the mount goes on.
            Err(e) => {
                tracing::warn!("accepting a metrics request failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let registry = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = answer(&mut stream, &registry).await {
                tracing::debug!("a metrics request failed: {e:#}");
            }
        });
    }
}

async fn answer(stream: &mut TcpStream, registry: &Registry) -> anyhow::Result<()> {
    tokio::time::timeout(REQUEST_TIMEOUT, read_request_head(stream))
        .await
        .context("no request")??;
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    encoder.encode(&registry.gather(), &mut body)?;
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        encoder.format_type(),
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn read_request_head(stream: &mut TcpStream) -> anyhow::Result<()> {
    let (mut head, mut buf) = (Vec::new(), [0; 1024]);
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await?;
        anyhow::ensure!(n > 0, "closed before the end of its request");
        head.extend_from_slice(&buf[..n]);
        anyhow::ensure!(
            head.len() <= MAX_REQUEST_HEAD,
            "a request head over {MAX_REQUEST_HEAD} bytes"
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod tests;
