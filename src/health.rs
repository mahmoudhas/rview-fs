//! What rview-fs runs without, because the machine does not offer it: passthrough, or
//! RDMA. It goes on without either, slower, and says so loudly: at once, and again every
//! ten minutes for as long as it does.

use std::collections::BTreeMap;
use std::time::Duration;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

const REMIND_EVERY: Duration = Duration::from_secs(600);

#[derive(Default)]
pub struct Degraded {
    /// What is missing, and why, by what it is.
    missing: Mutex<BTreeMap<&'static str, String>>,
}

impl Degraded {
    /// `what` is missing, for `why`. Said now unless it was said already, for the same
    /// reason.
    pub fn report(&self, what: &'static str, why: String) {
        let mut missing = self.missing.lock();
        if missing.get(what) == Some(&why) {
            return;
        }
        tracing::error!("DEGRADED: {what} is not available: {why}");
        missing.insert(what, why);
    }

    /// Say again every ten minutes what is missing, until cancelled.
    pub async fn remind(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let mut tick = tokio::time::interval(REMIND_EVERY);
        tick.tick().await; // the first tick is now, and report just said it
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                _ = tick.tick() => {}
            }
            for (what, why) in self.missing.lock().iter() {
                tracing::error!("DEGRADED: {what} is still not available: {why}");
            }
        }
    }
}

/// The transfers that went over TCP although RDMA was on, from velo's counts of
/// transfers by path (`rdma/ok`, or `chunked/` and the reason). Files too small for
/// RDMA (`chunked/below_min`) always go over TCP.
pub fn tcp_fallbacks(paths: &[(String, u64)]) -> Vec<(String, u64)> {
    paths
        .iter()
        .filter(|(path, count)| {
            *count > 0 && path.starts_with("chunked/") && path != "chunked/below_min"
        })
        .cloned()
        .collect()
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
