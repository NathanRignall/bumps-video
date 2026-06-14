//! AWS reachability probe — cheap stand-in for ICMP.
//!
//! TCP connect timing to a configurable `host:port` (default: an AWS endpoint)
//! gives us a meaningful "is the path to AWS healthy" number without needing
//! raw sockets. The measurement includes DNS resolution + TCP handshake, which
//! is exactly what a real SRT-to-AWS leg pays at session start anyway.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpStream;

use crate::stats::StatsState;

/// How often we probe.
const INTERVAL: Duration = Duration::from_secs(2);
/// How long we'll wait for a single TCP connect before declaring failure.
const TIMEOUT: Duration = Duration::from_secs(3);

pub async fn run(state: Arc<StatsState>, target: String) {
    tracing::info!(%target, "ping: AWS reachability probe started");
    let mut tick = tokio::time::interval(INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        let started = Instant::now();
        let attempt = tokio::time::timeout(TIMEOUT, TcpStream::connect(&target)).await;

        match attempt {
            Ok(Ok(_stream)) => {
                let rtt_us = started.elapsed().as_micros() as u64;
                state.ping_last_rtt_us.store(rtt_us, Ordering::Relaxed);
                state
                    .ping_last_success_us
                    .store(state.now_us(), Ordering::Relaxed);
                state.ping_success_total.fetch_add(1, Ordering::Relaxed);
                tracing::trace!(rtt_us, "ping: ok");
            }
            Ok(Err(e)) => {
                state.ping_failure_total.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(error = %e, "ping: connect failed");
            }
            Err(_) => {
                state.ping_failure_total.fetch_add(1, Ordering::Relaxed);
                tracing::debug!("ping: timeout");
            }
        }
    }
}
