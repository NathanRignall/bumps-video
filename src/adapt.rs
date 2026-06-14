//! Adaptive bitrate: watches SRT health and adjusts the encoder's `bitrate`
//! property up or down to fit the path.
//!
//! Algorithm (intentionally simple — the most common failure mode for these is
//! oscillation, so step size and cooldown are deliberately conservative):
//!
//! - **Step DOWN** when stats look bad for several consecutive seconds.
//!   "Bad" = packet loss > 1% **or** send buffer > 70%.
//! - **Step UP** when stats look clean for ~30 consecutive seconds.
//!   "Clean" = packet loss < 0.1% **and** send buffer < 30%.
//! - Each change is a 25% step. After any change, we hold for `COOLDOWN`
//!   before considering another so the encoder + SRT have time to settle.
//!
//! Reset to nominal when there's no active session (publisher gone), so each
//! new session starts at the user-configured target.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gstreamer::glib::object::ObjectExt;
use tokio::sync::{mpsc, watch};

use crate::capture::CaptureEventReq;
use crate::stats::{Snapshot, StatsState};

const COOLDOWN: Duration = Duration::from_secs(10);
const BAD_TICKS_TO_STEP_DOWN: u32 = 5; // ~5 s of sustained bad stats
const GOOD_TICKS_TO_STEP_UP: u32 = 30; // ~30 s of clean stats

const STEP_DOWN_MULT: u32 = 75; // ×0.75
const STEP_UP_MULT: u32 = 125; // ×1.25
const PERCENT: u32 = 100;

#[derive(Clone, Debug)]
pub struct AdapterConfig {
    pub enabled: bool,
    pub nominal_kbps: u32,
    pub min_kbps: u32,
    pub max_kbps: u32,
}

pub async fn run(
    state: Arc<StatsState>,
    cfg: AdapterConfig,
    mut stats_rx: watch::Receiver<Snapshot>,
    capture_events: mpsc::Sender<CaptureEventReq>,
) {
    if !cfg.enabled {
        tracing::info!("adapt: disabled");
        // Park forever so the task handle is still alive in main's select!.
        std::future::pending::<()>().await;
        return;
    }
    tracing::info!(
        nominal = cfg.nominal_kbps,
        min = cfg.min_kbps,
        max = cfg.max_kbps,
        "adapt: bitrate adaptation enabled"
    );

    let mut target = cfg.nominal_kbps;
    state.adapt_target_kbps.store(target, Ordering::Relaxed);

    let mut bad_streak: u32 = 0;
    let mut good_streak: u32 = 0;
    let mut last_change = Instant::now() - COOLDOWN; // allow an immediate decision

    loop {
        if stats_rx.changed().await.is_err() {
            return;
        }
        let snap = stats_rx.borrow_and_update().clone();

        // Operator pin takes precedence. Clamp into [min, max], apply when
        // it differs from current target, then bail out (no streak update,
        // no auto step).
        let override_kbps = state.adapt_override_kbps.load(Ordering::Relaxed);
        if override_kbps != 0 {
            let clamped = override_kbps.clamp(cfg.min_kbps, cfg.max_kbps);
            if target != clamped {
                target = clamped;
                apply(&state, target);
                last_change = Instant::now();
            }
            bad_streak = 0;
            good_streak = 0;
            continue;
        }

        // No active session → reset target and skip.
        if !snap.downlink.connected {
            if target != cfg.nominal_kbps {
                tracing::debug!("adapt: session gone, reset target to nominal");
            }
            target = cfg.nominal_kbps;
            state.adapt_target_kbps.store(target, Ordering::Relaxed);
            bad_streak = 0;
            good_streak = 0;
            continue;
        }

        // Classify this tick.
        let bad = is_bad(&snap);
        let good = is_good(&snap);
        if bad {
            bad_streak = bad_streak.saturating_add(1);
            good_streak = 0;
        } else if good {
            good_streak = good_streak.saturating_add(1);
            bad_streak = 0;
        } else {
            // Neither — degraded but not bad. Decay streaks slowly.
            bad_streak = bad_streak.saturating_sub(1);
            good_streak = good_streak.saturating_sub(1);
        }

        if last_change.elapsed() < COOLDOWN {
            continue;
        }

        let new_target = if bad_streak >= BAD_TICKS_TO_STEP_DOWN {
            let candidate = (target * STEP_DOWN_MULT / PERCENT).max(cfg.min_kbps);
            if candidate != target {
                state
                    .adapt_step_downs
                    .fetch_add(1, Ordering::Relaxed);
                Some(candidate)
            } else {
                None
            }
        } else if good_streak >= GOOD_TICKS_TO_STEP_UP {
            let candidate = (target * STEP_UP_MULT / PERCENT).min(cfg.max_kbps);
            if candidate != target {
                state.adapt_step_ups.fetch_add(1, Ordering::Relaxed);
                Some(candidate)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(new_target) = new_target {
            let direction = if new_target > target { "up" } else { "down" };
            tracing::info!(
                from = target,
                to = new_target,
                direction,
                "adapt: bitrate change"
            );
            let from = target;
            target = new_target;
            apply(&state, target);
            last_change = Instant::now();
            bad_streak = 0;
            good_streak = 0;

            // Best-effort: post to the capture event log. If channel is full
            // (would be surprising at this rate) or pipeline gone, drop.
            let _ = capture_events
                .try_send(CaptureEventReq {
                    kind: "bitrate_changed".into(),
                    details: serde_json::json!({
                        "from_kbps": from,
                        "to_kbps": new_target,
                        "direction": direction,
                        "loss_rate": snap.uplink.pkt_loss_rate,
                        "send_buf_pct": snap.uplink.send_buf_pct,
                        "rtt_ms": snap.uplink.rtt_ms,
                    }),
                });
        }
    }
}

fn is_bad(s: &Snapshot) -> bool {
    s.uplink.pkt_loss_rate > 0.01 || s.uplink.send_buf_pct > 0.7
}

fn is_good(s: &Snapshot) -> bool {
    s.uplink.pkt_loss_rate < 0.001 && s.uplink.send_buf_pct < 0.3
}

/// Apply a new bitrate to the live encoder. All three of our encoders
/// (`qsvh265enc`, `vtenc_h265`, `x264enc`) expose `bitrate` in kbps as a
/// runtime-settable property, so a single property write is sufficient.
fn apply(state: &StatsState, target_kbps: u32) {
    state
        .adapt_target_kbps
        .store(target_kbps, Ordering::Relaxed);
    let guard = match state.encoder.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    let Some(enc) = guard.as_ref() else {
        return;
    };
    if enc.find_property("bitrate").is_none() {
        tracing::warn!("adapt: encoder has no 'bitrate' property");
        return;
    }
    enc.set_property("bitrate", target_kbps);
}
