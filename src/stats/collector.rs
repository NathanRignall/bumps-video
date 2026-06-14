//! 1 Hz collector task: reads atomic counters + SRT element stats, computes
//! rolling EWMAs, publishes a `Snapshot` on a watch channel.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gstreamer::glib::object::ObjectExt;
use tokio::sync::watch;

use super::{
    DownlinkStats, EncoderStats, HealthRollup, PingStats, PipelineHealth, PreviewStats, Snapshot,
    StatsState, UplinkStats,
};

/// EWMA smoothing factor for bitrates and RTT (higher α = slower response).
const ALPHA: f32 = 0.5;

/// How frequently we tick. The watch channel will emit at most this often.
const TICK: Duration = Duration::from_millis(1000);

#[derive(Clone)]
pub struct CollectorConfig {
    pub target_bitrate_kbps: u32,
    pub encoder_codec_label: String,
    pub ping_target: String,
    pub adapt_enabled: bool,
    pub adapt_min_kbps: u32,
    pub adapt_max_kbps: u32,
    pub quality: f32,
}

pub async fn run(state: Arc<StatsState>, cfg: CollectorConfig, tx: watch::Sender<Snapshot>) {
    let mut prev = Sample::default();
    let mut down_bitrate_kbps_ewma = 0.0_f32;
    let mut enc_bitrate_kbps_ewma = 0.0_f32;
    let mut srt_rtt_ms_ewma = 0.0_f32;
    let mut srt_loss_ewma = 0.0_f32;
    let mut srt_send_kbps_ewma = 0.0_f32;
    let mut ping_rtt_ms_ewma = 0.0_f32;
    let mut prev_srt_sent_bytes: u64 = 0;

    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let now = state.now_us();
        let now_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let sample = Sample {
            ts_us: now,
            bytes_in: state.bytes_in.load(Ordering::Relaxed),
            frames_in: state.frames_in.load(Ordering::Relaxed),
            enc_bytes: state.enc_bytes_out.load(Ordering::Relaxed),
            enc_frames: state.enc_frames_out.load(Ordering::Relaxed),
        };

        // Per-second derived rates.
        let dt_s = if prev.ts_us == 0 {
            1.0
        } else {
            (sample.ts_us.saturating_sub(prev.ts_us) as f32) / 1_000_000.0
        }
        .max(0.05);

        let down_inst_kbps = bits_per_kbps(sample.bytes_in - prev.bytes_in, dt_s);
        let enc_inst_kbps = bits_per_kbps(sample.enc_bytes - prev.enc_bytes, dt_s);
        down_bitrate_kbps_ewma = ewma(down_bitrate_kbps_ewma, down_inst_kbps, ALPHA);
        enc_bitrate_kbps_ewma = ewma(enc_bitrate_kbps_ewma, enc_inst_kbps, ALPHA);

        let connected = state.publisher_connected.load(Ordering::Relaxed);
        let last_frame_us = state.last_frame_us.load(Ordering::Relaxed);
        let session_started_us = state.session_started_us.load(Ordering::Relaxed);

        let last_frame_age_ms = if last_frame_us == 0 {
            None
        } else {
            Some(((now.saturating_sub(last_frame_us)) / 1_000) as u32)
        };
        let session_uptime_s = if session_started_us == 0 {
            None
        } else {
            Some((now.saturating_sub(session_started_us) as f32) / 1_000_000.0)
        };

        // SRT stats from the element, if present.
        let raw_srt = poll_srt_stats(&state);
        let uplink_state_atomic = state.get_uplink_state();
        let uplink = if let Some(s) = raw_srt {
            srt_rtt_ms_ewma = ewma(srt_rtt_ms_ewma, s.rtt_ms, ALPHA);
            srt_loss_ewma = ewma(srt_loss_ewma, s.pkt_loss_rate, ALPHA);
            let send_byte_delta = s.sent_bytes.saturating_sub(prev_srt_sent_bytes);
            let send_inst_kbps = bits_per_kbps(send_byte_delta, dt_s);
            srt_send_kbps_ewma = ewma(srt_send_kbps_ewma, send_inst_kbps, ALPHA);
            prev_srt_sent_bytes = s.sent_bytes;
            UplinkStats {
                state: uplink_state_atomic,
                rtt_ms: srt_rtt_ms_ewma,
                send_kbps: srt_send_kbps_ewma,
                link_cap_mbps: s.bandwidth_mbps,
                send_buf_pct: s.send_buf_pct,
                sent_bytes: s.sent_bytes,
                retransmitted_pkts: s.retransmitted_pkts,
                lost_pkts: s.lost_pkts,
                pkt_loss_rate: srt_loss_ewma,
            }
        } else {
            prev_srt_sent_bytes = 0;
            srt_send_kbps_ewma = 0.0;
            UplinkStats {
                state: uplink_state_atomic,
                rtt_ms: 0.0,
                send_kbps: 0.0,
                link_cap_mbps: 0.0,
                send_buf_pct: 0.0,
                sent_bytes: 0,
                retransmitted_pkts: 0,
                lost_pkts: 0,
                pkt_loss_rate: 0.0,
            }
        };

        // suppress unused-import warning when 'connected' is only used for rollup
        let _ = connected;

        // AWS reachability probe — read atomics written by `ping::run`.
        let ping_last_rtt_us = state.ping_last_rtt_us.load(Ordering::Relaxed);
        let ping_last_succ_us = state.ping_last_success_us.load(Ordering::Relaxed);
        let ping_last_rtt_ms = if ping_last_rtt_us == 0 {
            None
        } else {
            Some(ping_last_rtt_us as f32 / 1000.0)
        };
        if let Some(rtt) = ping_last_rtt_ms {
            ping_rtt_ms_ewma = ewma(ping_rtt_ms_ewma, rtt, ALPHA);
        }
        let ping = PingStats {
            target: cfg.ping_target.clone(),
            last_rtt_ms: ping_last_rtt_ms,
            ewma_rtt_ms: ping_rtt_ms_ewma,
            success_count: state.ping_success_total.load(Ordering::Relaxed),
            failure_count: state.ping_failure_total.load(Ordering::Relaxed),
            last_success_age_s: if ping_last_succ_us == 0 {
                None
            } else {
                Some((now.saturating_sub(ping_last_succ_us) as f32) / 1_000_000.0)
            },
        };

        let preview = PreviewStats {
            clients: state.preview_clients.load(Ordering::Relaxed),
            sent_frames: state.preview_sent_frames.load(Ordering::Relaxed),
            sent_bytes: state.preview_sent_bytes.load(Ordering::Relaxed),
            dropped: state.preview_dropped.load(Ordering::Relaxed),
        };

        let downlink = DownlinkStats {
            connected,
            bytes_in: sample.bytes_in,
            frames_in: sample.frames_in,
            bitrate_kbps: down_bitrate_kbps_ewma,
            last_frame_age_ms,
            session_uptime_s,
        };

        let dynamic_target = state.adapt_target_kbps.load(Ordering::Relaxed);
        let encoder = EncoderStats {
            codec: cfg.encoder_codec_label.clone(),
            target_kbps: if dynamic_target == 0 {
                cfg.target_bitrate_kbps
            } else {
                dynamic_target
            },
            nominal_kbps: cfg.target_bitrate_kbps,
            actual_kbps: enc_bitrate_kbps_ewma,
            frames_out: sample.enc_frames,
            adapt_enabled: cfg.adapt_enabled,
            min_kbps: cfg.adapt_min_kbps,
            max_kbps: cfg.adapt_max_kbps,
            step_downs: state.adapt_step_downs.load(Ordering::Relaxed),
            step_ups: state.adapt_step_ups.load(Ordering::Relaxed),
            quality: cfg.quality,
            override_kbps: state.adapt_override_kbps.load(Ordering::Relaxed),
        };

        let rollup = compute_rollup(connected, last_frame_age_ms, &uplink);

        let snapshot = Snapshot {
            ts_unix_ms: now_unix_ms,
            downlink,
            encoder,
            preview,
            uplink,
            ping,
            pipeline: PipelineHealth {
                rollup,
                uptime_s: (now as f32) / 1_000_000.0,
                restarts: state.restarts.load(Ordering::Relaxed),
            },
        };

        // It's fine if no receivers — `send` returns Err only when no
        // receivers exist, which is benign.
        let _ = tx.send(snapshot);
        prev = sample;
    }
}

#[derive(Default)]
struct Sample {
    ts_us: u64,
    bytes_in: u64,
    frames_in: u64,
    enc_bytes: u64,
    enc_frames: u64,
}

fn bits_per_kbps(byte_delta: u64, dt_s: f32) -> f32 {
    (byte_delta as f32 * 8.0) / (dt_s * 1000.0)
}

fn ewma(prev: f32, sample: f32, alpha: f32) -> f32 {
    if prev == 0.0 {
        sample
    } else {
        prev * alpha + sample * (1.0 - alpha)
    }
}

fn compute_rollup(
    publisher_connected: bool,
    last_frame_age_ms: Option<u32>,
    uplink: &UplinkStats,
) -> HealthRollup {
    if !publisher_connected {
        return HealthRollup::Warn;
    }
    if let Some(age) = last_frame_age_ms {
        if age > 2000 {
            return HealthRollup::Bad;
        }
        if age > 500 {
            return HealthRollup::Warn;
        }
    } else {
        return HealthRollup::Warn;
    }
    if uplink.pkt_loss_rate > 0.05 {
        return HealthRollup::Bad;
    }
    if uplink.pkt_loss_rate > 0.005 {
        return HealthRollup::Warn;
    }
    HealthRollup::Ok
}

// ────────────────────────────────────────────────────────────────────────────
// SRT element stats polling
// ────────────────────────────────────────────────────────────────────────────

struct RawSrtStats {
    rtt_ms: f32,
    bandwidth_mbps: f32,
    send_buf_pct: f32,
    sent_bytes: u64,
    retransmitted_pkts: u64,
    lost_pkts: u64,
    pkt_loss_rate: f32,
}

fn poll_srt_stats(state: &StatsState) -> Option<RawSrtStats> {
    let guard = state.srtsink.lock().ok()?;
    let sink = guard.as_ref()?;
    // srtsink always exposes a "stats" property; bail safely if a future
    // version ever changes that.
    sink.find_property("stats")?;
    let stats = sink.property::<gstreamer::Structure>("stats");
    let s = stats.as_ref();

    // Field names are stable across recent srtsink versions (see GStreamer
    // docs for `srtsink`'s `stats` GstStructure). Missing fields fall back
    // to zero so older runtimes still surface a reasonable view.
    let rtt_ms = s.get::<f64>("rtt-ms").unwrap_or(0.0) as f32;
    let bandwidth_mbps = s.get::<f64>("bandwidth-mbps").unwrap_or(0.0) as f32;
    let sent_bytes = s.get::<u64>("bytes-sent").unwrap_or(0);
    let retransmitted_pkts = s.get::<u64>("packets-retransmitted").unwrap_or(0);
    let lost_pkts = s.get::<u64>("packets-sent-lost").unwrap_or(0);
    let sent_pkts = s.get::<u64>("packets-sent").unwrap_or(0);
    let pkt_loss_rate = if sent_pkts == 0 {
        0.0
    } else {
        lost_pkts as f32 / sent_pkts as f32
    };

    Some(RawSrtStats {
        rtt_ms,
        bandwidth_mbps,
        send_buf_pct: 0.0, // not exposed as a normalised value; left for later
        sent_bytes,
        retransmitted_pkts,
        lost_pkts,
        pkt_loss_rate,
    })
}
