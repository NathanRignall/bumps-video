//! Live stats: snapshot types, shared atomic state, and a collector task.
//!
//! Data flow per second:
//! - RTMP task, encoder pad probe, and preview appsink callback push into
//!   `Arc<StatsState>` (lock-free atomic increments).
//! - `collector::run` ticks at 1 Hz: reads atomics, polls `srtsink.stats`
//!   (under a mutex because the element handle gets swapped on session
//!   change), computes EWMAs, publishes a `Snapshot` on a watch channel.
//! - Web/WS handlers select on the watch channel and forward as JSON.

pub mod collector;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8};
use std::sync::Mutex;
use std::time::Instant;

use serde::Serialize;

/// Snapshot of pipeline + uplink + dashboard health. Sent to clients at 1 Hz.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub ts_unix_ms: u64,
    pub downlink: DownlinkStats,
    pub encoder: EncoderStats,
    pub preview: PreviewStats,
    pub uplink: UplinkStats,
    pub ping: PingStats,
    pub pipeline: PipelineHealth,
}

/// Reachability probe to a fixed `host:port` (AWS endpoint by default).
/// TCP-connect timing is used as a cheap stand-in for ICMP ping — measures
/// SYN→SYN-ACK round-trip plus DNS, which is what actually matters for the
/// real SRT relay path.
#[derive(Debug, Clone, Serialize)]
pub struct PingStats {
    pub target: String,
    /// Last individual measurement.
    pub last_rtt_ms: Option<f32>,
    /// EWMA over recent measurements.
    pub ewma_rtt_ms: f32,
    pub success_count: u64,
    pub failure_count: u64,
    pub last_success_age_s: Option<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DownlinkStats {
    pub connected: bool,
    pub bytes_in: u64,
    pub frames_in: u64,
    pub bitrate_kbps: f32,
    pub last_frame_age_ms: Option<u32>,
    pub session_uptime_s: Option<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EncoderStats {
    pub codec: String,
    /// Currently effective target bitrate. Equal to `nominal_kbps` when
    /// adaptation is disabled or hasn't moved yet; otherwise reflects the
    /// adapter's latest decision.
    pub target_kbps: u32,
    /// The starting / nominal target set by `--bitrate-kbps`.
    pub nominal_kbps: u32,
    pub actual_kbps: f32,
    pub frames_out: u64,
    /// Whether the adapter is allowed to change `target_kbps`.
    pub adapt_enabled: bool,
    /// Floor / ceiling for the adapter, even when enabled.
    pub min_kbps: u32,
    pub max_kbps: u32,
    pub step_downs: u64,
    pub step_ups: u64,
    /// Quality target 0.0–1.0. Configured at startup, doesn't change at
    /// runtime. Surfaced so the dashboard and metadata.json reflect it.
    pub quality: f32,
    /// Operator-set bitrate pin in kbps, 0 = no override. While non-zero
    /// the adapter is held off.
    pub override_kbps: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreviewStats {
    pub clients: u32,
    pub sent_frames: u64,
    pub sent_bytes: u64,
    pub dropped: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UplinkStats {
    pub state: UplinkState,
    pub rtt_ms: f32,
    /// SRT-reported send rate (delta of `sent_bytes` over the tick).
    pub send_kbps: f32,
    /// SRT's path-capacity estimate. On loopback this saturates to silly
    /// values; on Starlink it'll be the actual link's headroom.
    pub link_cap_mbps: f32,
    pub send_buf_pct: f32,
    pub sent_bytes: u64,
    pub retransmitted_pkts: u64,
    pub lost_pkts: u64,
    pub pkt_loss_rate: f32,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UplinkState {
    Idle,
    Connecting,
    Connected,
    Lost,
}

impl UplinkState {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Connecting => 1,
            Self::Connected => 2,
            Self::Lost => 3,
        }
    }
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Connecting,
            2 => Self::Connected,
            3 => Self::Lost,
            _ => Self::Idle,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineHealth {
    pub rollup: HealthRollup,
    pub uptime_s: f32,
    pub restarts: u32,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthRollup {
    Ok,
    Warn,
    Bad,
}

/// Shared mutable state. Written by data-path tasks (RTMP, encoder pad probe,
/// appsink callback, WS handler), read by `collector::run`.
pub struct StatsState {
    // ── downlink (RTMP from publisher) ─────────────────────────────────────
    pub publisher_connected: AtomicBool,
    pub bytes_in: AtomicU64,
    pub frames_in: AtomicU64,
    /// Microseconds since `process_started` of the most recent video tag.
    /// 0 means "no frame received yet this session".
    pub last_frame_us: AtomicU64,
    /// Microseconds since `process_started` of the current session's start.
    /// 0 means "no active session".
    pub session_started_us: AtomicU64,

    // ── encoder ────────────────────────────────────────────────────────────
    pub enc_bytes_out: AtomicU64,
    pub enc_frames_out: AtomicU64,

    // ── preview ────────────────────────────────────────────────────────────
    pub preview_clients: AtomicU32,
    pub preview_sent_frames: AtomicU64,
    pub preview_sent_bytes: AtomicU64,
    pub preview_dropped: AtomicU64,

    // ── pipeline meta ──────────────────────────────────────────────────────
    pub process_started: Instant,
    pub restarts: AtomicU32,

    // ── handles for property polls and property writes ─────────────────────
    /// Populated when a pipeline is built, cleared on teardown. The collector
    /// reads `srtsink.property::<gst::Structure>("stats")` while holding the
    /// lock — cheap because polled at 1 Hz only.
    pub srtsink: Mutex<Option<gstreamer::Element>>,
    /// Same lifecycle as `srtsink`. The adapter writes `bitrate` on it.
    pub encoder: Mutex<Option<gstreamer::Element>>,
    /// Same lifecycle as `srtsink`. The preview encoder is a separate
    /// downscaled H.264 encode dedicated to the browser; we need a handle
    /// on it to force IDRs when a WS client lags or first connects so the
    /// browser can resume decode without waiting for the next natural GOP.
    pub preview_encoder: Mutex<Option<gstreamer::Element>>,

    // ── adapter state ──────────────────────────────────────────────────────
    /// Current effective target bitrate in kbps. The adapter writes here on
    /// every step decision; the collector reads it into [`EncoderStats`].
    pub adapt_target_kbps: AtomicU32,
    pub adapt_step_downs: AtomicU64,
    pub adapt_step_ups: AtomicU64,
    /// Operator-set pin. Non-zero values disable adapter step-up/step-down
    /// and force the encoder to this rate. Zero = no override.
    pub adapt_override_kbps: AtomicU32,

    // ── uplink state machine ───────────────────────────────────────────────
    /// [`UplinkState`] encoded as a u8 for atomic access. Set by the pipeline
    /// task on session lifecycle and SRT failure; read by the collector.
    pub uplink_state: AtomicU8,
    pub srt_reconnect_attempts: AtomicU64,
    pub srt_recoveries: AtomicU64,
    /// Microseconds since `process_started` of the most recent srtsink warning
    /// (e.g. "Socket is broken or closed"). Used by the watchdog to clear the
    /// `Lost` state once warnings have stopped for a while.
    pub srt_warning_at_us: AtomicU64,

    // ── AWS reachability probe ─────────────────────────────────────────────
    /// Most recent successful TCP-connect RTT in microseconds. 0 = never.
    pub ping_last_rtt_us: AtomicU64,
    /// `now_us` at the time of the most recent success. 0 = never.
    pub ping_last_success_us: AtomicU64,
    pub ping_success_total: AtomicU64,
    pub ping_failure_total: AtomicU64,
}

impl StatsState {
    pub fn new() -> Self {
        Self {
            publisher_connected: AtomicBool::new(false),
            bytes_in: AtomicU64::new(0),
            frames_in: AtomicU64::new(0),
            last_frame_us: AtomicU64::new(0),
            session_started_us: AtomicU64::new(0),
            enc_bytes_out: AtomicU64::new(0),
            enc_frames_out: AtomicU64::new(0),
            preview_clients: AtomicU32::new(0),
            preview_sent_frames: AtomicU64::new(0),
            preview_sent_bytes: AtomicU64::new(0),
            preview_dropped: AtomicU64::new(0),
            process_started: Instant::now(),
            restarts: AtomicU32::new(0),
            srtsink: Mutex::new(None),
            encoder: Mutex::new(None),
            preview_encoder: Mutex::new(None),
            adapt_target_kbps: AtomicU32::new(0),
            adapt_step_downs: AtomicU64::new(0),
            adapt_step_ups: AtomicU64::new(0),
            adapt_override_kbps: AtomicU32::new(0),
            uplink_state: AtomicU8::new(UplinkState::Idle.as_u8()),
            srt_reconnect_attempts: AtomicU64::new(0),
            srt_recoveries: AtomicU64::new(0),
            srt_warning_at_us: AtomicU64::new(0),
            ping_last_rtt_us: AtomicU64::new(0),
            ping_last_success_us: AtomicU64::new(0),
            ping_success_total: AtomicU64::new(0),
            ping_failure_total: AtomicU64::new(0),
        }
    }

    /// Microseconds since `process_started`. Cheap monotonic clock.
    pub fn now_us(&self) -> u64 {
        self.process_started.elapsed().as_micros() as u64
    }

    pub fn set_uplink_state(&self, s: UplinkState) {
        use std::sync::atomic::Ordering;
        self.uplink_state.store(s.as_u8(), Ordering::Relaxed);
    }

    pub fn get_uplink_state(&self) -> UplinkState {
        use std::sync::atomic::Ordering;
        UplinkState::from_u8(self.uplink_state.load(Ordering::Relaxed))
    }

    /// Ask the current uplink encoder to emit an IDR keyframe on the next
    /// picture, with VPS/SPS/PPS headers attached so the SRT receiver can
    /// resync from it. No-op when there's no active pipeline.
    ///
    /// All of our encoders respond to the GStreamer upstream
    /// `force-key-unit` event.
    pub fn request_keyframe(&self) {
        force_key_unit(&self.encoder);
    }

    /// Same as [`Self::request_keyframe`] but targets the preview encoder
    /// — used to recover a freshly-connected browser tab without disturbing
    /// the uplink encoder's GOP cadence.
    pub fn request_preview_keyframe(&self) {
        force_key_unit(&self.preview_encoder);
    }
}

/// Send an upstream `force-key-unit` event into whichever encoder is
/// currently parked in the given mutex slot. No-op when slot is empty.
fn force_key_unit(slot: &Mutex<Option<gstreamer::Element>>) {
    use gstreamer::prelude::*;
    let Ok(guard) = slot.lock() else { return };
    let Some(enc) = guard.as_ref() else { return };
    let Some(pad) = enc.static_pad("sink") else { return };
    let event = gstreamer_video::UpstreamForceKeyUnitEvent::builder()
        .all_headers(true)
        .build();
    pad.send_event(event);
}
