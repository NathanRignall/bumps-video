//! GStreamer pipeline lifecycle + SRT reconnect.
//!
//! Each publisher session gets a fresh GStreamer pipeline. If the SRT receiver
//! goes away mid-stream, the bus watch raises an error, the pipeline is torn
//! down, and a reconnect attempt is scheduled with exponential backoff. The
//! cached FLV header + AVC sequence-header tag is re-pushed into the new
//! pipeline's `appsrc` so flvdemux can parse the resumed bytestream.

mod build;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::prelude::*;
use gstreamer_app::AppSrc;
use tokio::sync::{broadcast, mpsc, watch};

use crate::capture::{CaptureCfg, CaptureEventReq, CaptureSession, ConfigSnapshot};
use crate::rtmp::IngestEvent;
use crate::stats::{Snapshot, StatsState, UplinkState};
use crate::wsproto::{FrameChunk, InitInfo};

#[derive(Clone)]
pub struct Config {
    pub srt_uri: String,
    pub bitrate_kbps: u32,
    /// Encoder VBV ceiling. The encoder is told it must never burst above
    /// this. Should match the adapter's `max_kbps` so the two agree.
    pub max_bitrate_kbps: u32,
    pub gop_size: u32,
    pub encoder: EncoderKind,
    /// Quality target 0.0–1.0. See [`build::build_encoder`] for per-encoder
    /// interpretation.
    pub quality: f32,
    /// If `Some`, the encoded bitstream is also tapped via a `tee + appsink`
    /// and fed into these channels for the browser preview.
    pub preview: Option<PreviewSinks>,
    /// Shared stats counters; pipeline updates `enc_*` via a pad probe and
    /// publishes `srtsink` here so the collector can poll its stats property.
    pub stats: Arc<StatsState>,
    /// If `Some`, each publisher session writes a debug artifact directory
    /// (`metadata.json` + `snapshot.jsonl` + `events.jsonl`) under
    /// `data_dir/sessions/<id>/`.
    pub capture: Option<CaptureCfg>,
    /// Read side of the stats watch channel — handed to each `CaptureSession`
    /// so its snapshot writer can mirror the live feed to disk.
    pub stats_rx: tokio::sync::watch::Receiver<Snapshot>,
    /// Sender for pipeline errors — passed to each bus watch. The pipeline
    /// task itself drains the matching receiver.
    pub pipeline_error_tx: mpsc::Sender<PipelineError>,
    /// Sender for capture events — bus watch posts `pipeline_error` events.
    pub capture_event_tx: mpsc::Sender<CaptureEventReq>,
}

#[derive(Clone)]
pub struct PreviewSinks {
    pub init_tx: watch::Sender<Option<InitInfo>>,
    pub frame_tx: broadcast::Sender<FrameChunk>,
}

/// Pipeline-level error surfaced by the bus watch.
///
/// srtsink errors are deliberately *not* surfaced here — that element
/// auto-reconnects internally and tearing down the whole pipeline on each
/// of its transient errors creates a connect/disconnect feedback loop with
/// the receiver. See `spawn_bus_watch` for the handling path.
#[derive(Debug, Clone)]
pub enum PipelineError {
    /// A non-srtsink element errored. The current pipeline instance is
    /// rebuilt by the reconnect machinery in [`run`].
    Other { src: String, message: String },
}

/// Operator-initiated commands. Posted by the web WS handler; drained by the
/// pipeline task and applied to live state.
#[derive(Debug, Clone)]
pub enum PipelineCommand {
    /// Tear down the current pipeline immediately and trigger a rebuild on
    /// the next reconnect tick.
    Restart,
}

/// Which video encoder element to instantiate.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum EncoderKind {
    QsvHevc,
    VtHevc,
    X264,
    /// Intel QSV AV1 (`qsvav1enc`). Hardware AV1 encode on Core Ultra and
    /// newer Intel iGPUs. ~30 % better compression at the same quality as
    /// HEVC, at the cost of higher per-frame encode latency.
    QsvAv1,
    /// VA-API HEVC (`vah265enc`) from gst-plugins-bad's `va` plugin. Same
    /// Intel iGPU silicon as QSV, accessed via libva instead of oneVPL.
    /// Use this when the gst-plugins-bad build doesn't include the QSV
    /// plugin (common on NixOS / nixpkgs default builds).
    VaHevc,
    /// VA-API AV1 (`vaav1enc`). Hardware AV1 encode via libva. Core Ultra
    /// or newer Intel iGPUs.
    VaAv1,
}

impl Default for EncoderKind {
    fn default() -> Self {
        if cfg!(target_os = "macos") {
            Self::VtHevc
        } else {
            Self::QsvHevc
        }
    }
}

impl std::fmt::Display for EncoderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QsvHevc => f.write_str("qsv-hevc"),
            Self::VtHevc => f.write_str("vt-hevc"),
            Self::X264 => f.write_str("x264"),
            Self::QsvAv1 => f.write_str("qsv-av1"),
            Self::VaHevc => f.write_str("va-hevc"),
            Self::VaAv1 => f.write_str("va-av1"),
        }
    }
}

const WATCHDOG_TICK: Duration = Duration::from_millis(500);
const NO_INPUT_THRESHOLD: Duration = Duration::from_secs(5);

/// Long-lived task: owns the GStreamer pipeline lifecycle for the current
/// publisher session, including SRT reconnect with exponential backoff.
pub async fn run(
    mut events: mpsc::Receiver<IngestEvent>,
    mut capture_events: mpsc::Receiver<CaptureEventReq>,
    mut pipeline_errors: mpsc::Receiver<PipelineError>,
    mut commands: mpsc::Receiver<PipelineCommand>,
    cfg: Config,
) -> Result<()> {
    let mut session: Option<ActiveSession> = None;
    let mut capture: Option<CaptureSession> = None;

    // FLV state we need to re-push on a reconnect rebuild.
    let mut cached_seq_header_tag: Option<Bytes> = None;
    // SRT reconnect bookkeeping.
    let mut srt_lost_at: Option<Instant> = None;
    let mut srt_attempt: u32 = 0;
    // Watchdog stale-input bookkeeping (de-dupes event emission).
    let mut input_stale_emitted = false;

    let mut watchdog = tokio::time::interval(WATCHDOG_TICK);
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;

            // 1. Forward queued capture events to the active session.
            req = capture_events.recv() => {
                let Some(req) = req else { break; };
                if let Some(cs) = capture.as_mut() {
                    cs.emit_event(&req.kind, req.details).await;
                }
            }

            // 2. Pipeline errors from the bus watch.
            err = pipeline_errors.recv() => {
                let Some(err) = err else { break; };
                handle_pipeline_error(
                    err,
                    &cfg,
                    &mut session,
                    &mut capture,
                    &mut srt_lost_at,
                    &mut srt_attempt,
                ).await;
            }

            // 2b. Operator commands from the web/WS layer.
            cmd = commands.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    PipelineCommand::Restart => {
                        tracing::info!("operator: restart_pipeline");
                        if let Some(prev) = session.take() {
                            prev.teardown();
                        }
                        if let Some(cs) = capture.as_mut() {
                            cs.emit_event(
                                "operator_restart",
                                serde_json::json!({}),
                            ).await;
                        }
                        cfg.stats.set_uplink_state(UplinkState::Lost);
                        srt_lost_at = Some(Instant::now() - Duration::from_secs(60));
                        srt_attempt = 0;
                    }
                }
            }

            // 3. Periodic timer for watchdog + reconnect attempts.
            _ = watchdog.tick() => {
                tick_watchdog(&cfg, &mut capture, &mut input_stale_emitted).await;
                tick_reconnect(
                    &cfg,
                    &mut session,
                    &mut capture,
                    &cached_seq_header_tag,
                    &mut srt_lost_at,
                    &mut srt_attempt,
                ).await;
            }

            // 4. Ingest events from the RTMP server.
            ev = events.recv() => {
                let Some(ev) = ev else { break; };
                match ev {
                    IngestEvent::SessionStarted { peer, app, stream_key } => {
                        on_session_started(
                            &cfg,
                            &mut session,
                            &mut capture,
                            &mut cached_seq_header_tag,
                            &mut srt_lost_at,
                            &mut srt_attempt,
                            &mut input_stale_emitted,
                            peer, app, stream_key,
                        ).await;
                    }
                    IngestEvent::FlvChunk(bytes) => {
                        if is_avc_sequence_header_tag(&bytes) {
                            tracing::debug!(len = bytes.len(), "cached AVC sequence header tag");
                            cached_seq_header_tag = Some(bytes.clone());
                        }
                        let Some(s) = session.as_mut() else {
                            tracing::trace!("flv chunk before session start / during reconnect; dropping {} bytes", bytes.len());
                            continue;
                        };
                        if let Err(e) = s.push_flv(bytes) {
                            tracing::error!(error = ?e, "push_flv failed");
                            if let Some(cs) = capture.as_mut() {
                                cs.emit_event(
                                    "pipeline_error",
                                    serde_json::json!({ "where": "push_flv", "error": format!("{e:?}") }),
                                ).await;
                            }
                        }
                    }
                    IngestEvent::SessionEnded => {
                        tracing::info!("pipeline: session end");
                        cached_seq_header_tag = None;
                        srt_lost_at = None;
                        srt_attempt = 0;
                        input_stale_emitted = false;
                        cfg.stats.set_uplink_state(UplinkState::Idle);
                        if let Some(prev) = session.take() {
                            prev.teardown();
                        }
                        if let Some(preview) = &cfg.preview {
                            let _ = preview.init_tx.send(None);
                        }
                        if let Some(mut cs) = capture.take() {
                            cs.emit_event("session_ended", serde_json::json!({})).await;
                            cs.close("publisher_disconnect").await;
                        }
                    }
                }
            }
        }
    }

    if let Some(prev) = session.take() {
        prev.teardown();
    }
    if let Some(preview) = &cfg.preview {
        let _ = preview.init_tx.send(None);
    }
    if let Some(mut cs) = capture.take() {
        cs.emit_event("process_shutdown", serde_json::json!({})).await;
        cs.close("process_shutdown").await;
    }
    cfg.stats.set_uplink_state(UplinkState::Idle);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn on_session_started(
    cfg: &Config,
    session: &mut Option<ActiveSession>,
    capture: &mut Option<CaptureSession>,
    cached_seq_header_tag: &mut Option<Bytes>,
    srt_lost_at: &mut Option<Instant>,
    srt_attempt: &mut u32,
    input_stale_emitted: &mut bool,
    peer: std::net::SocketAddr,
    app: String,
    stream_key: String,
) {
    tracing::info!(%peer, %app, %stream_key, "pipeline: session start");
    *cached_seq_header_tag = None;
    *srt_lost_at = None;
    *srt_attempt = 0;
    *input_stale_emitted = false;

    if let Some(prev) = session.take() {
        tracing::warn!("session start without prior session end; tearing down prior");
        prev.teardown();
    }
    if let Some(prev) = capture.take() {
        prev.close("restart").await;
    }

    cfg.stats.set_uplink_state(UplinkState::Connecting);
    let pipeline_ok = match ActiveSession::new(cfg) {
        Ok(s) => {
            *session = Some(s);
            true
        }
        Err(e) => {
            tracing::error!(error = ?e, "pipeline build failed; dropping session");
            cfg.stats.set_uplink_state(UplinkState::Lost);
            *srt_lost_at = Some(Instant::now());
            *srt_attempt = 0;
            false
        }
    };

    if let Some(cap_cfg) = &cfg.capture {
        match CaptureSession::open(
            cap_cfg,
            stream_key.clone(),
            format!("{peer}"),
            config_snapshot(cfg),
            cfg.stats_rx.clone(),
        )
        .await
        {
            Ok(mut cs) => {
                cs.emit_event(
                    "session_started",
                    serde_json::json!({
                        "app": app,
                        "stream_key": stream_key,
                        "peer": format!("{peer}"),
                        "pipeline_built": pipeline_ok,
                    }),
                )
                .await;
                *capture = Some(cs);
            }
            Err(e) => tracing::warn!(?e, "capture: failed to open session dir"),
        }
    }
}

async fn handle_pipeline_error(
    err: PipelineError,
    cfg: &Config,
    session: &mut Option<ActiveSession>,
    capture: &mut Option<CaptureSession>,
    srt_lost_at: &mut Option<Instant>,
    srt_attempt: &mut u32,
) {
    let state = cfg.stats.get_uplink_state();
    if state == UplinkState::Lost {
        // Already in reconnect mode; ignore further error noise from the
        // dying pipeline.
        return;
    }

    let PipelineError::Other { src, message: msg } = &err;
    let kind = "pipeline_error";
    let message = format!("{src}: {msg}");
    tracing::warn!(%message, "pipeline: {kind}; tearing down and scheduling reconnect");

    if let Some(prev) = session.take() {
        prev.teardown();
    }
    if let Some(cs) = capture.as_mut() {
        cs.emit_event(
            kind,
            serde_json::json!({ "message": message }),
        )
        .await;
    }

    cfg.stats.set_uplink_state(UplinkState::Lost);
    *srt_lost_at = Some(Instant::now());
    *srt_attempt = 0;
}

async fn tick_reconnect(
    cfg: &Config,
    session: &mut Option<ActiveSession>,
    capture: &mut Option<CaptureSession>,
    cached_seq_header_tag: &Option<Bytes>,
    srt_lost_at: &mut Option<Instant>,
    srt_attempt: &mut u32,
) {
    use std::sync::atomic::Ordering;

    let Some(lost_at) = *srt_lost_at else { return };
    if !cfg.stats.publisher_connected.load(Ordering::Relaxed) {
        // Don't keep retrying if there's no publisher to receive bytes from.
        return;
    }
    let wait = backoff(*srt_attempt);
    if lost_at.elapsed() < wait {
        return;
    }

    *srt_attempt += 1;
    cfg.stats.set_uplink_state(UplinkState::Connecting);
    cfg.stats
        .srt_reconnect_attempts
        .fetch_add(1, Ordering::Relaxed);
    tracing::info!(attempt = *srt_attempt, waited_s = wait.as_secs(), "pipeline: reconnect attempt");

    match ActiveSession::new(cfg) {
        Ok(s) => {
            let mut new_session = s;
            // Replay cached FLV state so flvdemux can resume parsing.
            if let Err(e) = new_session.push_flv(crate::rtmp::flv_header_bytes()) {
                tracing::warn!(?e, "reconnect: push FLV header failed");
            }
            if let Some(tag) = cached_seq_header_tag.clone() {
                if let Err(e) = new_session.push_flv(tag) {
                    tracing::warn!(?e, "reconnect: push AVC seq header failed");
                }
            }
            *session = Some(new_session);
            *srt_lost_at = None;
            cfg.stats.srt_recoveries.fetch_add(1, Ordering::Relaxed);
            // State will transition to Connected when bus reports PLAYING.
            if let Some(cs) = capture.as_mut() {
                cs.emit_event(
                    "srt_reconnected",
                    serde_json::json!({ "attempt": *srt_attempt }),
                )
                .await;
            }
        }
        Err(e) => {
            tracing::warn!(error = ?e, attempt = *srt_attempt, "reconnect rebuild failed; backing off");
            // Reset clock for next backoff window.
            *srt_lost_at = Some(Instant::now());
            cfg.stats.set_uplink_state(UplinkState::Lost);
        }
    }
}

async fn tick_watchdog(
    cfg: &Config,
    capture: &mut Option<CaptureSession>,
    stale_emitted: &mut bool,
) {
    use std::sync::atomic::Ordering;

    // ── 1. SRT warning decay: clear Lost state once warnings have stopped ──
    // srtsink emits "Socket is broken or closed" warnings while it auto-
    // reconnects internally. We use that to flip uplink state to Lost. Once
    // warnings stop arriving for `SRT_WARN_DECAY` we assume the link is
    // healthy again and flip back to Connected.
    const SRT_WARN_DECAY: Duration = Duration::from_millis(3500);
    let warning_at = cfg.stats.srt_warning_at_us.load(Ordering::Relaxed);
    if warning_at > 0 {
        let age_us = cfg.stats.now_us().saturating_sub(warning_at);
        if cfg.stats.get_uplink_state() == UplinkState::Lost
            && age_us > SRT_WARN_DECAY.as_micros() as u64
        {
            cfg.stats.set_uplink_state(UplinkState::Connected);
            cfg.stats.srt_warning_at_us.store(0, Ordering::Relaxed);
            cfg.stats.srt_recoveries.fetch_add(1, Ordering::Relaxed);
            if let Some(cs) = capture.as_mut() {
                cs.emit_event(
                    "srt_recovered",
                    serde_json::json!({ "downtime_ms": age_us / 1000 }),
                )
                .await;
            }
            tracing::info!(downtime_ms = age_us / 1000, "watchdog: srt recovered");
        }
    }

    // ── 2. No-input stale detection ────────────────────────────────────────
    let connected = cfg.stats.publisher_connected.load(Ordering::Relaxed);
    if !connected {
        *stale_emitted = false;
        return;
    }
    let last_frame_us = cfg.stats.last_frame_us.load(Ordering::Relaxed);
    if last_frame_us == 0 {
        return;
    }
    let age = cfg.stats.now_us().saturating_sub(last_frame_us);
    let stale = age >= NO_INPUT_THRESHOLD.as_micros() as u64;

    if stale && !*stale_emitted {
        *stale_emitted = true;
        if let Some(cs) = capture.as_mut() {
            cs.emit_event(
                "downlink_stale",
                serde_json::json!({ "frame_age_ms": age / 1000 }),
            )
            .await;
        }
        tracing::warn!(frame_age_ms = age / 1000, "watchdog: downlink stale");
    } else if !stale && *stale_emitted {
        *stale_emitted = false;
        if let Some(cs) = capture.as_mut() {
            cs.emit_event("downlink_resumed", serde_json::json!({})).await;
        }
        tracing::info!("watchdog: downlink resumed");
    }
}

/// Detect whether an FLV tag chunk is the AVC sequence header video tag.
/// We cache this so a post-reconnect pipeline can parse the bytestream
/// without waiting for the publisher to send another (it normally doesn't).
fn is_avc_sequence_header_tag(chunk: &Bytes) -> bool {
    if chunk.len() < 13 {
        return false;
    }
    if chunk[0] != 9 {
        return false; // not a video tag
    }
    let frame_type = chunk[11] >> 4;
    let codec_id = chunk[11] & 0x0F;
    if frame_type != 1 || codec_id != 7 {
        return false; // not an AVC keyframe-shaped tag
    }
    chunk[12] == 0 // AVCPacketType == 0 (sequence header)
}

/// Exponential backoff for SRT reconnect attempts. After ~30s of failures
/// we settle at 15s pings indefinitely.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(match attempt {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        _ => 15,
    })
}

fn config_snapshot(cfg: &Config) -> ConfigSnapshot {
    ConfigSnapshot {
        encoder: format!("{}", cfg.encoder),
        bitrate_kbps: cfg.bitrate_kbps,
        gop_size: cfg.gop_size,
        quality: cfg.quality,
        srt_uri: cfg.srt_uri.clone(),
    }
}

struct ActiveSession {
    pipeline: gstreamer::Pipeline,
    appsrc: AppSrc,
    stats: Arc<StatsState>,
    /// Bus watch keeps a handle on the GLib main context. Drop tears it down.
    _bus_task: tokio::task::JoinHandle<()>,
}

impl ActiveSession {
    fn new(cfg: &Config) -> Result<Self> {
        let built = build::build_pipeline(cfg).context("build pipeline")?;
        let bus_task = spawn_bus_watch(
            &built.pipeline,
            cfg.pipeline_error_tx.clone(),
            cfg.capture_event_tx.clone(),
            cfg.stats.clone(),
        );

        // Publish srtsink + encoders to the stats state so the collector can
        // poll srtsink, the adapter can write to the uplink encoder, and the
        // WS handler can force IDRs on the preview encoder.
        {
            let mut g = cfg.stats.srtsink.lock().expect("srtsink mutex");
            *g = Some(built.srtsink.clone());
        }
        {
            let mut g = cfg.stats.encoder.lock().expect("encoder mutex");
            *g = Some(built.encoder.clone());
        }
        {
            let mut g = cfg
                .stats
                .preview_encoder
                .lock()
                .expect("preview_encoder mutex");
            *g = built.preview_encoder.clone();
        }

        built
            .pipeline
            .set_state(gstreamer::State::Playing)
            .context("set PLAYING")?;
        tracing::info!("pipeline: PLAYING");

        Ok(Self {
            pipeline: built.pipeline,
            appsrc: built.appsrc,
            stats: cfg.stats.clone(),
            _bus_task: bus_task,
        })
    }

    fn push_flv(&mut self, bytes: bytes::Bytes) -> Result<()> {
        tracing::trace!(len = bytes.len(), "push flv chunk");
        let mut buf = gstreamer::Buffer::with_size(bytes.len()).context("alloc buffer")?;
        {
            let buf_mut = buf.get_mut().ok_or_else(|| anyhow!("buffer not writable"))?;
            let mut map = buf_mut.map_writable().context("map_writable")?;
            map.copy_from_slice(&bytes);
        }
        let flow = self.appsrc.push_buffer(buf);
        tracing::trace!(?flow, "push_buffer returned");
        match flow {
            Ok(_) => Ok(()),
            Err(gstreamer::FlowError::Flushing) => {
                tracing::trace!("appsrc flushing");
                Ok(())
            }
            Err(other) => Err(anyhow!("appsrc push_buffer: {other:?}")),
        }
    }

    fn teardown(self) {
        let _ = self.appsrc.end_of_stream();
        let _ = self.pipeline.set_state(gstreamer::State::Null);
        *self.stats.srtsink.lock().expect("srtsink mutex") = None;
        *self.stats.encoder.lock().expect("encoder mutex") = None;
        *self
            .stats
            .preview_encoder
            .lock()
            .expect("preview_encoder mutex") = None;
    }
}

fn spawn_bus_watch(
    pipeline: &gstreamer::Pipeline,
    error_tx: mpsc::Sender<PipelineError>,
    capture_event_tx: mpsc::Sender<CaptureEventReq>,
    stats: Arc<StatsState>,
) -> tokio::task::JoinHandle<()> {
    let bus = pipeline.bus().expect("pipeline has bus");
    let weak = pipeline.downgrade();
    tokio::task::spawn_blocking(move || {
        use gstreamer::MessageView;
        loop {
            let Some(msg) = bus.timed_pop(gstreamer::ClockTime::from_mseconds(250)) else {
                if weak.upgrade().is_none() {
                    return;
                }
                continue;
            };
            match msg.view() {
                MessageView::Eos(_) => {
                    tracing::info!("pipeline: EOS");
                }
                MessageView::Error(err) => {
                    let src_name = err
                        .src()
                        .map(|s| s.name().to_string())
                        .unwrap_or_default();
                    let err_msg = err.error().to_string();
                    tracing::error!(
                        src = %src_name,
                        error = %err_msg,
                        debug = ?err.debug(),
                        "pipeline error"
                    );
                    let _ = capture_event_tx.try_send(CaptureEventReq {
                        kind: "pipeline_error".into(),
                        details: serde_json::json!({
                            "src": src_name,
                            "error": err_msg.clone(),
                        }),
                    });

                    if src_name == "uplink" {
                        // srtsink is responsible for its own reconnection.
                        // The element emits transient bus Errors when the
                        // SRT session wobbles — tearing down + rebuilding
                        // the whole pipeline in response creates a fresh
                        // SRT handshake every time, which MediaConnect
                        // registers as a brand-new "source connected"
                        // event. The result is a connect/disconnect cycle
                        // every few seconds with no actual recovery.
                        //
                        // Just mark the dashboard as Lost. The bus
                        // Warning watchdog further down already clears
                        // that state once srtsink stops complaining for
                        // SRT_WARN_DECAY (3.5 s).
                        use std::sync::atomic::Ordering;
                        stats
                            .srt_warning_at_us
                            .store(stats.now_us(), Ordering::Relaxed);
                        if stats.get_uplink_state() == UplinkState::Connected {
                            stats.set_uplink_state(UplinkState::Lost);
                        }
                    } else {
                        // Non-srtsink errors are unrecoverable for the
                        // current pipeline instance; route to the
                        // reconnect machinery for a clean rebuild.
                        let _ = error_tx.try_send(PipelineError::Other {
                            src: src_name.clone(),
                            message: err_msg.clone(),
                        });
                    }
                }
                MessageView::Warning(w) => {
                    let src_name = w.src().map(|s| s.name().to_string()).unwrap_or_default();
                    let warn_msg = w.error().to_string();
                    tracing::warn!(
                        src = %src_name,
                        warning = %warn_msg,
                        "pipeline warning"
                    );
                    // srtsink emits a `Socket is broken or closed. Trying to
                    // reconnect.` warning when its internal auto-reconnect
                    // kicks in. Mark the uplink as Lost so the dashboard
                    // reflects it; the watchdog will clear it once warnings
                    // stop arriving for a while.
                    if src_name == "uplink" {
                        use std::sync::atomic::Ordering;
                        stats
                            .srt_warning_at_us
                            .store(stats.now_us(), Ordering::Relaxed);
                        if stats.get_uplink_state() == UplinkState::Connected {
                            stats.set_uplink_state(UplinkState::Lost);
                            let _ = capture_event_tx.try_send(CaptureEventReq {
                                kind: "srt_warning".into(),
                                details: serde_json::json!({ "message": warn_msg }),
                            });
                        }
                    }
                }
                MessageView::StateChanged(s) => {
                    if s.src().map(|src| src.is::<gstreamer::Pipeline>()).unwrap_or(false) {
                        tracing::debug!(old = ?s.old(), new = ?s.current(), "pipeline state");
                        if s.current() == gstreamer::State::Playing {
                            stats.set_uplink_state(UplinkState::Connected);
                        }
                    }
                }
                _ => {}
            }
        }
    })
}
