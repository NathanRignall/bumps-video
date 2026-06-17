use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

mod adapt;
mod capture;
mod ping;
mod pipeline;
mod rtmp;
mod stats;
mod web;
mod wsproto;

use pipeline::EncoderKind;

#[derive(Parser, Debug, Clone)]
#[command(name = "bumps-pipe", version, about = "RTMP → flatten → encode → SRT")]
struct Args {
    /// RTMP listen address (the phone publishes here)
    #[arg(long, default_value = "0.0.0.0:1935", env = "BUMPS_RTMP_LISTEN")]
    rtmp_listen: String,

    /// SRT output URI. Pass `srt://host:port` and stability defaults are
    /// appended automatically (8 s latency, 100 % retransmit overhead,
    /// `maxbw` = 3 × bitrate, 25 MB socket buffers, 30 s peer-idle).
    ///
    /// If you include any `?param=value` of your own, the URI is used
    /// verbatim — nothing is appended. Use that when talking to a
    /// receiver that needs specific values.
    #[arg(
        long,
        default_value = "srt://127.0.0.1:9999",
        env = "BUMPS_SRT_URI"
    )]
    srt_uri: String,

    /// Encoder target bitrate in kbps. The default SRT URI's `maxbw` is
    /// derived from this (3 × bitrate) so the encoder and the path never
    /// disagree on the peak allowed rate.
    #[arg(long, default_value_t = 8000, env = "BUMPS_BITRATE_KBPS")]
    bitrate_kbps: u32,

    /// Encoder GOP size in frames (≈ 1s at 30fps when set to 30). Short
    /// GOPs mean a lost keyframe causes at most one second of decode
    /// failure on the receiver instead of two; for a Starlink path with
    /// frequent handovers this is worth the few-percent bitrate overhead.
    #[arg(long, default_value_t = 30, env = "BUMPS_GOP_SIZE")]
    gop_size: u32,

    /// Quality target on a 0.0–1.0 scale. Maps per encoder:
    /// - `vt-hevc`  → sets the VideoToolbox `quality` property directly
    /// - `qsv-hevc` → `target-usage` (1=highest quality, 7=highest speed)
    /// - `x264`     → `speed-preset` (slower preset = better quality)
    ///
    /// Higher = better image at the same bitrate target, more CPU/iGPU cost
    /// and slightly more encode latency. 0.6 (default) is the stability
    /// sweet spot: the encoder always meets its frame deadlines so the
    /// pipeline never stalls. Bump to 0.8+ only when you've confirmed the
    /// iGPU has headroom to spare.
    #[arg(long, default_value_t = 0.6, env = "BUMPS_QUALITY")]
    quality: f32,

    /// Which video encoder to use.
    ///
    /// - `qsv-hevc`: Intel QSV HEVC, the production target (Linux + Intel iGPU)
    /// - `vt-hevc` : Apple VideoToolbox HEVC, for Mac dev work
    /// - `x264`    : libx264 software H.264, portable fallback
    #[arg(long, value_enum, default_value_t = EncoderKind::default(), env = "BUMPS_ENCODER")]
    encoder: EncoderKind,

    /// Web dashboard listen address (loopback only by default).
    #[arg(long, default_value = "127.0.0.1:8080", env = "BUMPS_WEB_LISTEN")]
    web_listen: SocketAddr,

    /// Where session artifacts are written (one subdir per publisher session).
    /// Default: $BUMPS_DATA_DIR ∥ $HOME/.local/share/bumps-pipe ∥ ./bumps-data.
    #[arg(long, env = "BUMPS_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Disable per-session capture (no metadata.json / snapshot.jsonl / events.jsonl).
    #[arg(long, env = "BUMPS_NO_CAPTURE", default_value_t = false)]
    no_capture: bool,

    /// Keep at most this many recent session directories; older ones are
    /// deleted at startup.
    #[arg(long, default_value_t = 20, env = "BUMPS_RETENTION_SESSIONS")]
    retention_sessions: usize,

    /// Target for the AWS reachability probe (`host:port`). TCP-connect RTT
    /// is reported every 2s. Defaults to S3 in eu-west-2 (London) since that's
    /// the closest AWS region for UK Starlink users. Override for other relay
    /// regions.
    #[arg(long, default_value = "s3.eu-west-2.amazonaws.com:443", env = "BUMPS_PING_TARGET")]
    ping_target: String,

    /// Disable the AWS reachability probe entirely.
    #[arg(long, env = "BUMPS_NO_PING", default_value_t = false)]
    no_ping: bool,

    /// Enable adaptive bitrate. With this flag on, the encoder's bitrate
    /// is automatically stepped up and down based on observed SRT loss
    /// and send-buffer fullness. Off by default — for a stable stream
    /// you almost always want a fixed bitrate that matches your relay's
    /// configured ceiling, not a target that moves under your feet.
    #[arg(long, env = "BUMPS_ADAPT", default_value_t = false)]
    adapt: bool,

    /// Adaptive bitrate floor. The adapter never drops below this even on
    /// heavy loss. Defaults to 50 % of `--bitrate-kbps` — gives the adapter
    /// real headroom to back off when SRT loss starts before it ever has to
    /// hit a hard floor.
    #[arg(long, env = "BUMPS_MIN_BITRATE_KBPS")]
    min_bitrate_kbps: Option<u32>,

    /// Adaptive bitrate ceiling. The adapter never raises above this even on
    /// a perfect link. Defaults to 120 % of `--bitrate-kbps` — matched to the
    /// SRT `maxbw` so the encoder's VBV ceiling never bursts above what SRT
    /// has agreed to pace.
    #[arg(long, env = "BUMPS_MAX_BITRATE_KBPS")]
    max_bitrate_kbps: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,bumps_pipe=debug")),
        )
        .init();

    gstreamer::init().context("gstreamer::init")?;

    let args = Args::parse();
    tracing::info!(?args, "starting bumps-pipe (phase 3 / pass B)");

    // Resolve the capture config and run a retention sweep before anything else.
    let capture_cfg = if args.no_capture {
        tracing::info!("capture disabled via --no-capture");
        None
    } else {
        let data_dir = args
            .data_dir
            .clone()
            .unwrap_or_else(capture::default_data_dir);
        let cfg = capture::CaptureCfg {
            data_dir,
            retention_sessions: args.retention_sessions,
        };
        tracing::info!(dir = %cfg.data_dir.display(), retain = cfg.retention_sessions, "capture enabled");
        if let Err(e) = capture::run_retention_sweep(&cfg) {
            tracing::warn!(?e, "retention sweep failed (continuing)");
        }
        Some(cfg)
    };

    let stats_state = Arc::new(stats::StatsState::new());

    let (events_tx, events_rx) = tokio::sync::mpsc::channel(64);

    // Cross-task channel for posting into the active capture session's
    // events.jsonl. Adapter (and any future producer) writes; pipeline::run
    // drains and forwards to the current `CaptureSession`.
    let (capture_event_tx, capture_event_rx) = tokio::sync::mpsc::channel(64);

    // Bus watch → pipeline reconnect task. Each ActiveSession's bus watch
    // gets a Sender clone; pipeline::run drains the matching Receiver and
    // decides to teardown + rebuild on error.
    let (pipeline_error_tx, pipeline_error_rx) = tokio::sync::mpsc::channel(8);

    // Web → pipeline operator commands (restart, etc.).
    let (pipeline_command_tx, pipeline_command_rx) = tokio::sync::mpsc::channel(8);

    // Preview channels: init via watch (latest cached for late-joining clients),
    // video via broadcast (fan-out to all open dashboards).
    //
    // Capacity 60 = ~2 s at 30fps. With a downscaled+low-bitrate preview
    // encode this is enough to ride out brief WS-send hiccups while keeping
    // recovery from a stall fast (less stale data in the queue when a client
    // catches up).
    let (init_tx, init_rx) = tokio::sync::watch::channel(None);
    let (frame_tx, _) = tokio::sync::broadcast::channel(60);

    // Stats: collector publishes here, web/WS subscribes.
    let initial_snapshot = empty_snapshot(&args);
    let (stats_tx, stats_rx) = tokio::sync::watch::channel(initial_snapshot);

    let preview = pipeline::PreviewSinks {
        init_tx,
        frame_tx: frame_tx.clone(),
    };

    let adapt_enabled = args.adapt;
    let adapt_min_kbps = args
        .min_bitrate_kbps
        .unwrap_or((args.bitrate_kbps / 2).max(1000));
    let adapt_max_kbps = args
        .max_bitrate_kbps
        .unwrap_or((args.bitrate_kbps * 12 / 10).max(args.bitrate_kbps + 500));

    let resolved_srt_uri = finalise_srt_uri(&args.srt_uri, args.bitrate_kbps);
    if resolved_srt_uri != args.srt_uri {
        tracing::info!(uri = %resolved_srt_uri, "srt: appended stability defaults to bare URI");
    }

    let pipeline_cfg = pipeline::Config {
        srt_uri: resolved_srt_uri.clone(),
        bitrate_kbps: args.bitrate_kbps,
        // Align the encoder's VBV ceiling with the adapter's ceiling so the
        // two agree on the maximum bitrate that may actually be requested.
        max_bitrate_kbps: adapt_max_kbps,
        gop_size: args.gop_size,
        encoder: args.encoder,
        quality: args.quality,
        preview: Some(preview),
        stats: stats_state.clone(),
        capture: capture_cfg,
        stats_rx: stats_rx.clone(),
        pipeline_error_tx: pipeline_error_tx.clone(),
        capture_event_tx: capture_event_tx.clone(),
    };

    let collector_cfg = stats::collector::CollectorConfig {
        target_bitrate_kbps: args.bitrate_kbps,
        encoder_codec_label: format!("{}", args.encoder),
        ping_target: args.ping_target.clone(),
        adapt_enabled,
        adapt_min_kbps,
        adapt_max_kbps,
        quality: args.quality,
    };

    let adapt_cfg = adapt::AdapterConfig {
        enabled: adapt_enabled,
        nominal_kbps: args.bitrate_kbps,
        min_kbps: adapt_min_kbps,
        max_kbps: adapt_max_kbps,
    };

    let web_state = web::AppState {
        init_rx,
        frame_tx,
        stats_rx: stats_rx.clone(),
        stats: stats_state.clone(),
        pipeline_command_tx: pipeline_command_tx.clone(),
        capture_event_tx: capture_event_tx.clone(),
    };

    let pipeline_task = tokio::spawn(pipeline::run(
        events_rx,
        capture_event_rx,
        pipeline_error_rx,
        pipeline_command_rx,
        pipeline_cfg,
    ));
    let rtmp_task = tokio::spawn(rtmp::serve(
        args.rtmp_listen.clone(),
        events_tx,
        stats_state.clone(),
    ));
    let web_task = tokio::spawn(web::serve(args.web_listen, web_state));
    let collector_task = tokio::spawn(stats::collector::run(
        stats_state.clone(),
        collector_cfg,
        stats_tx,
    ));

    let ping_task: tokio::task::JoinHandle<()> = if args.no_ping {
        tracing::info!("ping probe disabled via --no-ping");
        tokio::spawn(std::future::pending())
    } else {
        tokio::spawn(ping::run(stats_state.clone(), args.ping_target.clone()))
    };

    let adapt_task = tokio::spawn(adapt::run(
        stats_state.clone(),
        adapt_cfg,
        stats_rx.clone(),
        capture_event_tx.clone(),
    ));

    tracing::info!(
        rtmp = %args.rtmp_listen,
        web  = %args.web_listen,
        srt  = %resolved_srt_uri,
        "endpoints up; dashboard at http://{}/", args.web_listen
    );

    tokio::select! {
        r = pipeline_task  => { tracing::error!(?r, "pipeline task exited"); }
        r = rtmp_task      => { tracing::error!(?r, "rtmp task exited"); }
        r = web_task       => { tracing::error!(?r, "web task exited"); }
        r = collector_task => { tracing::error!(?r, "stats collector exited"); }
        r = ping_task      => { tracing::error!(?r, "ping task exited"); }
        r = adapt_task     => { tracing::error!(?r, "adapt task exited"); }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c, shutting down");
        }
    }
    Ok(())
}

/// If `uri` has no query string, append a set of known-good stability
/// defaults derived from `bitrate_kbps`. If it already has `?…` params,
/// return it untouched so the caller's explicit values win.
///
/// Defaults applied to a bare `srt://host:port`:
/// - `mode=caller` — outbound to a listener (e.g. MediaConnect / srt-live-transmit).
/// - `latency=8000` / `peerlatency=8000` — generous receive buffer that
///   absorbs multi-second Starlink handovers plus ARQ retransmits.
/// - `oheadbw=100` — 100 % retransmit overhead allowed. Tells SRT it may
///   use up to as much bandwidth again as the input for retransmits.
/// - `inputbw = bitrate_kbps × 125` and `maxbw = bitrate_kbps × 250` —
///   2× headroom over the encoder's average target. CRITICALLY: libsrt's
///   `inputbw` and `maxbw` are in **BYTES per second**, not bits per
///   second (despite the SI-style naming). `kbps × 125 = bytes/sec`
///   (= `kbps × 1000 / 8`). Treating them as bits/sec — which is what we
///   did originally and what most callers naturally write — gives SRT
///   permission to send at 8× the intended cap, which on a lossy link
///   manifests as runaway retransmits (60+ Mbps on a 3 Mbps target).
/// - `rcvbuf=25000000`, `sndbuf=25000000` — 25 MB socket buffers (in
///   bytes; OS-level buffer sizes are always bytes).
/// - `peeridletimeo=30000` — 30 s before SRT declares the peer dead
///   (default 5 s would tear connections during longer stalls).
/// - `streamid=drone` — convenience label for MediaConnect logs.
fn finalise_srt_uri(uri: &str, bitrate_kbps: u32) -> String {
    if uri.contains('?') {
        return uri.to_string();
    }
    let inputbw = bitrate_kbps as u64 * 125; // bytes/sec
    let maxbw = bitrate_kbps as u64 * 250;   // 2× headroom, bytes/sec
    format!(
        "{uri}?mode=caller\
         &latency=8000&peerlatency=8000\
         &oheadbw=100\
         &inputbw={inputbw}&maxbw={maxbw}\
         &rcvbuf=25000000&sndbuf=25000000\
         &peeridletimeo=30000\
         &streamid=drone"
    )
}

/// An empty snapshot used to seed the watch channel before the collector ticks.
fn empty_snapshot(args: &Args) -> stats::Snapshot {
    use stats::*;
    Snapshot {
        ts_unix_ms: 0,
        downlink: DownlinkStats {
            connected: false,
            bytes_in: 0,
            frames_in: 0,
            bitrate_kbps: 0.0,
            last_frame_age_ms: None,
            session_uptime_s: None,
            pts_anomalies: 0,
        },
        encoder: EncoderStats {
            codec: format!("{}", args.encoder),
            target_kbps: args.bitrate_kbps,
            nominal_kbps: args.bitrate_kbps,
            actual_kbps: 0.0,
            frames_out: 0,
            adapt_enabled: args.adapt,
            min_kbps: args
                .min_bitrate_kbps
                .unwrap_or((args.bitrate_kbps / 2).max(1000)),
            max_kbps: args
                .max_bitrate_kbps
                .unwrap_or((args.bitrate_kbps * 12 / 10).max(args.bitrate_kbps + 500)),
            step_downs: 0,
            step_ups: 0,
            quality: args.quality,
            override_kbps: 0,
        },
        preview: PreviewStats {
            clients: 0,
            sent_frames: 0,
            sent_bytes: 0,
            dropped: 0,
        },
        uplink: UplinkStats {
            state: UplinkState::Idle,
            rtt_ms: 0.0,
            send_kbps: 0.0,
            link_cap_mbps: 0.0,
            send_buf_pct: 0.0,
            sent_bytes: 0,
            retransmitted_pkts: 0,
            lost_pkts: 0,
            pkt_loss_rate: 0.0,
        },
        ping: PingStats {
            target: args.ping_target.clone(),
            last_rtt_ms: None,
            ewma_rtt_ms: 0.0,
            success_count: 0,
            failure_count: 0,
            last_success_age_s: None,
        },
        pipeline: PipelineHealth {
            rollup: HealthRollup::Warn,
            uptime_s: 0.0,
            restarts: 0,
        },
    }
}
