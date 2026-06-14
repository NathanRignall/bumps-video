//! GStreamer pipeline construction.
//!
//! ```text
//! appsrc (video/x-flv)
//!   → flvdemux  (dynamic src pad → linked on pad-added)
//!   → h264parse
//!   → identity (placeholder for the timestamp flattener — pass-through in v1)
//!   → avdec_h264
//!   → videoconvert
//!   → video/x-raw,format=NV12
//!   → encoder (qsv-hevc | vt-hevc | x264)
//!   → parser  (h265parse | h264parse)
//!   → tee ─┬─→ queue → mpegtsmux → srtsink                (uplink)
//!          └─→ queue → appsink                            (preview, optional)
//! ```

use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};

use super::{Config, EncoderKind, PreviewSinks};
use crate::stats::StatsState;
use crate::wsproto::{FrameChunk, InitInfo};

pub(super) struct Built {
    pub pipeline: gstreamer::Pipeline,
    pub appsrc: AppSrc,
    /// Caller stashes this in `StatsState::srtsink` so the collector can poll.
    pub srtsink: gstreamer::Element,
    /// Caller stashes this in `StatsState::encoder` so the adapter can write
    /// the `bitrate` property at runtime.
    pub encoder: gstreamer::Element,
}

pub(super) fn build_pipeline(cfg: &Config) -> Result<Built> {
    let pipeline = gstreamer::Pipeline::with_name("bumps");

    // ── ingest ────────────────────────────────────────────────────────────
    let appsrc = gstreamer::ElementFactory::make("appsrc")
        .name("ingest")
        .property("is-live", true)
        .property("do-timestamp", false)
        .property("block", false)
        .property_from_str("stream-type", "stream")
        .property_from_str("format", "time")
        .build()
        .context("appsrc factory")?
        .downcast::<AppSrc>()
        .map_err(|_| anyhow!("appsrc downcast"))?;
    appsrc.set_caps(Some(&gstreamer::Caps::builder("video/x-flv").build()));

    let flvdemux = make("flvdemux", "demux")?;
    let h264parse = make("h264parse", "parse_h264")?;

    // Placeholder for the timestamp flattener (Phase 4). Pass-through for now.
    let flatten = make("identity", "flatten")?;

    let avdec = make("avdec_h264", "dec")?;
    let convert = make("videoconvert", "convert")?;
    let caps_raw = gstreamer::ElementFactory::make("capsfilter")
        .name("caps_nv12")
        .property(
            "caps",
            gstreamer::Caps::builder("video/x-raw")
                .field("format", "NV12")
                .build(),
        )
        .build()
        .context("caps_nv12")?;

    let EncoderBuilt { encoder, parser } = build_encoder(cfg)?;

    let tee = make("tee", "tee")?;
    // SRT branch: leaky=no so a saturated SRT path backpressures the encoder
    // (which then drops whole frames at the source, intentionally) rather than
    // silently shredding the MPEG-TS bytestream mid-packet.
    let queue_srt = make_queue("q_srt", false)?;
    let mpegts = make("mpegtsmux", "ts")?;
    let srtsink = gstreamer::ElementFactory::make("srtsink")
        .name("uplink")
        .property("uri", &cfg.srt_uri)
        .property("wait-for-connection", false)
        .build()
        .context("srtsink")?;

    // Pad probe on the encoder's src pad: count buffers and bytes that come
    // out of the encoder. Probes run on the streaming thread, so the body
    // must be very cheap — just a couple of atomic adds.
    attach_encoder_probe(&encoder, cfg.stats.clone());

    // Optional preview branch.
    let preview_branch = cfg
        .preview
        .as_ref()
        .map(|sinks| build_preview_branch(sinks.clone(), cfg.encoder, cfg.stats.clone()))
        .transpose()?;

    // ── assemble ──────────────────────────────────────────────────────────
    pipeline
        .add_many([
            appsrc.upcast_ref::<gstreamer::Element>(),
            &flvdemux,
            &h264parse,
            &flatten,
            &avdec,
            &convert,
            &caps_raw,
            &encoder,
            &parser,
            &tee,
            &queue_srt,
            &mpegts,
            &srtsink,
        ])
        .context("pipeline add_many (main chain)")?;

    if let Some(b) = &preview_branch {
        pipeline
            .add_many([&b.queue, b.appsink.upcast_ref::<gstreamer::Element>()])
            .context("pipeline add_many (preview branch)")?;
    }

    appsrc
        .upcast_ref::<gstreamer::Element>()
        .link(&flvdemux)
        .context("link appsrc → flvdemux")?;

    gstreamer::Element::link_many([
        &h264parse,
        &flatten,
        &avdec,
        &convert,
        &caps_raw,
        &encoder,
        &parser,
        &tee,
    ])
    .context("link encode chain")?;

    // tee → queue_srt → mpegts → srtsink
    link_tee_branch(&tee, &queue_srt).context("link tee → q_srt")?;
    gstreamer::Element::link_many([&queue_srt, &mpegts, &srtsink])
        .context("link uplink branch")?;

    // tee → queue_prev → appsink (if preview enabled)
    if let Some(b) = &preview_branch {
        link_tee_branch(&tee, &b.queue).context("link tee → q_preview")?;
        b.queue
            .link(b.appsink.upcast_ref::<gstreamer::Element>())
            .context("link q_preview → appsink")?;
    }

    // flvdemux dynamic-pad bridge.
    let h264parse_weak = h264parse.downgrade();
    flvdemux.connect_pad_added(move |_demux, src_pad| {
        let name = src_pad.name();
        if !name.starts_with("video") {
            tracing::debug!(%name, "flvdemux: ignoring non-video pad");
            return;
        }
        let Some(h264parse) = h264parse_weak.upgrade() else {
            tracing::warn!("h264parse gone before pad-added");
            return;
        };
        let Some(sink) = h264parse.static_pad("sink") else {
            tracing::error!("h264parse has no sink pad");
            return;
        };
        if sink.is_linked() {
            tracing::warn!("h264parse sink already linked; ignoring extra pad");
            return;
        }
        match src_pad.link(&sink) {
            Ok(_) => tracing::info!("flvdemux video pad linked"),
            Err(e) => tracing::error!(?e, "flvdemux → h264parse link failed"),
        }
    });

    Ok(Built {
        pipeline,
        appsrc,
        srtsink,
        encoder,
    })
}

fn attach_encoder_probe(encoder: &gstreamer::Element, stats: Arc<StatsState>) {
    let Some(src) = encoder.static_pad("src") else {
        tracing::error!("encoder has no src pad; can't attach stats probe");
        return;
    };
    src.add_probe(gstreamer::PadProbeType::BUFFER, move |_pad, info| {
        if let Some(gstreamer::PadProbeData::Buffer(ref buf)) = info.data {
            stats
                .enc_bytes_out
                .fetch_add(buf.size() as u64, Ordering::Relaxed);
            stats.enc_frames_out.fetch_add(1, Ordering::Relaxed);
        }
        gstreamer::PadProbeReturn::Ok
    });
}

fn make(factory: &str, name: &str) -> Result<gstreamer::Element> {
    gstreamer::ElementFactory::make(factory)
        .name(name)
        .build()
        .with_context(|| format!("ElementFactory::make({factory})"))
}

/// A queue suitable for sitting on a tee branch. `leaky_downstream=true` drops
/// oldest buffers when full — appropriate for the preview leg where a slow
/// consumer must not back up into the encoder.
fn make_queue(name: &str, leaky_downstream: bool) -> Result<gstreamer::Element> {
    let q = gstreamer::ElementFactory::make("queue")
        .name(name)
        .property("max-size-buffers", 8u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 0u64)
        .build()
        .with_context(|| format!("queue {name}"))?;
    if leaky_downstream {
        q.set_property_from_str("leaky", "downstream");
    }
    Ok(q)
}

fn link_tee_branch(tee: &gstreamer::Element, downstream: &gstreamer::Element) -> Result<()> {
    let tee_src = tee
        .request_pad_simple("src_%u")
        .ok_or_else(|| anyhow!("tee has no src_%u template"))?;
    let sink = downstream
        .static_pad("sink")
        .ok_or_else(|| anyhow!("downstream {} has no sink pad", downstream.name()))?;
    tee_src
        .link(&sink)
        .map_err(|e| anyhow!("tee_src.link: {e:?}"))?;
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────────
// Preview branch (tee → queue → appsink) + per-sample callback
// ────────────────────────────────────────────────────────────────────────────

struct PreviewBranch {
    queue: gstreamer::Element,
    appsink: AppSink,
}

fn build_preview_branch(
    sinks: PreviewSinks,
    encoder: EncoderKind,
    stats: Arc<StatsState>,
) -> Result<PreviewBranch> {
    let queue = make_queue("q_preview", false)?;
    // leaky=no on this queue: we instead rely on max-size-buffers to drop via
    // the appsink's max-buffers + drop properties below.
    queue.set_property_from_str("leaky", "downstream");

    let appsink = gstreamer::ElementFactory::make("appsink")
        .name("preview")
        .property("emit-signals", true)
        .property("sync", false)
        .property("max-buffers", 8u32)
        .property("drop", true)
        .build()
        .context("appsink (preview) factory")?
        .downcast::<AppSink>()
        .map_err(|_| anyhow!("appsink downcast"))?;

    install_preview_callbacks(&appsink, sinks, encoder, stats);

    Ok(PreviewBranch { queue, appsink })
}

fn install_preview_callbacks(
    appsink: &AppSink,
    sinks: PreviewSinks,
    encoder: EncoderKind,
    stats: Arc<StatsState>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let init_sent = Arc::new(AtomicBool::new(false));
    let kind = match encoder {
        EncoderKind::QsvHevc | EncoderKind::VtHevc | EncoderKind::VaHevc => MediaKind::Hevc,
        EncoderKind::X264 => MediaKind::H264,
        EncoderKind::QsvAv1 | EncoderKind::VaAv1 => MediaKind::Av1,
    };

    appsink.set_callbacks(
        AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = match sink.pull_sample() {
                    Ok(s) => s,
                    Err(_) => return Err(gstreamer::FlowError::Eos),
                };

                let buffer = match sample.buffer() {
                    Some(b) => b,
                    None => return Ok(gstreamer::FlowSuccess::Ok),
                };

                if !init_sent.load(Ordering::Relaxed) {
                    if let Some(caps) = sample.caps() {
                        if let Some(info) = init_from_caps(caps, kind) {
                            let _ = sinks.init_tx.send(Some(info));
                            init_sent.store(true, Ordering::Relaxed);
                        }
                    }
                }

                let pts_us = buffer
                    .pts()
                    .map(|t| t.useconds())
                    .unwrap_or(0);
                let is_keyframe = !buffer
                    .flags()
                    .contains(gstreamer::BufferFlags::DELTA_UNIT);

                let map = match buffer.map_readable() {
                    Ok(m) => m,
                    Err(_) => return Ok(gstreamer::FlowSuccess::Ok),
                };
                let data = Bytes::copy_from_slice(map.as_slice());

                // best-effort fan-out: ignore error when no subscribers
                let chunk_bytes = data.len() as u64;
                let send_res = sinks.frame_tx.send(FrameChunk {
                    pts_us,
                    is_keyframe,
                    data,
                });
                if send_res.is_ok() {
                    stats
                        .preview_sent_frames
                        .fetch_add(1, Ordering::Relaxed);
                    stats
                        .preview_sent_bytes
                        .fetch_add(chunk_bytes, Ordering::Relaxed);
                }

                Ok(gstreamer::FlowSuccess::Ok)
            })
            .build(),
    );
}

#[derive(Clone, Copy)]
enum MediaKind {
    Hevc,
    H264,
    Av1,
}

fn init_from_caps(caps: &gstreamer::CapsRef, kind: MediaKind) -> Option<InitInfo> {
    let s = caps.structure(0)?;
    let width: i32 = s.get("width").ok()?;
    let height: i32 = s.get("height").ok()?;
    let (fps_num, fps_den) = s
        .get::<gstreamer::Fraction>("framerate")
        .map(|f| (f.numer() as u32, f.denom() as u32))
        .unwrap_or((30, 1));

    let codec = match kind {
        MediaKind::Hevc => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "main".into());
            let tier: String = s.get("tier").unwrap_or_else(|_| "main".into());
            let level: String = s.get("level").unwrap_or_else(|_| "3.1".into());
            hevc_codec_string(&profile, &tier, &level)
        }
        MediaKind::H264 => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "baseline".into());
            let level: String = s.get("level").unwrap_or_else(|_| "3.1".into());
            h264_codec_string(&profile, &level)
        }
        MediaKind::Av1 => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "main".into());
            let tier: String = s.get("tier").unwrap_or_else(|_| "main".into());
            let level: String = s.get("level").unwrap_or_else(|_| "4.0".into());
            // av1parse exposes bit-depth as a uint in newer versions; fall
            // back to 8-bit if absent.
            let bit_depth: u32 = s
                .get::<u32>("bit-depth")
                .or_else(|_| s.get::<i32>("bit-depth").map(|v| v as u32))
                .unwrap_or(8);
            av1_codec_string(&profile, &tier, &level, bit_depth)
        }
    };

    Some(InitInfo {
        codec,
        width: width as u32,
        height: height as u32,
        fps_num,
        fps_den,
    })
}

/// Construct a WebCodecs HEVC codec string from h265parse caps fields.
///
/// Format: `hev1.<profile_idc>.6.<tier_flag><level_num>.B0`.
/// The `6` (general_profile_compatibility_flags) and `B0` (constraint byte)
/// are heuristic defaults that work for the Main profile in practice; we'd
/// refine them by reading the actual HVCC if we ever need to.
fn hevc_codec_string(profile: &str, tier: &str, level: &str) -> String {
    let profile_idc = match profile {
        "main" => 1u8,
        "main-10" => 2,
        "main-still-picture" => 3,
        _ => 1,
    };
    let tier_flag = if tier.eq_ignore_ascii_case("high") {
        'H'
    } else {
        'L'
    };
    let level_num = parse_hevc_level(level).unwrap_or(93);
    format!("hev1.{profile_idc}.6.{tier_flag}{level_num}.B0")
}

/// HEVC level "X.Y" → general_level_idc value (X*30 + Y*3). e.g. "3.1" → 93.
fn parse_hevc_level(level: &str) -> Option<u32> {
    let mut parts = level.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some(major * 30 + minor * 3)
}

/// Construct a WebCodecs H.264 codec string. Format: `avc1.<idc><compat><level>`.
fn h264_codec_string(profile: &str, level: &str) -> String {
    let (profile_idc, profile_compat) = match profile {
        "baseline" | "constrained-baseline" => (0x42u8, 0xC0u8),
        "main" => (0x4D, 0x40),
        "extended" => (0x58, 0x00),
        "high" => (0x64, 0x00),
        _ => (0x42, 0xC0),
    };
    let level_idc = parse_h264_level(level).unwrap_or(0x1F); // default 3.1
    format!("avc1.{profile_idc:02X}{profile_compat:02X}{level_idc:02X}")
}

/// H.264 level "X.Y" → level_idc value (X*10 + Y). e.g. "3.1" → 31.
fn parse_h264_level(level: &str) -> Option<u8> {
    let mut parts = level.split('.');
    let major: u8 = parts.next()?.parse().ok()?;
    let minor: u8 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some(major * 10 + minor)
}

/// WebCodecs AV1 codec string per the av1-isobmff spec:
/// `av01.<profile>.<seq_level_idx_0><seq_tier_0>.<bit_depth>`.
///
/// `seq_level_idx_0` is a two-digit zero-padded number; `seq_tier_0` is
/// `M` (main) or `H` (high). For our defaults (1080p30, Main profile, Main
/// tier, 8-bit) the string is `av01.0.08M.08`.
fn av1_codec_string(profile: &str, tier: &str, level: &str, bit_depth: u32) -> String {
    let profile_idc: u32 = match profile {
        "main" => 0,
        "high" => 1,
        "professional" => 2,
        _ => 0,
    };
    let seq_level_idx = av1_seq_level_idx(level).unwrap_or(8);
    let tier_char = if tier.eq_ignore_ascii_case("high") {
        'H'
    } else {
        'M'
    };
    format!("av01.{profile_idc}.{seq_level_idx:02}{tier_char}.{bit_depth:02}")
}

/// AV1 level "X.Y" → `seq_level_idx_0` per the spec (each major level packs
/// four indexes). 2.0=0, 3.0=4, 4.0=8, 5.1=13, 6.0=16, …
fn av1_seq_level_idx(level: &str) -> Option<u32> {
    let mut parts = level.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some(major.saturating_sub(2) * 4 + minor)
}

// ────────────────────────────────────────────────────────────────────────────
// Encoder factory
// ────────────────────────────────────────────────────────────────────────────

struct EncoderBuilt {
    encoder: gstreamer::Element,
    /// The bitstream parser matching the encoder's output. Sits between the
    /// encoder and the tee.
    parser: gstreamer::Element,
}

fn build_encoder(cfg: &Config) -> Result<EncoderBuilt> {
    let q = cfg.quality.clamp(0.0, 1.0);
    match cfg.encoder {
        EncoderKind::QsvHevc => {
            // VBR with explicit ceiling. `max-bitrate` is the hard cap that
            // the adapter is allowed to chase. `target-usage` rides the
            // quality lever: 1 = best quality / slow, 7 = fastest / lowest.
            let encoder = gstreamer::ElementFactory::make("qsvh265enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("max-bitrate", cfg.max_bitrate_kbps)
                .property("gop-size", cfg.gop_size)
                .property("low-latency", true)
                .property("target-usage", qsv_target_usage(q))
                .property_from_str("rate-control", "vbr")
                .build()
                .context(
                    "qsvh265enc factory — `gst-inspect-1.0 qsvh265enc` should show it; \
                     check intel-media-driver + libvpl + render group + vainfo on the host",
                )?;
            let parser = h265_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::VtHevc => {
            // VideoToolbox doesn't expose VBV directly; `realtime` + the
            // `quality` lever do the rate/quality balancing.
            let encoder = gstreamer::ElementFactory::make("vtenc_h265")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("max-keyframe-interval", cfg.gop_size as i32)
                .property("realtime", true)
                .property("allow-frame-reordering", false)
                // vtenc's `quality` is the only quality knob VideoToolbox
                // exposes. Empirically it goes nonlinear above ~0.6 and
                // starts ignoring the `bitrate` cap entirely (we observed
                // 73 Mbps at quality=1.0 against a 5 Mbps target). We map
                // the user's 0.0-1.0 lever onto a safe vtenc range that
                // still respects the bitrate target.
                .property("quality", vt_quality_clamped(q))
                .build()
                .context(
                    "vtenc_h265 factory — should ship with gst-plugins-bad on macOS; \
                     try `gst-inspect-1.0 vtenc_h265`",
                )?;
            let parser = h265_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::X264 => {
            // x264 quality is governed by `speed-preset` (slower = better at
            // the same bitrate). VBV cap unchanged.
            let opts = format!(
                "vbv-maxrate={}:vbv-bufsize={}",
                cfg.max_bitrate_kbps,
                cfg.max_bitrate_kbps * 2,
            );
            let encoder = gstreamer::ElementFactory::make("x264enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("key-int-max", cfg.gop_size)
                .property("bframes", 0u32)
                .property("option-string", &opts)
                .property_from_str("speed-preset", x264_speed_preset(q))
                .property_from_str("tune", "zerolatency")
                .build()
                .context("x264enc factory — provided by gst-plugins-ugly")?;
            let parser = gstreamer::ElementFactory::make("h264parse")
                .name("parse_out")
                .property("config-interval", -1i32)
                .build()
                .context("h264parse (output side)")?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::QsvAv1 => {
            // Intel QSV AV1 hardware encode. Same property shape as
            // qsvh265enc, but AV1 encode is heavier — clamp `target-usage`
            // at 3 (i.e. never below the "balanced" tier) so even
            // `--quality 1.0` stays comfortably realtime at 1080p30 on a
            // Core Ultra iGPU.
            let target_usage = qsv_target_usage(q).max(3);
            let encoder = gstreamer::ElementFactory::make("qsvav1enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("max-bitrate", cfg.max_bitrate_kbps)
                .property("gop-size", cfg.gop_size)
                .property("low-latency", true)
                .property("target-usage", target_usage)
                .property_from_str("rate-control", "vbr")
                .build()
                .context(
                    "qsvav1enc factory — needs an Intel iGPU with hardware AV1 \
                     encode (Core Ultra / Arc-class). Older iGPUs will not show \
                     this element in `gst-inspect-1.0`.",
                )?;
            let parser = av1_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::VaHevc => {
            // VA-API HEVC via gst-plugins-bad's `va` plugin. Same Intel iGPU
            // as QSV, accessed via libva. Property names differ slightly
            // from QSV: `key-int-max` (frames) instead of `gop-size`.
            let encoder = gstreamer::ElementFactory::make("vah265enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("max-bitrate", cfg.max_bitrate_kbps)
                .property("key-int-max", cfg.gop_size)
                .property("target-usage", qsv_target_usage(q))
                .property_from_str("rate-control", "vbr")
                .build()
                .context(
                    "vah265enc factory — provided by gst-plugins-bad's `va` \
                     plugin. Confirm with `gst-inspect-1.0 vah265enc`.",
                )?;
            let parser = h265_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::VaAv1 => {
            // VA-API AV1. Same caveat about AV1 encode being heavier than
            // HEVC; mirror the QSV clamp on target-usage.
            let target_usage = qsv_target_usage(q).max(3);
            let encoder = gstreamer::ElementFactory::make("vaav1enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("max-bitrate", cfg.max_bitrate_kbps)
                .property("key-int-max", cfg.gop_size)
                .property("target-usage", target_usage)
                .property_from_str("rate-control", "vbr")
                .build()
                .context(
                    "vaav1enc factory — provided by gst-plugins-bad's `va` \
                     plugin. Requires hardware AV1 encode on the iGPU \
                     (Core Ultra / Arc-class).",
                )?;
            let parser = av1_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
    }
}

fn av1_parser() -> Result<gstreamer::Element> {
    gstreamer::ElementFactory::make("av1parse")
        .name("parse_out")
        .build()
        .context("av1parse — provided by gst-plugins-bad")
}

/// Map quality 0.0–1.0 to QSV `target-usage` 1–7. 1.0 → 1 (best quality),
/// 0.0 → 7 (fastest).
fn qsv_target_usage(q: f32) -> u32 {
    let v = (7.0 - 6.0 * q).round() as i32;
    v.clamp(1, 7) as u32
}

/// Map quality 0.0–1.0 onto a vtenc-safe range. Above ~0.6 vtenc starts
/// ignoring the bitrate cap, so we stay in `[0.40, 0.55]`. Lever still has
/// visible effect on quality without blowing up the SRT uplink.
fn vt_quality_clamped(q: f32) -> f64 {
    let clamped = q.clamp(0.0, 1.0);
    (0.40 + 0.15 * clamped) as f64
}

/// Map quality 0.0–1.0 to x264 `speed-preset`. Buckets chosen to keep us in
/// "live-friendly" territory: even at q=1.0 we don't go slower than `medium`
/// because below that the encoder can't keep up with 1080p30 on most CPUs.
fn x264_speed_preset(q: f32) -> &'static str {
    if q < 0.25 {
        "superfast"
    } else if q < 0.5 {
        "veryfast"
    } else if q < 0.75 {
        "faster"
    } else if q < 0.9 {
        "fast"
    } else {
        "medium"
    }
}

fn h265_parser() -> Result<gstreamer::Element> {
    gstreamer::ElementFactory::make("h265parse")
        .name("parse_out")
        .property("config-interval", -1i32)
        .build()
        .context("h265parse")
}
