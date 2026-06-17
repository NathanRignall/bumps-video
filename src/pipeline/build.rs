//! GStreamer pipeline construction.
//!
//! ```text
//! appsrc (video/x-flv)
//!   → flvdemux  (dynamic src pad → linked on pad-added)
//!   → h264parse
//!   → avdec_h264
//!   → videorate                    (normalises irregular source cadence)
//!   → videoconvert
//!   → video/x-raw,format=NV12
//!   → tee_raw ─┬─→ queue (main, leaky=no)
//!              │     → main_encoder → main_parser
//!              │     → queue (uplink, deep, leaky=no)
//!              │     → mpegtsmux → srtsink                          (uplink)
//!              └─→ queue (preview, leaky=downstream)
//!                    → videoscale → caps 640×360
//!                    → x264enc (preview-only, low bitrate)
//!                    → h264parse → appsink                          (preview)
//! ```
//!
//! Preview is its own encode session so a high uplink bitrate doesn't drag
//! the browser-side decoder into the dirt. The branches are independent past
//! the raw-NV12 tee: a slow SRT path cannot stall the preview, and a slow
//! browser cannot starve the uplink.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

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
    /// the `bitrate` property at runtime, and so the WS handler can force
    /// IDR keyframes (which now serves both the SRT uplink and the browser
    /// preview, since they share the single encode).
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
    // Timestamp flattener: DJI Fly emits H.264 frames with duplicate DTS
    // pairs (two consecutive frames share a DTS every ~500 ms). Encoders
    // and mpegtsmux either stall or re-order on duplicate DTS, which is
    // the root cause of every "AWS stream is pulsing" symptom. Mirror the
    // monotonification ffmpeg's MPEG-TS muxer does internally — clamp
    // each buffer's DTS/PTS to `prev + 1 ms` when the source emits a
    // collision.
    attach_flatten_probe(&h264parse, cfg.stats.clone());
    let avdec = make("avdec_h264", "dec")?;
    // Frame-rate normaliser. The flattener fixes DTS *collisions* but the
    // drone can still deliver frames at irregular wall-clock spacing
    // (FLV-tag bursts when the publisher's WiFi briefly stalls) and with
    // sporadic small PTS gaps that the flattener doesn't touch.
    // `videorate` consumes the irregular input and emits a perfectly
    // uniform PTS cadence to the encoder by duplicating-or-dropping
    // frames, which means: encoder workload becomes constant, no IDR
    // burst gets mis-timed against the wall clock, and the receiver
    // sees an even frame rate. `skip-to-first=true` avoids filling the
    // gap between pipeline start and the first real frame with synthetic
    // duplicates.
    let videorate = gstreamer::ElementFactory::make("videorate")
        .name("rate_smooth")
        .property("skip-to-first", true)
        .build()
        .context("videorate")?;
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

    // Encoded-bitstream tee. Single encode feeds two destinations: the
    // uplink (mpegtsmux + srtsink) and the browser preview (appsink). At
    // 3 Mbps HEVC the cost of having the browser decode the production
    // stream is negligible on a modern machine — Mac Safari/Chrome do it
    // in hardware — so spending iGPU + CPU on a separate downscaled
    // encode just to feed the preview pane no longer pays for itself.
    let tee_encoded = make("tee", "tee_encoded")?;

    // SRT branch: leaky=no so a saturated SRT path backpressures the encoder
    // (which then drops whole frames at the source, intentionally) rather than
    // silently shredding the MPEG-TS bytestream mid-packet. Bigger than the
    // 8-buffer default because a brief SRT stall (<= a few hundred ms during
    // a Starlink handover) should ride out the encoder buffer rather than
    // immediately push the encoder into a frame-drop.
    let queue_srt = make_queue_for_branch("q_srt", BranchKind::Uplink)?;
    // `enable-custom-mappings=true` lets the muxer accept codecs whose
    // MPEG-TS stream-type mapping isn't in the base spec yet, in particular
    // AV1. Harmless for HEVC and H.264 (their mappings are standard) and
    // required for AV1 — without it the muxer errors:
    //   "Failed to determine stream type or mapping is not supported"
    let mpegts = gstreamer::ElementFactory::make("mpegtsmux")
        .name("ts")
        .property("enable-custom-mappings", true)
        .build()
        .context("mpegtsmux factory")?;
    let srtsink = gstreamer::ElementFactory::make("srtsink")
        .name("uplink")
        .property("uri", &cfg.srt_uri)
        .property("wait-for-connection", false)
        // sync=false: do *not* wait for the pipeline clock to catch up to
        // each buffer's PTS before sending. With RTMP-sourced PTS (from
        // the publisher's FLV tag timestamps) the clock-wait holds packets
        // until wallclock matches PTS, then dumps them in a burst — which
        // arrives at the receiver as the SRT TSBPD delivery window cycles
        // (0 / target / 0 / target every `?latency=` ms). For live caller
        // mode we want srtsink to push as soon as the encoder produces;
        // SRT's own `maxbw` does the network pacing.
        .property("sync", false)
        .build()
        .context("srtsink")?;

    // Pad probe on the encoder's src pad: count buffers and bytes that come
    // out of the encoder. Probes run on the streaming thread, so the body
    // must be very cheap — just a couple of atomic adds.
    attach_encoder_probe(&encoder, cfg.stats.clone());

    // Optional preview branch — just queue + appsink. The encoded HEVC
    // (or H.264 / AV1, depending on `--encoder`) bitstream is tapped
    // directly off the main tee.
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
            &avdec,
            &videorate,
            &convert,
            &caps_raw,
            &encoder,
            &parser,
            &tee_encoded,
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
        &avdec,
        &videorate,
        &convert,
        &caps_raw,
        &encoder,
        &parser,
        &tee_encoded,
    ])
    .context("link decode→encode→tee chain")?;

    // tee_encoded → queue_srt → mpegts → srtsink
    link_tee_branch(&tee_encoded, &queue_srt).context("link tee_encoded → q_srt")?;
    gstreamer::Element::link_many([&queue_srt, &mpegts, &srtsink])
        .context("link uplink branch")?;

    // tee_encoded → queue_prev → appsink (one-pass tap of the encoded stream)
    if let Some(b) = &preview_branch {
        link_tee_branch(&tee_encoded, &b.queue).context("link tee_encoded → q_preview")?;
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

/// State for [`attach_flatten_probe`]. Tracks the last PTS/DTS the probe
/// emitted so each subsequent buffer can be compared and, if needed,
/// bumped to maintain strict monotonicity.
struct FlattenState {
    prev_pts: Option<gstreamer::ClockTime>,
    prev_dts: Option<gstreamer::ClockTime>,
}

/// Bump applied to a colliding PTS/DTS, chosen to survive the 90 kHz
/// MPEG-TS PCR/PTS quantization downstream.
///
/// GStreamer PTS is in nanoseconds; `mpegtsmux` converts to 90 kHz ticks
/// (≈ 11.1 µs each), so any bump smaller than one tick rounds away. A
/// 1 ns bump (what I tried first) gets lost at the muxer and the wire
/// still has duplicate timestamps. 1 ms is two orders of magnitude
/// above the tick boundary — unambiguously distinct at every downstream
/// element — and still small enough that the bumped frame stays well
/// inside its 1/30 s (≈ 33 ms) inter-frame window, so the drift never
/// catches up with the next "real" timestamp and never accumulates.
///
/// This is the same bump size ffmpeg's MPEG-TS muxer uses internally
/// (one tick of the input timebase, which for FLV is 1 ms).
const FLATTEN_BUMP_NS: u64 = 1_000_000;

/// Install a pad probe on `element`'s src pad that rewrites each buffer's
/// PTS and DTS so the output stream is strictly monotonically increasing
/// at the MPEG-TS wire timebase, not just at GStreamer's nanosecond
/// timebase. When the source emits a buffer with `dts <= prev_dts` (the
/// DJI Fly duplicate-DTS pathology), the new DTS is set to
/// `prev + FLATTEN_BUMP_NS`; same for PTS. Increments
/// `stats.pts_anomalies` on each correction so the dashboard surfaces
/// how often the publisher is misbehaving.
///
/// This is the [`docs/plan.md`] §3.3 "MonotonicRebase" strategy. We
/// don't need the full wallclock-anchored variant for our observed
/// failure mode: the drone's frame *rate* is correct, only its
/// *spacing* between consecutive frames sporadically collapses to
/// zero. Bumping the colliding buffer preserves the average rate (no
/// drift accumulates across frames) while giving every buffer a unique
/// timestamp downstream elements can reorder against.
fn attach_flatten_probe(element: &gstreamer::Element, stats: Arc<StatsState>) {
    let Some(src) = element.static_pad("src") else {
        tracing::error!("flatten probe: element has no src pad");
        return;
    };
    let state = Arc::new(Mutex::new(FlattenState {
        prev_pts: None,
        prev_dts: None,
    }));
    let bump = gstreamer::ClockTime::from_nseconds(FLATTEN_BUMP_NS);

    src.add_probe(gstreamer::PadProbeType::BUFFER, move |_pad, info| {
        let Some(gstreamer::PadProbeData::Buffer(ref mut buf)) = info.data else {
            return gstreamer::PadProbeReturn::Ok;
        };
        let mut s = state.lock().expect("flatten state poisoned");
        let buf_mut = buf.make_mut();

        let mut bumped = false;
        if let Some(dts) = buf_mut.dts() {
            if let Some(prev) = s.prev_dts {
                if dts <= prev {
                    buf_mut.set_dts(Some(prev + bump));
                    bumped = true;
                }
            }
            s.prev_dts = buf_mut.dts();
        }
        if let Some(pts) = buf_mut.pts() {
            if let Some(prev) = s.prev_pts {
                if pts <= prev {
                    buf_mut.set_pts(Some(prev + bump));
                    bumped = true;
                }
            }
            s.prev_pts = buf_mut.pts();
        }
        if bumped {
            stats.pts_anomalies.fetch_add(1, Ordering::Relaxed);
        }

        gstreamer::PadProbeReturn::Ok
    });
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

/// Which branch a tee-queue lives on. Drives the queue's size + leakiness
/// policy. Both raw-side queues are `leaky=downstream`: the raw-NV12 tee
/// has two consumers (the main encoder and the preview encoder), and if
/// either one stalls briefly we'd rather drop raw frames on its branch
/// than block the tee — which would freeze both branches in lockstep.
/// Which branch a tee-queue lives on. Drives the queue's size + leakiness
/// policy. Both queues sit on the *encoded-bitstream* tee, so buffer sizes
/// here are bytes-of-bitstream, not raw frames.
#[derive(Clone, Copy)]
enum BranchKind {
    /// Encoded HEVC/AV1/H.264 feeding mpegtsmux + srtsink. Deep + non-leaky:
    /// we'd rather grow this queue than push the encoder into frame drops
    /// during a brief SRT pause. The matching SRT `latency`/`peerlatency`
    /// makes the receiver tolerate the resulting jitter.
    Uplink,
    /// Encoded bitstream feeding the browser appsink. Shallow + leaky-
    /// downstream: a slow browser tab is the only thing punished if it
    /// can't keep up — the uplink chain is unaffected.
    Preview,
}

fn make_queue_for_branch(name: &str, kind: BranchKind) -> Result<gstreamer::Element> {
    let (max_buffers, max_bytes, max_time_ns, leaky_downstream) = match kind {
        // ~200 buffers ≈ 6 s of 30 fps; bound also by time so a stall in
        // bytes-heavy keyframes doesn't run away. We sized this against the
        // 5 s SRT latency window — the queue should be able to absorb a
        // handover-length stall without overflowing into encoder drops.
        BranchKind::Uplink => (200u32, 0u32, 6 * gstreamer::ClockTime::SECOND.nseconds(), false),
        // ~60 encoded buffers ≈ 2 s of 30 fps. Bounded also in time so a
        // browser that vanishes (tab hidden) drops cleanly rather than
        // accumulating frames.
        BranchKind::Preview => (60u32, 0u32, 2 * gstreamer::ClockTime::SECOND.nseconds(), true),
    };
    let q = gstreamer::ElementFactory::make("queue")
        .name(name)
        .property("max-size-buffers", max_buffers)
        .property("max-size-bytes", max_bytes)
        .property("max-size-time", max_time_ns)
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
// Preview branch — taps the encoded uplink bitstream and emits it to the
// browser via WebSocket. No separate encode: at our nominal 3 Mbps target
// (and even at 8 Mbps), decoding the production stream in a modern browser
// is trivially cheap thanks to hardware HEVC decode on Mac (Safari + Chrome)
// and Windows. The dashboard preview is now at full uplink resolution and
// quality, with zero extra encode load on the field laptop.
//
// The browser must support the active codec — that depends on `--encoder`:
//   * vah265enc / qsvh265enc / vtenc_h265 → hev1.*  (HEVC)
//   * x264enc                              → avc1.* (H.264)
//   * vaav1enc / qsvav1enc                 → av01.* (AV1)
// `init_from_caps` reads h26{4,5}parse / av1parse src caps to build the
// WebCodecs codec string at runtime.
// ────────────────────────────────────────────────────────────────────────────

struct PreviewBranch {
    queue: gstreamer::Element,
    appsink: AppSink,
}

fn build_preview_branch(
    sinks: PreviewSinks,
    encoder_kind: EncoderKind,
    stats: Arc<StatsState>,
) -> Result<PreviewBranch> {
    let queue = make_queue_for_branch("q_preview", BranchKind::Preview)?;

    // Force Annex-B byte-stream output for the browser. WebCodecs is
    // configured *without* a `description` argument on the JS side, which
    // means the spec requires Annex-B framing. h26{4,5}parse will
    // otherwise happily negotiate AVCC/HVCC with appsink (no preference)
    // and the browser silently fails to decode any frame. AV1 has no
    // AVCC equivalent — its parser only produces one format — so we
    // don't constrain the caps in that case.
    let appsink_caps = match encoder_kind {
        EncoderKind::QsvHevc | EncoderKind::VtHevc | EncoderKind::VaHevc => {
            Some(
                gstreamer::Caps::builder("video/x-h265")
                    .field("stream-format", "byte-stream")
                    .field("alignment", "au")
                    .build(),
            )
        }
        EncoderKind::X264 => Some(
            gstreamer::Caps::builder("video/x-h264")
                .field("stream-format", "byte-stream")
                .field("alignment", "au")
                .build(),
        ),
        EncoderKind::QsvAv1 | EncoderKind::VaAv1 => None,
    };

    let mut appsink_builder = gstreamer::ElementFactory::make("appsink")
        .name("preview")
        .property("emit-signals", true)
        .property("sync", false)
        .property("max-buffers", 4u32)
        .property("drop", true);
    if let Some(caps) = appsink_caps.as_ref() {
        appsink_builder = appsink_builder.property("caps", caps);
    }
    let appsink = appsink_builder
        .build()
        .context("appsink (preview) factory")?
        .downcast::<AppSink>()
        .map_err(|_| anyhow!("appsink downcast"))?;

    install_preview_callbacks(&appsink, sinks, stats);

    Ok(PreviewBranch { queue, appsink })
}

fn install_preview_callbacks(
    appsink: &AppSink,
    sinks: PreviewSinks,
    stats: Arc<StatsState>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let init_sent = Arc::new(AtomicBool::new(false));

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
                        if let Some(info) = init_from_caps(caps) {
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

/// The preview now taps the main encoded bitstream, so the codec depends
/// on the configured uplink encoder. Read the caps structure name to pick
/// the right WebCodecs codec-string builder (`avc1.*` / `hev1.*` / `av01.*`).
fn init_from_caps(caps: &gstreamer::CapsRef) -> Option<InitInfo> {
    let s = caps.structure(0)?;
    let width: i32 = s.get("width").ok()?;
    let height: i32 = s.get("height").ok()?;
    let (fps_num, fps_den) = s
        .get::<gstreamer::Fraction>("framerate")
        .map(|f| (f.numer() as u32, f.denom() as u32))
        .unwrap_or((30, 1));

    let codec = match s.name().as_str() {
        "video/x-h264" => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "baseline".into());
            let level: String = s.get("level").unwrap_or_else(|_| "3.1".into());
            h264_codec_string(&profile, &level)
        }
        "video/x-h265" => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "main".into());
            let tier: String = s.get("tier").unwrap_or_else(|_| "main".into());
            let level: String = s.get("level").unwrap_or_else(|_| "3.1".into());
            hevc_codec_string(&profile, &tier, &level)
        }
        "video/x-av1" => {
            let profile: String = s.get("profile").unwrap_or_else(|_| "main".into());
            let tier: String = s.get("tier").unwrap_or_else(|_| "main".into());
            let level: String = s.get("level").unwrap_or_else(|_| "4.0".into());
            let bit_depth: u32 = s
                .get::<u32>("bit-depth")
                .or_else(|_| s.get::<i32>("bit-depth").map(|v| v as u32))
                .unwrap_or(8);
            av1_codec_string(&profile, &tier, &level, bit_depth)
        }
        other => {
            tracing::warn!(caps = %other, "preview: unrecognised caps structure name");
            return None;
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
/// Format: `hev1.<profile_idc>.6.<tier_flag><level_num>.B0`.
fn hevc_codec_string(profile: &str, tier: &str, level: &str) -> String {
    let profile_idc = match profile {
        "main" => 1u8,
        "main-10" => 2,
        "main-still-picture" => 3,
        _ => 1,
    };
    let tier_flag = if tier.eq_ignore_ascii_case("high") { 'H' } else { 'L' };
    let level_num = parse_hevc_level(level).unwrap_or(93);
    format!("hev1.{profile_idc}.6.{tier_flag}{level_num}.B0")
}

/// HEVC level "X.Y" → general_level_idc (X*30 + Y*3). e.g. "3.1" → 93.
fn parse_hevc_level(level: &str) -> Option<u32> {
    let mut parts = level.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some(major * 30 + minor * 3)
}

/// WebCodecs AV1 codec string per the av1-isobmff spec:
/// `av01.<profile>.<seq_level_idx_0><seq_tier_0>.<bit_depth>`.
fn av1_codec_string(profile: &str, tier: &str, level: &str, bit_depth: u32) -> String {
    let profile_idc: u32 = match profile {
        "main" => 0,
        "high" => 1,
        "professional" => 2,
        _ => 0,
    };
    let seq_level_idx = av1_seq_level_idx(level).unwrap_or(8);
    let tier_char = if tier.eq_ignore_ascii_case("high") { 'H' } else { 'M' };
    format!("av01.{profile_idc}.{seq_level_idx:02}{tier_char}.{bit_depth:02}")
}

/// AV1 level "X.Y" → `seq_level_idx_0` (each major level packs four indexes).
fn av1_seq_level_idx(level: &str) -> Option<u32> {
    let mut parts = level.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    Some(major.saturating_sub(2) * 4 + minor)
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
                // config-interval=1 inserts SPS/PPS roughly every second
                // alongside each keyframe. Critical on a lossy SRT path:
                // if the receiver loses the IDR carrying the parameter
                // sets it can pick them up at the next periodic refresh
                // instead of waiting for a whole GOP.
                .property("config-interval", 1i32)
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
            // as QSV, accessed via libva.
            //
            // We deliberately run this in **CBR**, not VBR. The VA-API
            // rate-control model treats VBR's `bitrate` field as a peak
            // burst ceiling and lets the actual rate swing widely below
            // it — which causes two stability problems on an SRT uplink:
            //   1. Bursts above the negotiated `maxbw` overflow SRT's send
            //      buffer in short spikes, producing gray/pixelated frames
            //      at the receiver.
            //   2. The send-buffer spikes look like congestion to our
            //      adapter, which then oscillates the target up and down.
            // CBR pins the encoder to `cfg.bitrate_kbps` and pads with
            // filler when needed so SRT pacing is predictable.
            let encoder = gstreamer::ElementFactory::make("vah265enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                // `cpb-size` is the HRD buffer in kbits. Pinning it to one
                // second of bitrate forces the encoder to comply with CBR
                // over a 1 s sliding window — without this the VA encoder
                // picks a multi-second default and lets the instantaneous
                // rate swing far above the target, which is precisely the
                // pattern that overflowed MediaConnect's max-bitrate cap.
                .property("cpb-size", cfg.bitrate_kbps)
                .property("key-int-max", cfg.gop_size)
                .property("target-usage", qsv_target_usage(q))
                .property_from_str("rate-control", "cbr")
                .build()
                .context(
                    "vah265enc factory — provided by gst-plugins-bad's `va` \
                     plugin. Confirm with `gst-inspect-1.0 vah265enc`.",
                )?;
            let parser = h265_parser()?;
            Ok(EncoderBuilt { encoder, parser })
        }
        EncoderKind::VaAv1 => {
            // VA-API AV1. Same CBR rationale as VaHevc above. Also clamp
            // target-usage at 3 — AV1 encode is heavier than HEVC, and
            // a slower target-usage at 1080p risks the encoder missing
            // frame deadlines.
            let target_usage = qsv_target_usage(q).max(3);
            let encoder = gstreamer::ElementFactory::make("vaav1enc")
                .name("enc")
                .property("bitrate", cfg.bitrate_kbps)
                .property("cpb-size", cfg.bitrate_kbps)
                .property("key-int-max", cfg.gop_size)
                .property("target-usage", target_usage)
                .property_from_str("rate-control", "cbr")
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
        // config-interval=1: inject VPS/SPS/PPS roughly every second along
        // with each keyframe. See the matching note on h264parse above —
        // this is the single biggest stability win for an SRT receiver on
        // a lossy link.
        .property("config-interval", 1i32)
        .build()
        .context("h265parse")
}
