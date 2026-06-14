//! RTMP server: accepts the DJI Fly publish, reassembles an FLV bytestream,
//! and emits [`IngestEvent`]s to the pipeline.
//!
//! Phase 1: one publisher at a time. Multiple concurrent TCP connections will
//! still be accepted (so a half-open old connection doesn't lock anyone out),
//! but the channel they all share means only one drives the pipeline at a time.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::stats::StatsState;

/// Events the RTMP layer emits to the pipeline.
#[derive(Debug)]
pub enum IngestEvent {
    /// Publisher accepted; pipeline should be built.
    SessionStarted {
        peer: SocketAddr,
        app: String,
        stream_key: String,
    },
    /// A chunk of well-formed FLV bytes (header on first send, then tags).
    /// Pushed straight into the GStreamer `appsrc` element typed as `video/x-flv`.
    FlvChunk(Bytes),
    /// Publisher gone. Pipeline should tear down.
    SessionEnded,
}

pub async fn serve(
    listen: String,
    events: mpsc::Sender<IngestEvent>,
    stats: Arc<StatsState>,
) -> Result<()> {
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("rtmp bind {listen}"))?;
    tracing::info!(%listen, "rtmp listener bound");

    loop {
        let (sock, peer) = listener.accept().await.context("rtmp accept")?;
        let events = events.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            tracing::info!(%peer, "rtmp tcp accepted");
            match handle_connection(sock, peer, events, stats).await {
                Ok(()) => tracing::info!(%peer, "rtmp connection ended"),
                Err(e) => tracing::warn!(%peer, error = ?e, "rtmp connection failed"),
            }
        });
    }
}

async fn handle_connection(
    mut sock: TcpStream,
    peer: SocketAddr,
    events: mpsc::Sender<IngestEvent>,
    stats: Arc<StatsState>,
) -> Result<()> {
    // ── RTMP handshake ─────────────────────────────────────────────────────
    let mut handshake = Handshake::new(PeerType::Server);
    let server_p0_p1 = handshake
        .generate_outbound_p0_and_p1()
        .map_err(|e| anyhow!("handshake init: {e:?}"))?;
    sock.write_all(&server_p0_p1).await?;

    let mut buf = vec![0u8; 8192];
    let leftover = loop {
        let n = sock.read(&mut buf).await?;
        if n == 0 {
            return Err(anyhow!("eof during handshake"));
        }
        match handshake
            .process_bytes(&buf[..n])
            .map_err(|e| anyhow!("handshake: {e:?}"))?
        {
            HandshakeProcessResult::InProgress { response_bytes } => {
                if !response_bytes.is_empty() {
                    sock.write_all(&response_bytes).await?;
                }
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                if !response_bytes.is_empty() {
                    sock.write_all(&response_bytes).await?;
                }
                break remaining_bytes;
            }
        }
    };

    // ── RTMP session ───────────────────────────────────────────────────────
    let config = ServerSessionConfig::new();
    let (mut session, init_results) =
        ServerSession::new(config).map_err(|e| anyhow!("session new: {e:?}"))?;

    let mut conn_state = ConnState::default();
    let mut pending: Vec<ServerSessionResult> = init_results;

    if !leftover.is_empty() {
        let r = session
            .handle_input(&leftover)
            .map_err(|e| anyhow!("handle leftover: {e:?}"))?;
        pending.extend(r);
    }

    loop {
        // Drain pending results. handle_event may append new results to
        // `pending` (e.g. from accept_request), so we keep draining until
        // the vec is empty before reading more bytes from the socket.
        while !pending.is_empty() {
            for r in std::mem::take(&mut pending) {
                match r {
                    ServerSessionResult::OutboundResponse(packet) => {
                        sock.write_all(&packet.bytes).await?;
                    }
                    ServerSessionResult::RaisedEvent(event) => {
                        handle_event(
                            event,
                            &mut session,
                            &mut conn_state,
                            &events,
                            peer,
                            &mut pending,
                            &stats,
                        )
                        .await?;
                    }
                    ServerSessionResult::UnhandleableMessageReceived(_) => {}
                }
            }
        }

        tracing::trace!("rtmp: awaiting socket read");
        let n = sock.read(&mut buf).await?;
        tracing::trace!(n, "rtmp: socket read returned");
        if n == 0 {
            if conn_state.session_announced {
                let _ = events.send(IngestEvent::SessionEnded).await;
                stats.publisher_connected.store(false, Ordering::Relaxed);
                stats.session_started_us.store(0, Ordering::Relaxed);
                stats.last_frame_us.store(0, Ordering::Relaxed);
            }
            return Ok(());
        }
        pending = session
            .handle_input(&buf[..n])
            .map_err(|e| anyhow!("handle_input: {e:?}"))?;
        tracing::trace!(read = n, results = pending.len(), "rtmp: handle_input done");
    }
}

#[derive(Default)]
struct ConnState {
    app: String,
    stream_key: String,
    /// True once we've sent SessionStarted + the FLV header downstream.
    session_announced: bool,
}

async fn handle_event(
    event: ServerSessionEvent,
    session: &mut ServerSession,
    conn: &mut ConnState,
    events: &mpsc::Sender<IngestEvent>,
    peer: SocketAddr,
    pending: &mut Vec<ServerSessionResult>,
    stats: &Arc<StatsState>,
) -> Result<()> {
    match event {
        ServerSessionEvent::ConnectionRequested {
            request_id,
            app_name,
        } => {
            tracing::debug!(%peer, %app_name, "rtmp connect");
            conn.app = app_name;
            let results = session
                .accept_request(request_id)
                .map_err(|e| anyhow!("accept connect: {e:?}"))?;
            pending.extend(results);
        }
        ServerSessionEvent::PublishStreamRequested {
            request_id,
            app_name,
            stream_key,
            ..
        } => {
            tracing::info!(%peer, %app_name, %stream_key, "rtmp publish");
            conn.app = app_name.clone();
            conn.stream_key = stream_key.clone();
            let results = session
                .accept_request(request_id)
                .map_err(|e| anyhow!("accept publish: {e:?}"))?;
            pending.extend(results);
            // Announce session + start FLV stream.
            events
                .send(IngestEvent::SessionStarted {
                    peer,
                    app: app_name,
                    stream_key,
                })
                .await
                .ok();
            events.send(IngestEvent::FlvChunk(flv_header())).await.ok();
            conn.session_announced = true;
            stats.publisher_connected.store(true, Ordering::Relaxed);
            stats.session_started_us.store(stats.now_us(), Ordering::Relaxed);
            stats.last_frame_us.store(0, Ordering::Relaxed);
        }
        ServerSessionEvent::PublishStreamFinished { .. } => {
            tracing::info!(%peer, "rtmp publish finished");
            if conn.session_announced {
                events.send(IngestEvent::SessionEnded).await.ok();
                conn.session_announced = false;
                stats.publisher_connected.store(false, Ordering::Relaxed);
                stats.session_started_us.store(0, Ordering::Relaxed);
                stats.last_frame_us.store(0, Ordering::Relaxed);
            }
        }
        ServerSessionEvent::VideoDataReceived {
            data, timestamp, ..
        } => {
            tracing::trace!(ts = timestamp.value, len = data.len(), "rtmp video tag");
            let payload_len = data.len() as u64;
            let tag = flv_tag(TAG_TYPE_VIDEO, timestamp.value, &data);
            stats.bytes_in.fetch_add(payload_len, Ordering::Relaxed);
            stats.frames_in.fetch_add(1, Ordering::Relaxed);
            stats.last_frame_us.store(stats.now_us(), Ordering::Relaxed);
            if events.send(IngestEvent::FlvChunk(tag)).await.is_err() {
                tracing::warn!("pipeline channel closed; dropping video");
            }
        }
        ServerSessionEvent::AudioDataReceived { .. } => {
            // No audio in v1.
        }
        ServerSessionEvent::StreamMetadataChanged { metadata, .. } => {
            tracing::debug!(?metadata, "rtmp metadata");
            // Optional: synthesize a script-data tag for flvdemux. flvdemux can
            // do without it for live H.264, so we skip it for now.
        }
        other => {
            tracing::trace!(?other, "rtmp event (ignored)");
        }
    }
    Ok(())
}

// ───────────────────────────────────────────────────────────────────────────
// FLV framing helpers
// ───────────────────────────────────────────────────────────────────────────

const TAG_TYPE_VIDEO: u8 = 9;

/// FLV file header + initial PreviousTagSize0. Public so the pipeline task
/// can re-emit it on a reconnect rebuild.
pub fn flv_header_bytes() -> Bytes {
    flv_header()
}

/// FLV file header + initial PreviousTagSize0.
fn flv_header() -> Bytes {
    let mut b = BytesMut::with_capacity(13);
    b.put_slice(b"FLV"); // signature
    b.put_u8(1); // version
    b.put_u8(0x01); // flags: bit 0 = video, bit 2 = audio
    b.put_u32(9); // data offset
    b.put_u32(0); // previous tag size 0
    b.freeze()
}

/// One FLV tag (header + payload + trailing PreviousTagSize).
fn flv_tag(tag_type: u8, timestamp_ms: u32, payload: &[u8]) -> Bytes {
    let data_size = payload.len() as u32;
    let mut b = BytesMut::with_capacity(11 + payload.len() + 4);
    b.put_u8(tag_type);
    // data size: 24-bit big-endian
    b.put_u8((data_size >> 16) as u8);
    b.put_u8((data_size >> 8) as u8);
    b.put_u8(data_size as u8);
    // timestamp: 24-bit lower, then 8-bit upper (extended)
    b.put_u8((timestamp_ms >> 16) as u8);
    b.put_u8((timestamp_ms >> 8) as u8);
    b.put_u8(timestamp_ms as u8);
    b.put_u8((timestamp_ms >> 24) as u8);
    // stream id: always 0, 24 bits
    b.put_u8(0);
    b.put_u8(0);
    b.put_u8(0);
    b.put_slice(payload);
    // previous tag size = 11 (tag header) + data_size
    b.put_u32(11 + data_size);
    b.freeze()
}

