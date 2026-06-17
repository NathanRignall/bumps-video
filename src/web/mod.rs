//! axum HTTP + WebSocket server.
//!
//! Phase 2 surface area:
//! - `GET /`            — single-page dashboard, served from a static blob
//! - `GET /ws`          — preview WebSocket (init JSON + binary frame chunks)

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use tokio::sync::{broadcast, watch};

use crate::capture::CaptureEventReq;
use crate::pipeline::PipelineCommand;
use crate::stats::{Snapshot, StatsState};
use crate::wsproto::{ClientMsg, FrameChunk, InitInfo, ServerMsg};

#[derive(Clone)]
pub struct AppState {
    pub init_rx: watch::Receiver<Option<InitInfo>>,
    pub frame_tx: broadcast::Sender<FrameChunk>,
    pub stats_rx: watch::Receiver<Snapshot>,
    pub stats: Arc<StatsState>,
    /// Channel to the pipeline task for operator commands.
    pub pipeline_command_tx: tokio::sync::mpsc::Sender<PipelineCommand>,
    /// Channel to the active CaptureSession's events.jsonl for posting
    /// operator-triggered events.
    pub capture_event_tx: tokio::sync::mpsc::Sender<CaptureEventReq>,
}

const INDEX_HTML: &str = include_str!("../../frontend/index.html");

pub async fn serve(listen: SocketAddr, state: AppState) -> Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("web bind {listen}"))?;
    tracing::info!(%listen, "web server bound");
    axum::serve(listener, app).await.context("axum::serve")?;
    Ok(())
}

async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut init_rx = state.init_rx.clone();
    let mut frame_rx = state.frame_tx.subscribe();
    let mut stats_rx = state.stats_rx.clone();

    state
        .stats
        .preview_clients
        .fetch_add(1, Ordering::Relaxed);
    tracing::debug!("ws client connected");

    // Send the current init (if any) right away so the browser can configure
    // VideoDecoder immediately when there's an active session. `borrow_and_update`
    // marks this value as seen so the loop below doesn't re-fire on it.
    let initial_init = init_rx.borrow_and_update().clone();
    if let Some(init) = initial_init {
        if send_init(&mut sender, &init).await.is_err() {
            state.stats.preview_clients.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        // Kick the *preview* encoder to emit a fresh keyframe so this
        // client can start decoding immediately rather than waiting up to
        // GOP-size frames for the next natural one. Uses the preview-side
        // encoder so the uplink GOP cadence isn't disturbed every time a
        // browser tab is opened.
        state.stats.request_preview_keyframe();
    }

    // Send the current stats snapshot immediately so the dashboard has
    // something to render before the next tick.
    let snap_now = stats_rx.borrow_and_update().clone();
    if send_stats(&mut sender, &snap_now).await.is_err() {
        state.stats.preview_clients.fetch_sub(1, Ordering::Relaxed);
        return;
    }

    loop {
        tokio::select! {
            r = init_rx.changed() => {
                if r.is_err() { break; }
                let next = init_rx.borrow_and_update().clone();
                if let Some(init) = next {
                    if send_init(&mut sender, &init).await.is_err() { break; }
                }
                // init=None means session ended; browser sees frames stop.
            }
            r = stats_rx.changed() => {
                if r.is_err() { break; }
                let snap = stats_rx.borrow_and_update().clone();
                if send_stats(&mut sender, &snap).await.is_err() { break; }
            }
            r = frame_rx.recv() => {
                match r {
                    Ok(chunk) => {
                        let bytes = chunk.encode();
                        if sender.send(Message::Binary(bytes.to_vec())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Browser fell behind — by the time it re-syncs to
                        // the broadcast head we may be mid-GOP, which means
                        // it cannot decode anything until the next keyframe.
                        // Force one now so playback resumes within a frame
                        // rather than within `gop_size` frames.
                        tracing::warn!(n, "ws client lagged on broadcast; forcing preview keyframe");
                        state.stats.preview_dropped.fetch_add(n, Ordering::Relaxed);
                        state.stats.request_preview_keyframe();
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = receiver.next() => {
                match msg {
                    None => break,
                    Some(Err(_)) => break,
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMsg>(&text) {
                            Ok(cmd) => handle_client_command(cmd, &state).await,
                            Err(e) => tracing::warn!(?e, %text, "bad client message"),
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    state.stats.preview_clients.fetch_sub(1, Ordering::Relaxed);
    tracing::debug!("ws client disconnected");
}

async fn handle_client_command(cmd: ClientMsg, state: &AppState) {
    use gstreamer::prelude::*;
    match cmd {
        ClientMsg::RequestKeyframe => {
            // Operator-initiated → IDR both encoders. The uplink IDR helps
            // the SRT receiver resync; the preview IDR helps the dashboard
            // viewer resync. They're independent encoders now and we don't
            // know which one the operator is rescuing, so do both.
            state.stats.request_keyframe();
            state.stats.request_preview_keyframe();
            post_event(state, "operator_keyframe", serde_json::json!({})).await;
            tracing::info!("operator: request_keyframe");
        }
        ClientMsg::SetBitrate { kbps } => {
            state
                .stats
                .adapt_override_kbps
                .store(kbps, Ordering::Relaxed);
            // Apply immediately to the live encoder so the operator sees the
            // change in the next snapshot rather than waiting for the next
            // adapter tick.
            if let Ok(guard) = state.stats.encoder.lock() {
                if let Some(enc) = guard.as_ref() {
                    if enc.find_property("bitrate").is_some() {
                        enc.set_property("bitrate", kbps);
                    }
                }
            }
            state
                .stats
                .adapt_target_kbps
                .store(kbps, Ordering::Relaxed);
            post_event(
                state,
                "bitrate_pinned",
                serde_json::json!({ "kbps": kbps }),
            )
            .await;
            tracing::info!(kbps, "operator: set_bitrate");
        }
        ClientMsg::ClearBitrateOverride => {
            state
                .stats
                .adapt_override_kbps
                .store(0, Ordering::Relaxed);
            post_event(state, "bitrate_unpinned", serde_json::json!({})).await;
            tracing::info!("operator: clear_bitrate_override");
        }
        ClientMsg::RestartPipeline => {
            let _ = state
                .pipeline_command_tx
                .try_send(PipelineCommand::Restart);
        }
    }
}

async fn post_event(state: &AppState, kind: &str, details: serde_json::Value) {
    let _ = state.capture_event_tx.try_send(CaptureEventReq {
        kind: kind.to_string(),
        details,
    });
}

async fn send_init(
    sender: &mut SplitSink<WebSocket, Message>,
    init: &InitInfo,
) -> Result<(), axum::Error> {
    let msg = ServerMsg::Init(init.clone());
    let json = serde_json::to_string(&msg).expect("InitInfo serialises");
    sender.send(Message::Text(json)).await
}

async fn send_stats(
    sender: &mut SplitSink<WebSocket, Message>,
    snap: &Snapshot,
) -> Result<(), axum::Error> {
    let msg = ServerMsg::Stats(Box::new(snap.clone()));
    let json = serde_json::to_string(&msg).expect("Snapshot serialises");
    sender.send(Message::Text(json)).await
}
