//! Per-session debug artifact directory.
//!
//! Each time a publisher session starts, a fresh directory is created under
//! `<data_dir>/sessions/<id>/`. Three files are populated for its lifetime:
//!
//!   - `metadata.json`  — config snapshot + start/stop timestamps + duration
//!   - `snapshot.jsonl` — one [`Snapshot`] per second from the stats watch channel
//!   - `events.jsonl`   — typed records of state transitions and errors
//!
//! Raw FLV / encoded TS taps are deferred (Phase 7).
//!
//! On close, `metadata.json` is rewritten with stop time + duration + reason.
//! A retention sweep at process startup deletes the oldest sessions beyond
//! the configured limit so the data_dir doesn't grow unboundedly.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{oneshot, watch};

use crate::stats::Snapshot;

/// Cross-task event submission. Any task with a `Sender<CaptureEventReq>` can
/// post into the active session's `events.jsonl`. The pipeline task is the
/// only consumer — it forwards to whatever `CaptureSession` is open, or
/// drops if none.
#[derive(Debug, Clone)]
pub struct CaptureEventReq {
    pub kind: String,
    pub details: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct CaptureCfg {
    pub data_dir: PathBuf,
    pub retention_sessions: usize,
}

/// Captured-once view of the runtime config, written into `metadata.json`.
#[derive(Serialize, Clone, Debug)]
pub struct ConfigSnapshot {
    pub encoder: String,
    pub bitrate_kbps: u32,
    pub gop_size: u32,
    pub quality: f32,
    pub srt_uri: String,
}

#[derive(Serialize, Clone, Debug)]
pub struct SessionMetadata {
    pub session_id: String,
    pub start_unix_ms: u64,
    pub start_iso_utc: String,
    pub stream_key: String,
    pub peer: String,
    pub bumps_version: String,
    pub config: ConfigSnapshot,
    pub stop_unix_ms: Option<u64>,
    pub stop_iso_utc: Option<String>,
    pub duration_s: Option<f64>,
    pub stop_reason: Option<String>,
}

#[derive(Serialize)]
struct EventRecord<'a> {
    ts_unix_ms: u64,
    kind: &'a str,
    details: serde_json::Value,
}

/// Default location for the data dir.
///
/// Priority: `$BUMPS_DATA_DIR` → `$HOME/.local/share/bumps-pipe` → `./bumps-data`.
pub fn default_data_dir() -> PathBuf {
    if let Ok(d) = std::env::var("BUMPS_DATA_DIR") {
        return PathBuf::from(d);
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".local/share/bumps-pipe");
    }
    PathBuf::from("bumps-data")
}

pub struct CaptureSession {
    dir: PathBuf,
    metadata: SessionMetadata,
    events_file: fs::File,
    shutdown_tx: Option<oneshot::Sender<()>>,
    snapshot_task: Option<tokio::task::JoinHandle<()>>,
}

impl CaptureSession {
    pub async fn open(
        cfg: &CaptureCfg,
        stream_key: String,
        peer: String,
        config: ConfigSnapshot,
        stats_rx: watch::Receiver<Snapshot>,
    ) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let now_ms = now.as_millis() as u64;
        let suffix_us = (now.as_micros() % 1_000_000) as u32;
        let session_id = format!(
            "{}-{:06}-{}",
            iso_basic(now_ms),
            suffix_us,
            sanitise(&stream_key),
        );

        let dir = cfg.data_dir.join("sessions").join(&session_id);
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create_dir_all({})", dir.display()))?;

        let metadata = SessionMetadata {
            session_id: session_id.clone(),
            start_unix_ms: now_ms,
            start_iso_utc: iso_human(now_ms),
            stream_key,
            peer,
            bumps_version: env!("CARGO_PKG_VERSION").to_string(),
            config,
            stop_unix_ms: None,
            stop_iso_utc: None,
            duration_s: None,
            stop_reason: None,
        };

        // Write initial metadata (will be rewritten on close).
        write_metadata(&dir, &metadata).await?;

        let events_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("events.jsonl"))
            .await
            .context("open events.jsonl")?;

        let snap_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("snapshot.jsonl"))
            .await
            .context("open snapshot.jsonl")?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let snapshot_task = tokio::spawn(snapshot_writer(snap_file, stats_rx, shutdown_rx));

        tracing::info!(dir = %dir.display(), id = %session_id, "capture: session opened");

        Ok(Self {
            dir,
            metadata,
            events_file,
            shutdown_tx: Some(shutdown_tx),
            snapshot_task: Some(snapshot_task),
        })
    }

    pub async fn emit_event(&mut self, kind: &str, details: serde_json::Value) {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let rec = EventRecord {
            ts_unix_ms: now_ms,
            kind,
            details,
        };
        let line = match serde_json::to_string(&rec) {
            Ok(mut s) => {
                s.push('\n');
                s
            }
            Err(e) => {
                tracing::warn!(?e, "events serialise");
                return;
            }
        };
        if let Err(e) = self.events_file.write_all(line.as_bytes()).await {
            tracing::warn!(?e, "events write");
            return;
        }
        let _ = self.events_file.flush().await;
    }

    pub async fn close(mut self, reason: &str) {
        // Stop snapshot writer.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.snapshot_task.take() {
            let _ = task.await;
        }
        let _ = self.events_file.flush().await;

        // Finalise metadata.
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut meta = self.metadata.clone();
        meta.stop_unix_ms = Some(now_ms);
        meta.stop_iso_utc = Some(iso_human(now_ms));
        meta.duration_s =
            Some((now_ms.saturating_sub(meta.start_unix_ms)) as f64 / 1000.0);
        meta.stop_reason = Some(reason.to_string());

        if let Err(e) = write_metadata(&self.dir, &meta).await {
            tracing::warn!(?e, "metadata finalise");
        }

        tracing::info!(
            dir = %self.dir.display(),
            reason,
            duration_s = meta.duration_s.unwrap_or(0.0),
            "capture: session closed"
        );
    }

    #[allow(dead_code)]
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

async fn write_metadata(dir: &Path, meta: &SessionMetadata) -> Result<()> {
    let json = serde_json::to_string_pretty(meta).context("metadata serialise")?;
    fs::write(dir.join("metadata.json"), json)
        .await
        .context("metadata write")?;
    Ok(())
}

async fn snapshot_writer(
    mut file: fs::File,
    mut rx: watch::Receiver<Snapshot>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            r = rx.changed() => {
                if r.is_err() { break; }
                let snap = rx.borrow_and_update().clone();
                let mut line = match serde_json::to_string(&snap) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(?e, "snapshot serialise");
                        continue;
                    }
                };
                line.push('\n');
                if let Err(e) = file.write_all(line.as_bytes()).await {
                    tracing::warn!(?e, "snapshot write");
                    break;
                }
            }
        }
    }
    let _ = file.flush().await;
}

// ── retention ───────────────────────────────────────────────────────────────

/// Synchronous on purpose — runs once at process startup before tokio is busy.
pub fn run_retention_sweep(cfg: &CaptureCfg) -> Result<()> {
    let sessions_dir = cfg.data_dir.join("sessions");
    if !sessions_dir.exists() {
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(&sessions_dir)
        .with_context(|| format!("read_dir({})", sessions_dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    // Newest first.
    entries.sort_by_key(|e| {
        e.metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH)
    });
    entries.reverse();

    let mut deleted = 0;
    for entry in entries.iter().skip(cfg.retention_sessions) {
        let path = entry.path();
        match std::fs::remove_dir_all(&path) {
            Ok(()) => deleted += 1,
            Err(e) => tracing::warn!(?path, ?e, "retention: failed to remove"),
        }
    }
    if deleted > 0 || entries.len() > cfg.retention_sessions {
        tracing::info!(
            kept = cfg.retention_sessions.min(entries.len()),
            deleted,
            "retention sweep done"
        );
    }
    Ok(())
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn sanitise(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(64)
        .collect();
    if cleaned.is_empty() {
        "anon".to_string()
    } else {
        cleaned
    }
}

/// Compact ISO 8601 for filenames: `YYYYMMDDTHHMMSSZ`.
fn iso_basic(ms: u64) -> String {
    let (y, mo, d, h, mi, s) = ymdhms(ms / 1000);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// Human RFC3339 (UTC, ms precision): `YYYY-MM-DDTHH:MM:SS.fffZ`.
fn iso_human(ms: u64) -> String {
    let (y, mo, d, h, mi, s) = ymdhms(ms / 1000);
    let frac = ms % 1000;
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:03}Z")
}

/// Civil from days — Howard Hinnant's algorithm.
fn ymdhms(epoch_secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (epoch_secs / 86400) as i64;
    let secs_of_day = (epoch_secs % 86400) as u32;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i32 + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y_final = if mo <= 2 { y + 1 } else { y };
    (
        y_final,
        mo,
        d,
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60,
    )
}
