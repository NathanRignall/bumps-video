//! Wire protocol between the Rust pipeline and the browser dashboard.

use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::stats::Snapshot;

/// Server → Client JSON messages.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServerMsg {
    Init(InitInfo),
    Stats(Box<Snapshot>),
    #[allow(dead_code)] // surfaced in a later phase (event banner)
    Event(EventMsg),
}

/// Codec/resolution info needed by the browser's VideoDecoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InitInfo {
    /// WebCodecs codec string, e.g. "hev1.1.6.L93.B0" or "avc1.42E01F".
    pub codec: String,
    pub width: u32,
    pub height: u32,
    pub fps_num: u32,
    pub fps_den: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventMsg {
    pub severity: String,
    pub message: String,
}

/// Client → Server JSON commands from the dashboard's operator controls.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClientMsg {
    /// Force the encoder to emit an IDR keyframe immediately.
    RequestKeyframe,
    /// Pin the encoder bitrate to a specific value, disabling adapter
    /// step-up/step-down until cleared. `kbps` is clamped to the adapter's
    /// configured [min, max] range.
    SetBitrate { kbps: u32 },
    /// Release a previous `SetBitrate` pin; adapter resumes normal operation.
    ClearBitrateOverride,
    /// Tear down the current pipeline and rebuild it immediately. Brief
    /// preview gap. Useful when the operator wants to force a clean state.
    RestartPipeline,
}

/// One encoded access unit destined for the browser preview.
#[derive(Debug, Clone)]
pub struct FrameChunk {
    pub pts_us: u64,
    pub is_keyframe: bool,
    pub data: Bytes,
}

impl FrameChunk {
    /// Binary wire format: `[flags:u8 | pts_us:u64 LE | payload bytes...]`.
    /// Flags bit 0 = keyframe.
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(9 + self.data.len());
        b.put_u8(if self.is_keyframe { 0x01 } else { 0x00 });
        b.put_u64_le(self.pts_us);
        b.put_slice(&self.data);
        b.freeze()
    }
}
