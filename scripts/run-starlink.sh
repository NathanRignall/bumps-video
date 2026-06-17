#!/usr/bin/env bash
#
# run-starlink.sh — bumps-pipe tuned for a Starlink Mini uplink.
#
# Differences from run-stable.sh (which is the fibre/dev baseline):
#   * Adaptive bitrate is ON. Starlink handovers cause brief packet-loss
#     spikes every ~15 s; the adapter steps the encoder down by 25 % when
#     it sees sustained loss and steps back up after 30 s of clean stats.
#     Without adapt those losses pile into the retransmit budget and
#     eventually overflow buffers.
#   * Bitrate envelope is sized for Starlink Mini's typical 5-10 Mbps
#     uplink with retransmit headroom on top of every value:
#       - nominal 3000 kbps   → ~4 Mbps SRT send under normal conditions
#       - floor   1500 kbps   → ~2 Mbps emergency rate during sustained
#                               congestion; still watchable
#       - ceiling 4000 kbps   → ~5 Mbps peak when the link is clean,
#                               well below the Mini's worst-case uplink
#   * VA-API HEVC hardware encoder. Once the surrounding pipeline was
#     fixed (timestamp flattener, videorate cadence smoother, srtsink
#     sync=false, CBR with cpb-size pinned), vah265enc behaved cleanly
#     — and it gives ~30 % better quality than x264 at the same bitrate
#     at near-zero CPU cost. If it ever regresses in the field, fall
#     back to `BUMPS_ENCODER=x264` for the software path.
#
# Override via environment:
#   BUMPS_SRT_HOST=1.2.3.4 ./scripts/run-starlink.sh
#   BUMPS_BITRATE_KBPS=4000 ./scripts/run-starlink.sh   # tighter signal margin
#   BUMPS_MIN_KBPS=1000     ./scripts/run-starlink.sh   # lower emergency floor
#   BUMPS_MAX_KBPS=5000     ./scripts/run-starlink.sh   # higher ceiling
#
# Viewer URL (paste into ffplay / VLC / OBS):
#   srt://<host>:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000
#
# What to watch on the dashboard:
#   * Encoder `target` line — adapter's current chosen bitrate. Should
#     move slowly; the `↑N ↓M` counters show step-up / step-down history.
#   * Uplink `loss %` and `retrans` — the signals the adapter reacts to.
#     A handover typically registers as loss spike → adapter step-down
#     within 5 s → loss subsides → step-up after 30 s of clean stats.

set -euo pipefail

cd "$(dirname "$0")/.."

: "${BUMPS_SRT_HOST:=3.11.124.82}"
: "${BUMPS_SRT_PORT:=9999}"
: "${BUMPS_BITRATE_KBPS:=3000}"
: "${BUMPS_MIN_KBPS:=1500}"
: "${BUMPS_MAX_KBPS:=4000}"
: "${BUMPS_ENCODER:=va-hevc}"

echo "== bumps-pipe Starlink launcher =="
echo "  encoder    : ${BUMPS_ENCODER}"
echo "  bitrate    : ${BUMPS_BITRATE_KBPS} kbps nominal (adaptive, ${BUMPS_MIN_KBPS}-${BUMPS_MAX_KBPS} kbps)"
echo "  est. send  : ~$((BUMPS_BITRATE_KBPS * 13 / 10))-$((BUMPS_MAX_KBPS * 2)) kbps (with overhead + retransmits)"
echo "  relay      : srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}"
echo "  viewer URL : srt://${BUMPS_SRT_HOST}:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000"
echo

exec cargo run --release -- \
  --encoder "${BUMPS_ENCODER}" \
  --bitrate-kbps "${BUMPS_BITRATE_KBPS}" \
  --min-bitrate-kbps "${BUMPS_MIN_KBPS}" \
  --max-bitrate-kbps "${BUMPS_MAX_KBPS}" \
  --adapt \
  --srt-uri "srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}" \
  "$@"
