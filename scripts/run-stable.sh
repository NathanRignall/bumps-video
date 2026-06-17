#!/usr/bin/env bash
#
# run-stable.sh — launch bumps-pipe with the known-good stability config.
#
# Defaults to libx264 software encode at 3 Mbps CBR — sized for a Starlink
# Mini uplink with retransmit headroom. The actual SRT send rate ends up
# ~4 Mbps once MPEG-TS (10 %) and SRT headers (~5 %) are added, which
# fits comfortably inside a typical 5-10 Mbps Starlink uplink and
# leaves room for SRT to retransmit lost packets during handovers.
#
# Override via environment:
#   BUMPS_BITRATE_KBPS=6000 ./scripts/run-stable.sh   # fibre / dev testing
#   BUMPS_BITRATE_KBPS=2000 ./scripts/run-stable.sh   # cautious / weak signal
#   BUMPS_SRT_HOST=1.2.3.4 ./scripts/run-stable.sh
#   BUMPS_ENCODER=va-hevc ./scripts/run-stable.sh     # hardware HEVC (less stable)
#
# Viewer URL (paste into ffplay / VLC / OBS):
#   srt://<host>:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000
#
# Bandwidth budget rule of thumb:
#   encoder_kbps × 1.30 ≈ SRT send_kbps under normal conditions
#   encoder_kbps × 2.00 ≈ peak SRT send_kbps during retransmit storm
# Pick `encoder_kbps` so the peak fits well below your uplink ceiling.

set -euo pipefail

cd "$(dirname "$0")/.."

: "${BUMPS_SRT_HOST:=3.11.124.82}"
: "${BUMPS_SRT_PORT:=9999}"
: "${BUMPS_BITRATE_KBPS:=3000}"
: "${BUMPS_ENCODER:=x264}"

echo "== bumps-pipe stable launcher =="
echo "  encoder    : ${BUMPS_ENCODER}"
echo "  bitrate    : ${BUMPS_BITRATE_KBPS} kbps (CBR)"
echo "  est. send  : ~$((BUMPS_BITRATE_KBPS * 13 / 10))-$((BUMPS_BITRATE_KBPS * 2)) kbps (with overhead + retransmits)"
echo "  relay      : srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}"
echo "  viewer URL : srt://${BUMPS_SRT_HOST}:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000"
echo

exec cargo run --release -- \
  --encoder "${BUMPS_ENCODER}" \
  --bitrate-kbps "${BUMPS_BITRATE_KBPS}" \
  --srt-uri "srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}" \
  "$@"
