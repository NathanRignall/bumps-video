#!/usr/bin/env bash
#
# run-stable.sh — launch bumps-pipe with the known-good stability config.
#
# All SRT path params (latency, oheadbw, maxbw, socket buffers, peer idle
# timeout) are baked into the binary now, so this script's only job is to
# pass three flags and point at the relay.
#
# Override via environment:
#   BUMPS_BITRATE_KBPS=6000 ./scripts/run-stable.sh
#   BUMPS_SRT_HOST=1.2.3.4 ./scripts/run-stable.sh
#   BUMPS_ENCODER=x264 ./scripts/run-stable.sh
#
# Viewer URL (paste into ffplay / VLC / OBS):
#   srt://<host>:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000&peeridletimeo=30000

set -euo pipefail

cd "$(dirname "$0")/.."

: "${BUMPS_SRT_HOST:=3.11.124.82}"
: "${BUMPS_SRT_PORT:=9999}"
: "${BUMPS_BITRATE_KBPS:=8000}"
: "${BUMPS_ENCODER:=va-hevc}"

echo "== bumps-pipe stable launcher =="
echo "  encoder    : ${BUMPS_ENCODER}"
echo "  bitrate    : ${BUMPS_BITRATE_KBPS} kbps"
echo "  relay      : srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}"
echo "  viewer URL : srt://${BUMPS_SRT_HOST}:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000&peeridletimeo=30000"
echo

exec cargo run --release -- \
  --encoder "${BUMPS_ENCODER}" \
  --bitrate-kbps "${BUMPS_BITRATE_KBPS}" \
  --srt-uri "srt://${BUMPS_SRT_HOST}:${BUMPS_SRT_PORT}" \
  "$@"
