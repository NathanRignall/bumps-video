#!/usr/bin/env bash
#
# test-from-drone.sh
#
# Live test against a real drone via DJI Fly. No synthetic publisher.
#
# Starts:
#   1) SRT receiver — gst-launch listener, dumps encoded HEVC to disk
#   2) bumps-pipe   — RTMP listener + stats collector + AWS ping + dashboard
#
# Then prints the RTMP URL to enter on the phone and the dashboard URL to
# open in a browser.
#
# Phone setup:
#   - Phone must be on the same WiFi network as this Mac.
#   - DJI Fly  →  Transmission  →  Live Streaming Platforms  →  RTMP
#   - URL:     rtmp://<your-mac-ip>:1935/live/drone
#   - Tap "Go Live".
#
# Artifacts (per run, under ./test-runs/<ts>-drone/):
#   - srt-output.ts         — the encoded HEVC bytes that reached the receiver
#   - bumps-pipe.log        — full process log
#   - srt-receiver.log      — gst-launch log
#   - bumps-data/sessions/  — one dir per publisher session: metadata, stats, events
#
# Ctrl-C to stop. Multiple "Go Live → stop" cycles all land in the same run dir.
#

set -uo pipefail

# ── config (overrideable via env) ─────────────────────────────────────────
RTMP_LISTEN="${RTMP_LISTEN:-0.0.0.0:1935}"
RTMP_KEY="${RTMP_KEY:-drone}"
SRT_PORT="${SRT_PORT:-9999}"
ENCODE_KBPS="${ENCODE_KBPS:-5000}"
# Default loopback; set WEB_LISTEN=0.0.0.0:8080 to also view from phone/laptop on LAN.
WEB_LISTEN="${WEB_LISTEN:-127.0.0.1:8080}"
PING_TARGET="${PING_TARGET:-s3.eu-west-2.amazonaws.com:443}"
OUT_DIR="${OUT_DIR:-./test-runs/$(date -u +%Y%m%dT%H%M%SZ)-drone}"

# ── LAN-IP discovery (for the instructions block) ─────────────────────────
LAN_IP="${LAN_IP:-}"
LAN_IFACE=""
if [[ -z "$LAN_IP" ]]; then
  for iface in en0 en1 en2; do
    ip=$(ipconfig getifaddr "$iface" 2>/dev/null || true)
    if [[ -n "$ip" ]]; then
      LAN_IP="$ip"
      LAN_IFACE="$iface"
      break
    fi
  done
fi
LAN_IP="${LAN_IP:-<your-mac-ip>}"

# ── preflight ─────────────────────────────────────────────────────────────
for bin in cargo gst-launch-1.0; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: '$bin' not in PATH. Run inside 'nix develop'." >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

# ── build ─────────────────────────────────────────────────────────────────
echo "[harness] building bumps-pipe..."
if ! cargo build --bin bumps-pipe 2>"$OUT_DIR_ABS/cargo-build.log"; then
  echo "error: cargo build failed; see $OUT_DIR_ABS/cargo-build.log" >&2
  tail -20 "$OUT_DIR_ABS/cargo-build.log" >&2
  exit 1
fi
BIN="$(pwd)/target/debug/bumps-pipe"

# ── process bookkeeping ───────────────────────────────────────────────────
PIDS=()
note() { printf "[harness] %s\n" "$*"; }

cleanup() {
  trap - INT TERM EXIT
  echo ""
  note "stopping (${#PIDS[@]} processes)…"
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for _ in 1 2 3 4 5; do
    local still_up=0
    for pid in "${PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null && still_up=1
    done
    [[ "$still_up" -eq 0 ]] && break
    sleep 0.4
  done
  for pid in "${PIDS[@]}"; do
    kill -KILL "$pid" 2>/dev/null || true
  done

  echo ""
  echo "──────────────────────────────────────────────────────────────"
  echo "  Run finished. Artifacts: $OUT_DIR_ABS"
  echo "──────────────────────────────────────────────────────────────"
  if [[ -f "$OUT_DIR_ABS/srt-output.ts" ]]; then
    local size
    size=$(stat -f%z "$OUT_DIR_ABS/srt-output.ts" 2>/dev/null \
        || stat -c%s "$OUT_DIR_ABS/srt-output.ts" 2>/dev/null \
        || echo 0)
    printf "  srt-output.ts                       : %s bytes\n" "$size"
  fi
  if [[ -d "$OUT_DIR_ABS/bumps-data/sessions" ]]; then
    local nsess
    nsess=$(find "$OUT_DIR_ABS/bumps-data/sessions" -maxdepth 1 -type d ! -path "$OUT_DIR_ABS/bumps-data/sessions" 2>/dev/null | wc -l | tr -d ' ')
    printf "  publisher sessions captured         : %s\n" "$nsess"
    for s in "$OUT_DIR_ABS/bumps-data/sessions"/*/; do
      [[ -d "$s" ]] || continue
      local lines duration
      lines=$(wc -l < "$s/snapshot.jsonl" 2>/dev/null | tr -d ' ' || echo 0)
      duration=$(python3 -c "import json,sys; d=json.load(open('$s/metadata.json')); print(d.get('duration_s'))" 2>/dev/null || echo "?")
      printf "    %s  %ss  (%s snapshots)\n" "$(basename "$s")" "$duration" "$lines"
    done
  fi
  echo ""
}
trap cleanup INT TERM EXIT

spawn() {
  local name="$1"; shift
  "$@" >"$OUT_DIR_ABS/${name}.log" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  note "started $name (pid $pid)"
}

# ── 1) SRT receiver ───────────────────────────────────────────────────────
spawn srt-receiver gst-launch-1.0 -q \
  srtsrc uri="srt://0.0.0.0:${SRT_PORT}?mode=listener&latency=300" \
  ! filesink location="$OUT_DIR_ABS/srt-output.ts" sync=false
sleep 0.5

# ── 2) bumps-pipe ─────────────────────────────────────────────────────────
spawn bumps-pipe "$BIN" \
  --rtmp-listen "$RTMP_LISTEN" \
  --srt-uri "srt://127.0.0.1:${SRT_PORT}?mode=caller&latency=300" \
  --bitrate-kbps "$ENCODE_KBPS" \
  --web-listen "$WEB_LISTEN" \
  --ping-target "$PING_TARGET" \
  --data-dir "$OUT_DIR_ABS/bumps-data"
BUMPS_PID="${PIDS[-1]}"
sleep 2

if ! kill -0 "$BUMPS_PID" 2>/dev/null; then
  note "bumps-pipe died at startup; tail of log:"
  tail -30 "$OUT_DIR_ABS/bumps-pipe.log"
  exit 1
fi

# ── instructions ──────────────────────────────────────────────────────────
cat <<EOF

──────────────────────────────────────────────────────────────
  bumps-pipe is up.

  Phone setup (must be on the same WiFi as this Mac):
    1. Open DJI Fly
    2. Transmission → Live Streaming Platforms → RTMP
    3. URL:   rtmp://${LAN_IP}:1935/live/${RTMP_KEY}
    4. Tap "Go Live"

  Dashboard:
    From this Mac : http://127.0.0.1:8080/
EOF
if [[ "$WEB_LISTEN" != 127.0.0.1:* ]]; then
  cat <<EOF
    From LAN      : http://${LAN_IP}:8080/
EOF
else
  cat <<EOF
    LAN access    : disabled by default. Add  WEB_LISTEN=0.0.0.0:8080
                    if you want to view from your phone or another device.
EOF
fi
cat <<EOF

  Notes:
    • macOS may prompt to allow incoming connections on port 1935.
      Phone can't reach you? Check System Settings → Network → Firewall.
    • LAN IP detected on iface  : ${LAN_IFACE:-<not detected>}
      Override if wrong         : LAN_IP=192.168.x.y ./scripts/test-from-drone.sh
    • AWS ping target           : ${PING_TARGET}
    • Artifacts grow under      :
        ${OUT_DIR_ABS}

  Ctrl-C to stop. You can "Go Live" / "Stop" multiple times during this run;
  each cycle becomes its own session directory under bumps-data/sessions/.
──────────────────────────────────────────────────────────────

EOF

# ── live event tail ───────────────────────────────────────────────────────
# Surface interesting publisher/pipeline events so you know what's happening
# without having to keep eyes on the dashboard.
(
  tail -F "$OUT_DIR_ABS/bumps-pipe.log" 2>/dev/null | \
  grep --line-buffered -E \
    'rtmp tcp accepted|rtmp publish|pipeline: session start|pipeline: PLAYING|pipeline: session end|publish finished|rtmp connection ended|capture: session opened|capture: session closed|pipeline error' | \
  while IFS= read -r line; do
    # strip ANSI color codes that tracing emits for terminal output
    clean=$(echo "$line" | sed -E 's/\x1b\[[0-9;]*m//g')
    echo "[$(date +%H:%M:%S)] $clean"
  done
) &
TAIL_PID=$!
PIDS+=("$TAIL_PID")

# ── wait ──────────────────────────────────────────────────────────────────
wait "$BUMPS_PID"
