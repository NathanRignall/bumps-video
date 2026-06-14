#!/usr/bin/env bash
#
# test-loopback.sh
#
# End-to-end loopback test for bumps-pipe without a phone or drone.
#
# Spawns three processes on localhost:
#   1) SRT receiver    — ffmpeg in listener mode, records output to .ts file
#   2) bumps-pipe      — RTMP listener → flatten → QSV encode → SRT caller
#   3) RTMP publisher  — ffmpeg pushes a generated source to bumps-pipe
#
# All processes are killed on Ctrl-C / EXIT. Logs + recorded SRT output go
# under ./test-runs/<timestamp>/.
#
# Usage (most things default to sensible values):
#
#   ./scripts/test-loopback.sh                       # 30s smpte bars
#   DURATION=0 ./scripts/test-loopback.sh            # run until Ctrl-C
#   SOURCE=testsrc ./scripts/test-loopback.sh        # ffmpeg testsrc instead
#   DURATION=60 SRC_W=1920 SRC_H=1080 \
#     ./scripts/test-loopback.sh                     # 1m of 1080p
#
#   # Test with a real video file (looped). MP4s are usually H.264 already, so
#   # COPY=1 streams the source bitstream as-is (faster, preserves quality).
#   # Without COPY=1, ffmpeg re-encodes with libx264 to normalise bitrate/GOP.
#   SOURCE=./flight.mp4 DURATION=0 ./scripts/test-loopback.sh
#   SOURCE=./flight.mp4 COPY=1 ./scripts/test-loopback.sh
#
#   NO_RECEIVER=1 ./scripts/test-loopback.sh         # bring your own SRT sink
#   NO_PUBLISHER=1 ./scripts/test-loopback.sh        # use a real phone instead
#
# Validation: after the run, the script prints whether srt-output.ts grew and
# whether bumps-pipe ever reached PLAYING. Non-zero exit = something is wrong.
#

set -uo pipefail

# ── config (overrideable via env) ─────────────────────────────────────────
RTMP_LISTEN="${RTMP_LISTEN:-127.0.0.1:1935}"
RTMP_PUSH_URL="${RTMP_PUSH_URL:-rtmp://127.0.0.1:1935/live/drone}"
SRT_PORT="${SRT_PORT:-9999}"
SRT_LATENCY_MS="${SRT_LATENCY_MS:-300}"
DURATION="${DURATION:-30}"
SOURCE="${SOURCE:-smpte}"
SRC_W="${SRC_W:-1280}"
SRC_H="${SRC_H:-720}"
SRC_FPS="${SRC_FPS:-30}"
SRC_KBPS="${SRC_KBPS:-4000}"
ENCODE_KBPS="${ENCODE_KBPS:-5000}"
NO_RECEIVER="${NO_RECEIVER:-0}"
NO_PUBLISHER="${NO_PUBLISHER:-0}"
COPY="${COPY:-0}"        # 1 = -c:v copy (only valid for H.264 file sources)
OUT_DIR="${OUT_DIR:-./test-runs/$(date -u +%Y%m%dT%H%M%SZ)}"

# ── preflight ─────────────────────────────────────────────────────────────
for bin in ffmpeg cargo; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: '$bin' not in PATH. Run inside 'nix develop'." >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

# Default encoder follows the binary's own per-OS default; override with
# ENCODER=qsv-hevc / vt-hevc / x264 (or --encoder when invoking the binary).
ENCODER="${ENCODER:-}"

# ── process bookkeeping ───────────────────────────────────────────────────
PIDS=()
declare -A NAME_BY_PID=()
HARNESS_RC=0

note() { printf "[harness] %s\n" "$*"; }

cleanup() {
  trap - INT TERM EXIT
  note "cleanup; stopping ${#PIDS[@]} process(es)"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  # Give them a moment, then SIGKILL anything that's still up.
  local i
  for i in 1 2 3 4 5; do
    local still_up=0
    for pid in "${PIDS[@]}"; do
      kill -0 "$pid" 2>/dev/null && still_up=1
    done
    [[ "$still_up" -eq 0 ]] && break
    sleep 0.5
  done
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done

  # Final summary.
  echo ""
  echo "============================================================"
  echo "  Test run summary — $OUT_DIR_ABS"
  echo "============================================================"
  if [[ -f "$OUT_DIR_ABS/srt-output.ts" ]]; then
    local size
    size=$(stat -c%s "$OUT_DIR_ABS/srt-output.ts" 2>/dev/null \
        || stat -f%z "$OUT_DIR_ABS/srt-output.ts" 2>/dev/null \
        || echo 0)
    printf "  srt-output.ts   : %s bytes\n" "$size"
    if [[ "$size" -lt 4096 ]]; then
      echo "                    (small/empty — likely nothing reached the receiver)"
      HARNESS_RC=1
    fi
  fi
  if [[ -f "$OUT_DIR_ABS/bumps-pipe.log" ]]; then
    if grep -q "pipeline: PLAYING" "$OUT_DIR_ABS/bumps-pipe.log"; then
      echo "  bumps-pipe      : reached PLAYING ✓"
    else
      echo "  bumps-pipe      : never reached PLAYING ✗"
      HARNESS_RC=1
    fi
    local errs
    errs=$(grep -cE 'ERROR|pipeline error|failed' "$OUT_DIR_ABS/bumps-pipe.log" || true)
    printf "  bumps-pipe.log  : %s ERROR/failed lines\n" "$errs"
  fi
  echo "  artifacts:"
  ls -la "$OUT_DIR_ABS" | tail -n +2 | sed 's/^/    /'
  echo "============================================================"
  exit "$HARNESS_RC"
}
trap cleanup INT TERM EXIT

spawn() {
  local name="$1"; shift
  "$@" >"$OUT_DIR_ABS/${name}.log" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  NAME_BY_PID[$pid]="$name"
  note "started $name (pid $pid) — log: $OUT_DIR_ABS/${name}.log"
}

# ── build ─────────────────────────────────────────────────────────────────
note "building bumps-pipe (cargo build)…"
if ! cargo build --bin bumps-pipe 2>"$OUT_DIR_ABS/cargo-build.log"; then
  echo "error: cargo build failed; see $OUT_DIR_ABS/cargo-build.log" >&2
  tail -20 "$OUT_DIR_ABS/cargo-build.log" >&2
  exit 1
fi
BUMPS_BIN="$(pwd)/target/debug/bumps-pipe"
[[ -x "$BUMPS_BIN" ]] || { echo "error: $BUMPS_BIN missing"; exit 1; }

cat <<EOF

============================================================
  bumps-pipe loopback test
  Out dir        : $OUT_DIR_ABS
  RTMP listen    : $RTMP_LISTEN
  RTMP push URL  : $RTMP_PUSH_URL
  SRT port       : $SRT_PORT (loopback)
  Source         : $SOURCE @ ${SRC_W}x${SRC_H} ${SRC_FPS}fps ${SRC_KBPS}kbps
  Encoder        : ${ENCODER:-<binary default>} @ ${ENCODE_KBPS} kbps
  Duration       : ${DURATION}s ($( [[ "$DURATION" -eq 0 ]] && echo 'until Ctrl-C' || echo 'then auto-stop'))
============================================================

EOF

# ── 1) SRT receiver ───────────────────────────────────────────────────────
# gst-launch is used here rather than ffmpeg because ffmpeg's libsrt input
# refuses to bind a listener on at least nixpkgs ffmpeg 8.1. srtsrc dumps raw
# bytes (MPEG-TS from our pipeline) straight to disk for post-hoc inspection.
if [[ "$NO_RECEIVER" != "1" ]]; then
  spawn srt-receiver gst-launch-1.0 -q \
    srtsrc uri="srt://0.0.0.0:${SRT_PORT}?mode=listener&latency=${SRT_LATENCY_MS}" \
    ! filesink location="$OUT_DIR_ABS/srt-output.ts" sync=false
  sleep 0.5
else
  note "NO_RECEIVER=1 — skipping SRT receiver"
fi

# ── 2) bumps-pipe ─────────────────────────────────────────────────────────
ENCODER_ARGS=()
[[ -n "$ENCODER" ]] && ENCODER_ARGS=(--encoder "$ENCODER")

spawn bumps-pipe "$BUMPS_BIN" \
  --rtmp-listen "$RTMP_LISTEN" \
  --srt-uri "srt://127.0.0.1:${SRT_PORT}?mode=caller&latency=${SRT_LATENCY_MS}" \
  --bitrate-kbps "$ENCODE_KBPS" \
  "${ENCODER_ARGS[@]}"
BUMPS_PID="${PIDS[-1]}"
sleep 1

if ! kill -0 "$BUMPS_PID" 2>/dev/null; then
  note "bumps-pipe died before publisher even started; tail of log:"
  tail -40 "$OUT_DIR_ABS/bumps-pipe.log" >&2
  HARNESS_RC=1
  exit 1
fi

# ── 3) RTMP publisher ─────────────────────────────────────────────────────
if [[ "$NO_PUBLISHER" != "1" ]]; then
  IS_FILE=0
  case "$SOURCE" in
    smpte)
      SRC_ARGS=(-f lavfi -i "smptebars=size=${SRC_W}x${SRC_H}:rate=${SRC_FPS}")
      ;;
    testsrc)
      SRC_ARGS=(-f lavfi -i "testsrc=size=${SRC_W}x${SRC_H}:rate=${SRC_FPS}")
      ;;
    *)
      if [[ -f "$SOURCE" ]]; then
        SRC_ARGS=(-stream_loop -1 -i "$SOURCE")
        IS_FILE=1
        # Probe the source so the operator knows what they're feeding in.
        if command -v ffprobe >/dev/null 2>&1; then
          note "source: $SOURCE"
          ffprobe -hide_banner -v error -select_streams v:0 \
            -show_entries stream=codec_name,width,height,r_frame_rate,bit_rate \
            -of default=nw=1 "$SOURCE" 2>&1 | sed 's/^/         /'
        fi
      else
        echo "error: SOURCE='$SOURCE' is not smpte/testsrc and not an existing file" >&2
        exit 1
      fi
      ;;
  esac

  TIMEBOUND=()
  [[ "$DURATION" -gt 0 ]] && TIMEBOUND=(-t "$DURATION")

  # Encoder selection:
  #   COPY=1                → -c:v copy, source bitstream as-is (only safe for H.264 inputs)
  #   synthetic source      → libx264 (we generated raw frames, need an encoder)
  #   file source w/o COPY  → libx264 to normalise (lets you control bitrate/GOP)
  if [[ "$COPY" == "1" ]]; then
    if [[ "$IS_FILE" != "1" ]]; then
      echo "error: COPY=1 requires a file SOURCE (got '$SOURCE')" >&2
      exit 1
    fi
    ENC_ARGS=(-c:v copy)
    note "publisher: -c:v copy (passthrough; bitrate/GOP come from source)"
  else
    # When the source is a file, normalise to SRC_W×SRC_H @ SRC_FPS via -vf/-r.
    # For lavfi sources the size/rate are already set on the input, but adding
    # these flags is harmless and ensures the output matches DJI-Fly-ish caps.
    ENC_ARGS=(
      -vf "scale=${SRC_W}:${SRC_H}:flags=fast_bilinear,format=yuv420p,fps=${SRC_FPS}"
      -c:v libx264 -preset veryfast -tune zerolatency
      -profile:v baseline -pix_fmt yuv420p
      -b:v "${SRC_KBPS}k" -maxrate "${SRC_KBPS}k" -bufsize "$((SRC_KBPS*2))k"
      -g "$((SRC_FPS*2))" -keyint_min "$((SRC_FPS*2))" -bf 0
    )
    note "publisher: libx264 baseline ${SRC_W}x${SRC_H}@${SRC_FPS} ${SRC_KBPS}kbps GOP $((SRC_FPS*2))"
  fi

  # -an drops audio. bumps-pipe v1 ignores audio anyway, and some inputs have
  # codecs (e.g. AAC) that need extra muxer flags for FLV.
  spawn rtmp-publisher ffmpeg -hide_banner -nostdin -y -re "${SRC_ARGS[@]}" "${TIMEBOUND[@]}" \
    -an "${ENC_ARGS[@]}" \
    -f flv "$RTMP_PUSH_URL"
else
  note "NO_PUBLISHER=1 — push your own RTMP to $RTMP_PUSH_URL"
fi

# ── wait ──────────────────────────────────────────────────────────────────
if [[ "$DURATION" -gt 0 ]]; then
  note "running for ${DURATION}s… (Ctrl-C to stop early)"
  for _ in $(seq 1 "$DURATION"); do
    sleep 1
    if ! kill -0 "$BUMPS_PID" 2>/dev/null; then
      note "bumps-pipe exited; ending early"
      HARNESS_RC=1
      break
    fi
  done
else
  note "running until Ctrl-C (or bumps-pipe exits)…"
  wait "$BUMPS_PID" || true
fi

# EXIT trap does the rest.
