#!/usr/bin/env bash
#
# record-rtmp.sh
#
# Listen for an RTMP push (from the DJI Fly app on your phone) and record:
#   - the raw stream verbatim (stream.flv)        -> exact bytes the publisher sent
#   - per-packet metadata (packets.json)          -> ffprobe dump for offline analysis
#   - session metadata (session.json)             -> wallclock at start/stop, host
#   - ffmpeg listener log (ffmpeg.log)            -> reconnects, errors
#   - optional wire capture (wire.pcap)           -> tcpdump for true arrival times
#
# Usage:
#   ./record-rtmp.sh                  # listen on default rtmp://0.0.0.0:1935/live/drone
#   PORT=1935 APP=live KEY=drone ./record-rtmp.sh
#   PCAP=1 ./record-rtmp.sh           # also capture wire pcap (sudo prompt)
#   OUT_DIR=./captures/test1 ./record-rtmp.sh
#
# In DJI Fly: Transmission -> Live Streaming Platforms -> RTMP, enter:
#   rtmp://<this-machine-ip>:1935/live/drone
#
# Stop with Ctrl-C (or by stopping the stream in DJI Fly). After ffmpeg exits,
# the script post-processes the FLV to emit packets.json.
#
# Prereqs:  brew install ffmpeg
#

set -uo pipefail

PORT="${PORT:-1935}"
APP="${APP:-live}"
KEY="${KEY:-drone}"
BIND="${BIND:-0.0.0.0}"
OUT_DIR="${OUT_DIR:-./captures/$(date -u +%Y%m%dT%H%M%SZ)}"
PCAP="${PCAP:-0}"
IFACE="${IFACE:-en0}"

# --- preflight ---------------------------------------------------------------
for bin in ffmpeg ffprobe python3; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "error: '$bin' not found in PATH. Install with: brew install ffmpeg" >&2
    exit 1
  fi
done

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

# --- session metadata --------------------------------------------------------
python3 - "$OUT_DIR_ABS" "$PORT" "$APP" "$KEY" <<'PY' > "$OUT_DIR_ABS/session.json"
import json, os, platform, socket, sys, time
out_dir, port, app, key = sys.argv[1:5]
now_ns = time.time_ns()
print(json.dumps({
    "out_dir": out_dir,
    "start_wallclock_unix_ns": now_ns,
    "start_wallclock_iso_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now_ns / 1e9))
                                + f".{now_ns % 1_000_000_000:09d}Z",
    "host": socket.gethostname(),
    "platform": platform.platform(),
    "rtmp_url_listen": f"rtmp://0.0.0.0:{port}/{app}/{key}",
    "phase": "start",
}, indent=2))
PY

LOCAL_IPS=$(ipconfig getifaddr en0 2>/dev/null; ipconfig getifaddr en1 2>/dev/null)
echo ""
echo "============================================================"
echo "  Recording RTMP capture session"
echo "  Output dir : $OUT_DIR_ABS"
echo "  Listening  : rtmp://${BIND}:${PORT}/${APP}/${KEY}"
echo "  Push to    : rtmp://<host-ip>:${PORT}/${APP}/${KEY}"
[[ -n "$LOCAL_IPS" ]] && echo "  Likely IPs : $(echo $LOCAL_IPS | tr '\n' ' ')"
echo "  Stop with  : Ctrl-C, or stop the stream in DJI Fly"
echo "============================================================"
echo ""

# --- optional wire capture ---------------------------------------------------
TCPDUMP_PID=""
if [[ "$PCAP" == "1" ]]; then
  echo "Starting tcpdump on $IFACE (sudo may prompt for password)..."
  sudo -v
  sudo tcpdump -i "$IFACE" -s 0 -w "$OUT_DIR_ABS/wire.pcap" "tcp port $PORT" &
  TCPDUMP_PID=$!
  sleep 0.3
fi

cleanup() {
  if [[ -n "$TCPDUMP_PID" ]] && kill -0 "$TCPDUMP_PID" 2>/dev/null; then
    echo "Stopping tcpdump (pid $TCPDUMP_PID)..."
    sudo kill -INT "$TCPDUMP_PID" 2>/dev/null || true
    wait "$TCPDUMP_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# --- RTMP listener (records FLV verbatim, preserves publisher timestamps) ---
# -c copy   : no transcoding, bitstream-exact
# -listen 1 : ffmpeg acts as an RTMP server
# -rw_timeout: 0 -> wait forever for connection
echo "ffmpeg listening... (waiting for DJI Fly to start streaming)"
ffmpeg -hide_banner -nostdin \
  -listen 1 -rw_timeout 0 \
  -i "rtmp://${BIND}:${PORT}/${APP}/${KEY}" \
  -c copy \
  -f flv "$OUT_DIR_ABS/stream.flv" \
  2> "$OUT_DIR_ABS/ffmpeg.log"
FFMPEG_RC=$?

echo ""
echo "ffmpeg exited (rc=$FFMPEG_RC). Extracting per-packet metadata..."

# --- per-packet dump (offline pass over the FLV) -----------------------------
# Captures: stream_index, pts, pts_time, dts, dts_time, duration, size, flags, pos
# size+flags let you see keyframes; pos is byte offset within the FLV.
ffprobe -hide_banner -loglevel error \
  -show_streams \
  -show_packets \
  -show_entries \
    'stream=index,codec_type,codec_name,time_base,r_frame_rate,avg_frame_rate,start_time:packet=stream_index,pts,pts_time,dts,dts_time,duration,duration_time,size,flags,pos' \
  -of json \
  "$OUT_DIR_ABS/stream.flv" \
  > "$OUT_DIR_ABS/packets.json" \
  2> "$OUT_DIR_ABS/ffprobe.log" || true

# --- finalise session metadata ----------------------------------------------
python3 - "$OUT_DIR_ABS" "$FFMPEG_RC" <<'PY'
import json, os, sys, time
out_dir, rc = sys.argv[1], int(sys.argv[2])
path = os.path.join(out_dir, "session.json")
with open(path) as f:
    meta = json.load(f)
now_ns = time.time_ns()
meta["stop_wallclock_unix_ns"] = now_ns
meta["stop_wallclock_iso_utc"] = (
    time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now_ns / 1e9))
    + f".{now_ns % 1_000_000_000:09d}Z"
)
meta["duration_seconds"] = (now_ns - meta["start_wallclock_unix_ns"]) / 1e9
meta["ffmpeg_exit_code"] = rc
meta["phase"] = "complete"
flv = os.path.join(out_dir, "stream.flv")
meta["flv_bytes"] = os.path.getsize(flv) if os.path.exists(flv) else 0
with open(path, "w") as f:
    json.dump(meta, f, indent=2)
PY

# --- tiny sanity summary (not analysis — just confirms recording worked) ----
python3 - "$OUT_DIR_ABS/packets.json" <<'PY' || true
import json, sys
from collections import defaultdict
try:
    p = json.load(open(sys.argv[1]))
except Exception as e:
    print(f"  (could not parse packets.json: {e})")
    sys.exit(0)
streams = p.get("streams", [])
pkts = p.get("packets", [])
print(f"  streams: {len(streams)}, packets: {len(pkts)}")
for s in streams:
    print(f"    [{s.get('index')}] {s.get('codec_type')}/{s.get('codec_name')}  time_base={s.get('time_base')}")
by_s = defaultdict(list)
for k in pkts:
    by_s[k.get("stream_index")].append(k)
for idx, items in sorted(by_s.items()):
    def ints(field):
        out = []
        for it in items:
            v = it.get(field)
            if v is None or v == "N/A": continue
            try: out.append(int(v))
            except: pass
        return out
    ptss, dtss = ints("pts"), ints("dts")
    line = f"    stream {idx}: {len(items)} pkts"
    if ptss:
        diffs = [b - a for a, b in zip(ptss, ptss[1:])]
        line += f", pts {min(ptss)}..{max(ptss)}, backward jumps: {sum(1 for d in diffs if d < 0)}"
    print(line)
PY

echo ""
echo "Done. Artifacts:"
echo "  $OUT_DIR_ABS/stream.flv     - raw publisher bytes"
echo "  $OUT_DIR_ABS/packets.json   - per-packet pts/dts/size/flags"
echo "  $OUT_DIR_ABS/session.json   - wallclock + run metadata"
echo "  $OUT_DIR_ABS/ffmpeg.log     - listener log (look here for reconnects)"
[[ "$PCAP" == "1" ]] && echo "  $OUT_DIR_ABS/wire.pcap      - wire arrivals (use tshark/Wireshark)"
