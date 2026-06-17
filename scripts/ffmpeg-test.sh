#!/usr/bin/env bash
#
# ffmpeg-test.sh — minimum-viable HEVC encode + SRT push, zero bumps-pipe.
#
# Strips the whole pipeline out of the equation so we can tell whether the
# problem is in our binary or in the SRT path / relay / viewer. If this
# script produces a clean picture at the receiver, the bug is in bumps-pipe.
# If this also stutters, the bug is downstream (SRT path, MediaConnect, or
# viewer setup).
#
# By default it uses a synthetic SMPTE colour-bars source so there's no
# RTMP publisher to worry about. Override with `SRC=rtmp` to instead
# listen for an RTMP publisher (e.g. your phone) on port 1935.
#
# Usage examples:
#   ./scripts/ffmpeg-test.sh                          # SMPTE bars
#   BITRATE=4000 ./scripts/ffmpeg-test.sh             # lower bitrate
#   SRT_HOST=1.2.3.4 ./scripts/ffmpeg-test.sh         # different relay
#   SRC=rtmp ./scripts/ffmpeg-test.sh                 # listen on RTMP
#   SRC=rtmp CODEC=copy ./scripts/ffmpeg-test.sh      # passthrough (no encode)
#
# Viewer URL (paste into ffplay / VLC / OBS):
#   srt://<host>:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000

set -euo pipefail

: "${SRT_HOST:=3.11.124.82}"
: "${SRT_PORT:=9999}"
: "${BITRATE:=8000}"   # kbps; ignored when CODEC=copy
: "${SRC:=bars}"       # "bars" | "rtmp"
: "${CODEC:=hevc}"     # "hevc" (re-encode) | "copy" (stream-copy, RTMP only)

case "$SRC" in
  bars)
    # `-re` paces input to wallclock so the encoder sees a steady 30fps
    # stream — same shape a real live source would have.
    SRC_ARGS=(-re -f lavfi -i "smptebars=size=1920x1080:rate=30")
    SRC_DESC="SMPTE bars 1920x1080@30 (synthetic)"
    ;;
  rtmp)
    # `-probesize` + `-analyzeduration` give ffmpeg enough headroom to
    # identify the FLV stream before declaring it un-decodable; the DJI
    # Fly publisher can take 1–2 s after connect before the SPS/PPS land.
    # `-fflags +genpts+discardcorrupt` regenerates PTS when the source's
    # timestamps are wonky (the entire reason bumps-pipe exists) and
    # silently drops any NAL units the decoder can't parse instead of
    # failing the whole input.
    SRC_ARGS=(
      -fflags +genpts+discardcorrupt
      -probesize 10000000
      -analyzeduration 10000000
      -listen 1
      -i "rtmp://0.0.0.0:1935/live/drone"
    )
    SRC_DESC="RTMP listener on 0.0.0.0:1935/live/drone — point DJI Fly at this"
    ;;
  *)
    echo "error: SRC must be 'bars' or 'rtmp' (got '$SRC')" >&2
    exit 1
    ;;
esac

# SRT inputbw/maxbw are in BYTES per second, not bits — `kbps × 125` is
# the right conversion. Writing them as bits gave SRT 8× the intended
# bandwidth budget and produced runaway retransmit storms on lossy links.
INPUTBW=$((BITRATE * 125))      # bytes/sec
MAXBW=$((BITRATE * 250))        # 2× headroom, bytes/sec
# Note: `peeridletimeo` is a libsrt option but ffmpeg's URL parser doesn't
# whitelist it — leaving it in produces "Option not found" at startup.
# Default peer-idle is 5 s which is fine for a smoke test.
SRT_URI="srt://${SRT_HOST}:${SRT_PORT}?mode=caller&latency=8000&peerlatency=8000&oheadbw=100&inputbw=${INPUTBW}&maxbw=${MAXBW}&rcvbuf=25000000&sndbuf=25000000&streamid=drone"

cat <<EOF
== ffmpeg SRT smoke test ==
  source     : ${SRC_DESC}
  bitrate    : ${BITRATE} kbps (CBR)
  GOP        : 30 frames (1 s)
  SRT target : ${SRT_URI}
  viewer URL : srt://${SRT_HOST}:9998?mode=caller&latency=8000&peerlatency=8000&rcvbuf=25000000

EOF

case "$CODEC" in
  hevc)
    # Why each x265 / ffmpeg flag:
    #   -preset ultrafast / -tune zerolatency  : real-time encode on CPU,
    #                                            no B-frames, no lookahead.
    #   -b:v / -maxrate / -bufsize all equal   : CBR with a 1s HRD buffer.
    #   keyint = min-keyint = 30, scenecut=0   : fixed 1 s GOP, no surprise IDRs.
    #   repeat-headers=1                       : VPS/SPS/PPS at every IDR so the
    #                                            receiver can resync from any IDR.
    #   -pix_fmt yuv420p                       : universally decodable HEVC.
    #   -an                                    : no audio (matches bumps-pipe).
    #   -f mpegts                              : MPEG-TS container; SRT carries TS.
    CODEC_ARGS=(
      -c:v libx265
      -preset ultrafast
      -tune zerolatency
      -b:v "${BITRATE}k"
      -maxrate "${BITRATE}k"
      -bufsize "${BITRATE}k"
      -pix_fmt yuv420p
      -x265-params "keyint=30:min-keyint=30:scenecut=0:repeat-headers=1:bframes=0"
    )
    ;;
  copy)
    # Stream-copy mode: take the publisher's H.264 NAL units as-is and
    # repackage into MPEG-TS for SRT. No decoder, no encoder, no PTS
    # rewriting beyond what FLV→MPEG-TS muxing requires. Use this when
    # the publisher's H.264 stream is too pathological to decode cleanly
    # (DJI Fly emits NAL framing that ffmpeg's strict parser rejects but
    # gstreamer's avdec_h264 accepts). The receiver gets H.264, not HEVC.
    #
    # If even *this* doesn't push cleanly, the drone's bitstream itself
    # is what's making bumps-pipe stutter at the AWS receiver, and we
    # need a timestamp flattener before the encoder rather than after.
    CODEC_ARGS=(-c:v copy)
    ;;
  *)
    echo "error: CODEC must be 'hevc' or 'copy' (got '$CODEC')" >&2
    exit 1
    ;;
esac

exec ffmpeg \
  -hide_banner \
  -loglevel info \
  "${SRC_ARGS[@]}" \
  -map 0:v:0 \
  "${CODEC_ARGS[@]}" \
  -an \
  -f mpegts \
  "${SRT_URI}"
