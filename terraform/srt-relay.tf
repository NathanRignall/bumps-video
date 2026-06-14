# =============================================================================
# AWS MediaConnect SRT relay
# =============================================================================
#
# Topology:
#
#   [bumps-pipe field laptop]
#            │  SRT caller, encoded HEVC
#            ▼
#   [MediaConnect Flow Source — srt-listener :9999]
#            │
#            ▼
#   [MediaConnect Flow Output — srt-listener :9998]
#            ▲
#            │  SRT caller, into OBS or `ffplay`
#   [Home PC / OBS]
#
# Both ends of MediaConnect are *listeners* so the field laptop and the home
# PC don't need to be reachable from the public internet (no port-forwarding,
# no dynamic-DNS for residential IPs). MediaConnect itself is the central
# meeting point with stable, region-pinned addresses.
#
# Cost note: a running MediaConnect flow + one output costs roughly
# $0.20/hour in eu-west-2 (≈ $144/mo if left running 24×7). Use the
# `scripts/aws-relay.sh start|stop` helpers to only run it during flights.

locals {
  flow_name         = "bumps-video-drone"
  source_name       = "drone-srt-in"
  output_name       = "home-srt-out"
  srt_input_port    = 9999
  srt_output_port   = 9998
  srt_latency_ms    = 2500
  srt_max_bitrate   = 8000000 # bits/s — matches the ?maxbw= in the URI
  availability_zone = "eu-west-2a"
}

resource "awscc_mediaconnect_flow" "drone" {
  name              = local.flow_name
  availability_zone = local.availability_zone

  source = {
    name           = local.source_name
    description    = "Incoming SRT from bumps-pipe via Starlink"
    protocol       = "srt-listener"
    ingest_port    = local.srt_input_port
    whitelist_cidr = "0.0.0.0/0" # SRT is authenticated by passphrase if set; rotation/IP-pinning isn't practical with Starlink
    # MediaConnect SRT-listener sources accept only MinLatency (the receive
    # buffer). MaxLatency is rejected with a 400 — that field is for caller
    # mode only. Outputs still take MaxLatency.
    min_latency = local.srt_latency_ms
    max_bitrate = local.srt_max_bitrate
  }
}

resource "awscc_mediaconnect_flow_output" "home" {
  flow_arn = awscc_mediaconnect_flow.drone.flow_arn
  name     = local.output_name
  protocol = "srt-listener"
  port     = local.srt_output_port
  # `max_latency` is *not* valid on srt-listener outputs (despite the docs
  # implying it is). The peer's caller URI's `?latency=` controls the buffer.
  # Same asymmetry as on the source side — listener mode in MediaConnect
  # accepts only the minimum hints it needs.
  # `description` likewise omitted — it produces an opaque "Invalid
  # description." 400 even for plain ASCII. Console-cosmetic only.
  cidr_allow_list = ["0.0.0.0/0"] # restrict to your home /32 if you want and have a fixed IP
}
