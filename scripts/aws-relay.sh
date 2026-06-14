#!/usr/bin/env bash
#
# aws-relay.sh — wrap the AWS MediaConnect flow defined in terraform/.
#
# Subcommands:
#   start     Start the flow (begins ~$0.20/hr billing).
#   stop      Stop the flow (billing stops).
#   status    Print the current flow state, source, output.
#   urls      Print the SRT URLs for bumps-pipe + the home PC. Requires the
#             flow to be in STARTING or ACTIVE state to know the listener IPs.
#   logs N    Tail the last N (default 50) CloudWatch entries for the flow.
#   help      This message.
#
# Lookup strategy: the flow ARN is discovered via tag — anything named
# `bumps-video-drone` in eu-west-2 matches. No state file dependency, so this
# works whether you're at home or on a hotspot.
#
# Requires: aws CLI, jq.

set -uo pipefail

REGION="${AWS_REGION:-eu-west-2}"
FLOW_NAME="${FLOW_NAME:-bumps-video-drone}"

usage() {
  sed -n 's/^# \?//;/^[[:space:]]*$/q;p' "$0" | tail -n +2
}

need() {
  for bin in "$@"; do
    command -v "$bin" >/dev/null 2>&1 || {
      echo "error: $bin not in PATH" >&2
      exit 1
    }
  done
}

flow_arn() {
  aws --region "$REGION" mediaconnect list-flows \
    --query "Flows[?Name=='${FLOW_NAME}'].FlowArn | [0]" \
    --output text 2>/dev/null
}

describe() {
  local arn
  arn="$(flow_arn)"
  if [[ -z "$arn" || "$arn" == "None" ]]; then
    echo "error: no MediaConnect flow named '$FLOW_NAME' in $REGION" >&2
    echo "       run `tofu -chdir=terraform apply` first?" >&2
    exit 1
  fi
  aws --region "$REGION" mediaconnect describe-flow --flow-arn "$arn"
}

cmd_start() {
  local arn; arn="$(flow_arn)"
  echo "starting flow $arn ..."
  aws --region "$REGION" mediaconnect start-flow --flow-arn "$arn" > /dev/null
  echo "  → STARTING. Run \`$0 status\` again in ~30s; URLs appear once it's ACTIVE."
}

cmd_stop() {
  local arn; arn="$(flow_arn)"
  echo "stopping flow $arn ..."
  aws --region "$REGION" mediaconnect stop-flow --flow-arn "$arn" > /dev/null
  echo "  → STOPPING. Billing stops once it's STANDBY."
}

cmd_status() {
  describe | jq -r '
    "Flow         : \(.Flow.Name)",
    "ARN          : \(.Flow.FlowArn)",
    "Status       : \(.Flow.Status)",
    "AZ           : \(.Flow.AvailabilityZone)",
    "Egress IP    : \(.Flow.EgressIp // "(not yet assigned)")",
    "",
    "Source",
    "  Name       : \(.Flow.Source.Name)",
    "  Protocol   : \(.Flow.Source.Transport.Protocol)",
    "  Ingest IP  : \(.Flow.Source.IngestIp // "(not yet assigned)")",
    "  Ingest Port: \(.Flow.Source.IngestPort // .Flow.Source.Transport.SourceListenerPort)",
    "  Latency    : \(.Flow.Source.Transport.MinLatency)–\(.Flow.Source.Transport.MaxLatency) ms",
    "  Max BW     : \(.Flow.Source.Transport.MaxBitrate / 1000) kbps",
    "",
    "Output",
    (.Flow.Outputs[] |
      "  Name       : \(.Name)",
      "  Protocol   : \(.Transport.Protocol)",
      "  Listener   : \(.ListenerAddress // "(not yet assigned)"):\(.Port)",
      "  Latency    : \(.Transport.MaxLatency) ms")
  '
}

cmd_urls() {
  local data ingest_ip ingest_port listener_addr listener_port min_lat max_bw
  data="$(describe)"

  ingest_ip=$(    echo "$data" | jq -r '.Flow.Source.IngestIp // empty')
  ingest_port=$(  echo "$data" | jq -r '.Flow.Source.IngestPort // .Flow.Source.Transport.SourceListenerPort')
  listener_addr=$(echo "$data" | jq -r '.Flow.Outputs[0].ListenerAddress // empty')
  listener_port=$(echo "$data" | jq -r '.Flow.Outputs[0].Port')
  min_lat=$(      echo "$data" | jq -r '.Flow.Source.Transport.MinLatency')
  max_bw=$(       echo "$data" | jq -r '.Flow.Source.Transport.MaxBitrate')

  if [[ -z "$ingest_ip" ]] || [[ -z "$listener_addr" ]]; then
    echo "Flow isn't ACTIVE yet — start it with: $0 start" >&2
    echo "Re-run \`$0 urls\` once status reports both IPs."         >&2
    exit 1
  fi

  cat <<EOF
# ─── bumps-pipe (the field laptop) ─────────────────────────────────────────
# Set BUMPS_SRT_URI to:
srt://${ingest_ip}:${ingest_port}?mode=caller&latency=${min_lat}&peerlatency=${min_lat}&oheadbw=50&maxbw=${max_bw}&streamid=drone

# ─── home PC / OBS (pull side) ────────────────────────────────────────────
# Open this in OBS Media Source, or:
#   ffplay 'srt://...?mode=caller&latency=${min_lat}'
srt://${listener_addr}:${listener_port}?mode=caller&latency=${min_lat}
EOF
}

cmd_logs() {
  local n="${1:-50}"
  local arn name
  arn="$(flow_arn)"
  name=$(basename "$arn")  # "flow:1-xxxxxxxx:name"
  local log_group="/aws/MediaConnect/flow"
  aws --region "$REGION" logs filter-log-events \
    --log-group-name "$log_group" \
    --filter-pattern "$FLOW_NAME" \
    --limit "$n" \
    --output text 2>/dev/null \
    || echo "no CloudWatch logs found (the flow may need a log group attached)"
}

need aws jq

case "${1:-help}" in
  start)    cmd_start ;;
  stop)     cmd_stop ;;
  status)   cmd_status ;;
  urls)     cmd_urls ;;
  logs)     cmd_logs "${2:-50}" ;;
  help|*)   usage ;;
esac
