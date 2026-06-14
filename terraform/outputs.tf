output "mediaconnect_flow_arn" {
  description = "Pass to `aws mediaconnect start-flow/stop-flow --flow-arn`."
  value       = awscc_mediaconnect_flow.drone.flow_arn
}

output "mediaconnect_flow_name" {
  description = "Convenience: the flow name for AWS CLI / console search."
  value       = local.flow_name
}

# The actual listener IPs aren't always populated in Terraform state because
# Cloud-Control surfaces them late and they only become routable once the flow
# is started. Use `scripts/aws-relay.sh urls` after starting the flow to print
# the URLs your bumps-pipe and home PC need.
#
# We do expose ports, latencies, and bitrate so the URLs are constructible
# once the IP is known.

output "srt_input_port" {
  description = "Port for bumps-pipe to push to (SRT caller)."
  value       = local.srt_input_port
}

output "srt_output_port" {
  description = "Port for the home PC to pull from (SRT caller)."
  value       = local.srt_output_port
}

output "srt_latency_ms" {
  description = "SRT receive buffer both sides should set."
  value       = local.srt_latency_ms
}

output "srt_max_bitrate_bps" {
  description = "Hard ceiling MediaConnect enforces; align with bumps-pipe `?maxbw=`."
  value       = local.srt_max_bitrate
}
