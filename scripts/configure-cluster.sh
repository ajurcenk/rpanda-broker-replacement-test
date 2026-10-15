#!/usr/bin/env bash
# configure-cluster.sh: Tiered Storage on MinIO, rack awareness, continuous balancing, test cadence.
# Fast commission is deliberately left DISABLED (initial_retention_local_target_* stay null).
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"

set_prop() {
  local k="$1" v="$2" cur
  cur="$(cfg_get "$k" 2>/dev/null || true)"
  if [[ "$cur" == "$v" ]]; then log "  $k already $v"; return; fi
  rpk_exec cluster config set "$k" "$v" >/dev/null
  log "  set $k=$v (was ${cur:-?})"
}
log "applying cluster configuration"
set_prop write_caching_default false            # image default is true (developer_mode); durability test
set_prop cloud_storage_access_key "$S3_ACCESS_KEY"
set_prop cloud_storage_secret_key "$S3_SECRET_KEY"
set_prop cloud_storage_region "$S3_REGION"
set_prop cloud_storage_bucket "$S3_BUCKET"
set_prop cloud_storage_api_endpoint minio
set_prop cloud_storage_api_endpoint_port 9000
set_prop cloud_storage_disable_tls true
set_prop cloud_storage_url_style path
set_prop cloud_storage_credentials_source config_file
set_prop cloud_storage_segment_max_upload_interval_sec "$UPLOAD_INTERVAL_SEC"
# With retention_local_strict=false, TS partitions may grow past retention.local.target and are
# only trimmed under disk pressure, so data would not be "only in MinIO". Force trimming.
set_prop retention_local_strict true
set_prop enable_rack_awareness true
set_prop partition_autobalancing_mode continuous
mkdir -p "$RESULTS/raw"
[[ -f "$RESULTS/raw/orig-concurrent-moves" ]] || cfg_get partition_autobalancing_concurrent_moves > "$RESULTS/raw/orig-concurrent-moves"
set_prop partition_autobalancing_concurrent_moves "$CONCURRENT_MOVES"   # mimics the production throttle
set_prop cloud_storage_enabled true

# Fast commission must start disabled (until the fast commission flow's Phase B enables it).
if [[ ! -f "$RESULTS/fast-commission-config-enabled.json" ]]; then
  for k in initial_retention_local_target_bytes_default initial_retention_local_target_ms_default; do
    v="$(cfg_get "$k")"; [[ "$v" == "null" ]] || die "$k is '$v'; fast commission must start disabled (null)"
  done
else
  log "  fast commission already enabled by Phase B (re-run); not asserting disabled"
fi

# Rolling restart where required.
for i in $(running_instances); do
  nid="$(id_of "$i")"
  if rpk_exec cluster config status | awk -v id="$nid" '$1==id {print $3}' | grep -q true; then
    log "restarting $i (node $nid) to apply restart-required properties"
    "$ROOT/scripts/broker.sh" restart "$i"; sleep 3; wait_back "$i"
  fi
done
st="$(rpk_exec cluster config status)"
echo "$st" > "$RESULTS/config-status-setup.txt"
awk 'NR>1 && ($3=="true" || $4!="[]" || $5!="[]") {bad=1} END {exit bad}' <<<"$st" || die "config status not clean after restart:\n$st"
[[ "$(cfg_get cloud_storage_enabled)" == "true" ]] || die "cloud_storage_enabled not true"

# Recovery rate stays at its default; record the setup value (never deleted).
rr="$(cfg_get raft_learner_recovery_rate)"
[[ -f "$RESULTS/recovery-rate.setup" ]] || echo "$rr" > "$RESULTS/recovery-rate.setup"
log "raft_learner_recovery_rate=$rr (setup value recorded)"
rpk_exec cluster config export -f /tmp/cluster-config.yaml >/dev/null && docker exec "$(cname "$(any_broker)")" cat /tmp/cluster-config.yaml > "$RESULTS/cluster-config-setup.yaml" || true
log "configure-cluster done"
