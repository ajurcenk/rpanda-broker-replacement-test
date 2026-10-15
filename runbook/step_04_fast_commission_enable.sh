#!/usr/bin/env bash
# =============================================================================
# step_04_fast_commission_enable.sh - Phase 1, runbook step 8 (then re-checks, step 9)
# =============================================================================
# Turns fast commission on with cluster properties (no restart needed):
#   initial_retention_local_target_ms_default    = $FC_TARGET_MS   (keep above normal consumer lag)
#   initial_retention_local_target_bytes_default = $FC_TARGET_BYTES (optional size cap)
# plus optional per-topic overrides from FC_TOPIC_OVERRIDES ("topic=ms").
#
# New replicas then start from data already in object storage and copy only the
# recent window, instead of the full local retention.
#
# Undo: rpk cluster config set initial_retention_local_target_ms_default null
#       (and _bytes_default null, and remove topic overrides with
#        rpk topic alter-config <topic> --delete initial.retention.local.target.ms)
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"

header "Before"
info "initial_retention_local_target_ms_default    = $(cfg_get initial_retention_local_target_ms_default)"
info "initial_retention_local_target_bytes_default = $(cfg_get initial_retention_local_target_bytes_default)"
[[ "$(cfg_get cloud_storage_enabled)" == true ]] || die "cloud_storage_enabled is not true - fast commission needs Tiered Storage"

header "Changes"
cur="$(cfg_get initial_retention_local_target_ms_default)"
if [[ "$cur" == "$FC_TARGET_MS" ]]; then ok "time target already $FC_TARGET_MS ms"
else change "set initial_retention_local_target_ms_default = $FC_TARGET_MS" \
       rpk cluster config set initial_retention_local_target_ms_default "$FC_TARGET_MS"; fi
if [[ -n "$FC_TARGET_BYTES" ]]; then
  cur="$(cfg_get initial_retention_local_target_bytes_default)"
  if [[ "$cur" == "$FC_TARGET_BYTES" ]]; then ok "size cap already $FC_TARGET_BYTES bytes"
  else change "set initial_retention_local_target_bytes_default = $FC_TARGET_BYTES" \
         rpk cluster config set initial_retention_local_target_bytes_default "$FC_TARGET_BYTES"; fi
fi
for o in "${FC_TOPIC_OVERRIDES[@]}"; do
  t="${o%%=*}"; v="${o#*=}"
  change "topic override $t initial.retention.local.target.ms=$v" \
    rpk topic alter-config "$t" --set initial.retention.local.target.ms="$v"
done

header "Config status (no restart may be required)"
st="$(rpk cluster config status)"; echo "$st" | sed 's/^/  /'
check "no broker needs a restart, nothing invalid" \
  awk 'NR>1 && ($3=="true" || $4!="[]" || $5!="[]") {bad=1} END {exit bad}' <<<"$st"
rb_summary
echo
info "Next: run step_03_fast_commission_check.sh enabled  (every Tiered Storage topic must be covered)"
