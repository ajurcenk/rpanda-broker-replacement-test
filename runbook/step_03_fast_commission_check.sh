#!/usr/bin/env bash
# =============================================================================
# step_03_fast_commission_check.sh - Phase 1, runbook steps 7 and 9. READ-ONLY.
# =============================================================================
# Fast commission has no on/off switch in 25.3. It is in effect for a topic when:
#   * Tiered Storage is on for the topic (redpanda.remote.write/read), and
#   * an initial local retention target applies: a topic override
#     (initial.retention.local.target.ms / .bytes) or the cluster default
#     (initial_retention_local_target_ms_default / _bytes_default).
# Both cluster defaults are null by default, which means OFF.
#
# Usage:
#   step_03_fast_commission_check.sh            report the state (run before step_04)
#   step_03_fast_commission_check.sh enabled    also fail unless every Tiered Storage topic is covered
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
expect="${RB_ARGS[0]:-report}"

header "Cluster defaults"
ms="$(cfg_get initial_retention_local_target_ms_default)"
by="$(cfg_get initial_retention_local_target_bytes_default)"
info "cloud_storage_enabled                         = $(cfg_get cloud_storage_enabled)"
info "initial_retention_local_target_ms_default     = $ms"
info "initial_retention_local_target_bytes_default  = $by"

header "Per topic (effective initial target)"
printf '  %-40s %-6s %-34s %s\n' TOPIC TS EFFECTIVE-TARGET COVERED
covered=0 uncovered=0 overrides=0
while read -r t; do
  cfgs="$(rpk topic describe "$t" -c)"
  rw="$(awk '$1=="redpanda.remote.write" {print $2}' <<<"$cfgs")"
  # A topic override shows source DYNAMIC_TOPIC_CONFIG; anything else is the cluster default.
  tms="$(awk '$1=="initial.retention.local.target.ms" && $3=="DYNAMIC_TOPIC_CONFIG" {print $2}' <<<"$cfgs")"
  tby="$(awk '$1=="initial.retention.local.target.bytes" && $3=="DYNAMIC_TOPIC_CONFIG" {print $2}' <<<"$cfgs")"
  [[ -n "$tms$tby" ]] && overrides=$((overrides + 1))
  eff="${tms:+ms=$tms}${tby:+ bytes=$tby}"
  if [[ -z "$eff" ]]; then        # no topic override: the cluster defaults apply
    [[ "$ms" != null ]] && eff="ms=$ms"
    [[ "$by" != null ]] && eff="${eff:+$eff }bytes=$by"
    [[ -n "$eff" ]] && eff="$eff (default)"
  fi
  if [[ "$rw" == true && -n "$eff" ]]; then c=yes; covered=$((covered + 1))
  elif [[ "$rw" != true ]]; then c="no (no Tiered Storage)"
  else c=NO; uncovered=$((uncovered + 1)); fi
  printf '  %-40s %-6s %-34s %s\n' "$t" "${rw:-?}" "${eff:-none}" "$c"
done < <(rpk topic list | awk 'NR>1 {print $1}')

header "State"
if [[ "$ms" == null && "$by" == null && $overrides == 0 ]]; then state=DISABLED
elif (( covered > 0 )); then state=ENABLED
else state=INCONSISTENT; fi
info "fast commission is $state (Tiered Storage topics covered: $covered, not covered: $uncovered)"
echo "$(ts) $state covered=$covered uncovered=$uncovered ms=$ms bytes=$by" >> "$RB_STATE_DIR/fast_commission.log"
if [[ "$expect" == enabled ]]; then
  check "fast commission ENABLED" [ "$state" = ENABLED ]
  check "every Tiered Storage topic covered" [ "$uncovered" -eq 0 ]
fi
rb_summary
