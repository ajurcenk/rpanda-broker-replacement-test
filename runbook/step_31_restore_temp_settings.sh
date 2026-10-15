#!/usr/bin/env bash
# =============================================================================
# step_31_restore_temp_settings.sh - Phase 3, runbook steps 25-26
# =============================================================================
# Puts back the temporary settings changed for the migration:
#   * partition_autobalancing_concurrent_moves -> original saved by step_02
#   * leader pinning: lists topics with redpanda.leaders.preference and the cluster
#     default_leaders_preference, and offers to remove each one (only if it was added
#     for the migration - answer "no" for pinning that existed before)
# Fast commission targets are KEPT (they help every future broker replacement).
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"

header "partition_autobalancing_concurrent_moves"
orig="$(state_get "$RB_STATE_DIR/orig.partition_autobalancing_concurrent_moves")"
cur="$(cfg_get partition_autobalancing_concurrent_moves)"
if [[ -z "$orig" ]]; then warn "original not saved (step_02 not run?) - leaving $cur"
elif [[ "$cur" == "$orig" ]]; then ok "already at the original value $orig"
else change "restore partition_autobalancing_concurrent_moves $cur -> $orig" \
       rpk cluster config set partition_autobalancing_concurrent_moves "$orig"; fi

header "Leader pinning"
dlp="$(cfg_get default_leaders_preference 2>/dev/null || echo unknown)"
info "cluster default_leaders_preference = $dlp"
if [[ "$dlp" != none && "$dlp" != unknown && "$dlp" != null && -n "$dlp" ]]; then
  confirm "Was default_leaders_preference set for the migration (remove it)?" \
    && change "reset default_leaders_preference" rpk cluster config set default_leaders_preference none
fi
while read -r t; do
  v="$(rpk topic describe "$t" -c | awk '$1=="redpanda.leaders.preference" && $3=="DYNAMIC_TOPIC_CONFIG" {print $2}')"
  [[ -z "$v" ]] && continue
  info "topic $t pins leaders: $v"
  if confirm "Was this pinning on $t added for the migration (remove it)?"; then
    change "remove leader pinning on $t" rpk topic alter-config "$t" --delete redpanda.leaders.preference
  fi
done < <(rpk topic list | awk 'NR>1 {print $1}')

header "Result"
check "config status clean" config_status_clean
info "Old volumes/instances: confirm in AWS that every old instance from SWAP_PLAN is terminated and its EBS volumes deleted."
rb_summary
