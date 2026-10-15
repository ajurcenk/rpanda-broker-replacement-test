#!/usr/bin/env bash
# =============================================================================
# step_20_soak_check.sh <swap> [minutes] - Phase 2, runbook step 23
# =============================================================================
# Watches the cluster for [minutes] (default $SOAK_CHECK_MIN) after the swap:
# every 30 s it checks health, under-replicated partitions and the test clients
# (up, no gaps, no producer errors, e2e p99 within ${ROLLBACK_P99_RATIO}x baseline).
#
# The runbook soak is 24 HOURS. Run this script at the start of the soak and again
# just before the next swap (step_10 requires this swap's soak.ok). Only mark the
# soak as passed when your production dashboards are also clean for 24 hours.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
mins="${RB_ARGS[1]:-$SOAK_CHECK_MIN}"
check "old broker retired (step_19)" test -f "$SWAP_DIR/retired.ok"
(( RB_FAILS == 0 )) || rb_summary

header "Soak watch: $mins minutes"
end=$(( $(date +%s) + mins * 60 )); samples=0; problems=0
while (( $(date +%s) < end )); do
  s=""
  is_healthy || s+=" UNHEALTHY"
  urp_zero >/dev/null || s+=" URP=$(urp_count)"
  client_ok >/dev/null || s+=" CLIENTS"                          # TEST ONLY: step_00 test clients
  p99_within "$ROLLBACK_P99_RATIO" >/dev/null || s+=" P99-HIGH"  # TEST ONLY: test-client latency
  samples=$((samples + 1)); [[ -n "$s" ]] && problems=$((problems + 1))
  info "sample $samples:${s:- ok} | $(client_health_line)"
  sleep 30
done
check "no problems in $samples samples" [ "$problems" -eq 0 ]
check "one replica per rack" placement_audit
check "recovery rate at original" recovery_rate_is_original

if (( RB_FAILS == 0 )); then
  if confirm "Have your production dashboards been clean for the full 24 h soak?"; then
    state_set "$SWAP_DIR/soak.ok" "$(ts)"; ok "soak recorded as passed"
  else
    warn "soak not marked as passed yet; re-run this step when the 24 h soak is over"
  fi
fi
rb_summary
