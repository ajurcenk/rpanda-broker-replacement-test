#!/usr/bin/env bash
# =============================================================================
# step_10_go_no_go.sh <swap> - Phase 2, runbook step 11. READ-ONLY.
# =============================================================================
# Run at the start of EVERY swap. <swap> is the DNS name from SWAP_PLAN (e.g.
# b2.kafka.example.com) or its number (1-5). All checks must pass before step_11/12.
#
# Checks:
#   * previous swaps in the plan are finished (their soak passed)
#   * OLD_HOST is an active, alive member; NEW_HOST is not a member yet
#   * cluster healthy, balancer idle, no maintenance mode, no under-replicated partitions
#   * one replica per rack; all brokers on $EXPECTED_VERSION
#   * raft_learner_recovery_rate is at its original value (no earlier swap left it raised)
#   * fast commission is on; important topics have no upload backlog
#   * seed rules hold (every seed DNS name -> active broker, one per AZ)
#   * test clients healthy and their p99 close to the baseline
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg

header "Swap order"
for (( i = 1; i < SWAP_NO; i++ )); do
  read -r d _ <<<"${SWAP_PLAN[$((i - 1))]}"
  prev_dir="$RB_STATE_DIR/swap-$i-${d%%.*}"
  check "swap $i ($d) finished (soak passed)" test -f "$prev_dir/soak.ok"
done
check "this swap not already finished" [ ! -f "$SWAP_DIR/soak.ok" ]

header "Brokers in this swap"
old_id="$(broker_id_of_host "$SWAP_OLD")"
check "OLD $SWAP_OLD is a member (node ${old_id:-none})" test -n "$old_id"
if [[ -n "$old_id" ]]; then
  check "OLD node $old_id is active and alive" node_active "$old_id"
  check "OLD node $old_id is in rack $SWAP_RACK" node_rack_is "$old_id" "$SWAP_RACK"
  state_set "$SWAP_DIR/old_id" "$old_id"
fi
check "NEW $SWAP_NEW is not a member yet" not_member "$SWAP_NEW"
check "$SWAP_DNS currently points at OLD" dns_points_to "$SWAP_DNS" "$SWAP_OLD"

header "Cluster"
check "healthy" is_healthy
check "balancer idle" balancer_idle
check "no broker in maintenance" no_maintenance
check "no under-replicated partitions" urp_zero
check "one replica per rack" placement_audit
check "versions" all_versions_ok
check "recovery rate at original value" recovery_rate_is_original
check "config status clean" config_status_clean

header "Fast commission and Tiered Storage"
fc_ms="$(cfg_get initial_retention_local_target_ms_default)"; fc_b="$(cfg_get initial_retention_local_target_bytes_default)"
check "fast commission on (ms=$fc_ms bytes=$fc_b)" [ "$fc_ms" != null -o "$fc_b" != null ]
for t in $(important_topics); do
  lag="$(ts_backlog_max "$t")"
  # A small tail (the segment being written) is normal; thousands of offsets means uploads lag.
  if (( lag < 10000 )); then ok "$t upload backlog $lag offsets"; else warn "$t upload backlog $lag offsets"; fi
done

header "Seeds and clients"
check "seed rules" seeds_check
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
check "client p99 within ${ROLLBACK_P99_RATIO}x baseline" p99_within "$ROLLBACK_P99_RATIO"
info "Also confirm on your dashboards: produce p99 at baseline, no alerts open."

if (( RB_FAILS == 0 )); then state_set "$SWAP_DIR/go.ok" "$(ts)"; fi
rb_summary
echo
info "GO. Next: step_11 (optional raise of recovery rate) or step_12 (prepare the new broker)."
