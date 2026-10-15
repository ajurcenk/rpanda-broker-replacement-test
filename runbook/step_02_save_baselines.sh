#!/usr/bin/env bash
# =============================================================================
# step_02_save_baselines.sh - Phase 0, runbook steps 4 and 6
# =============================================================================
# Saves the values the runbook must restore at the end, and records baselines:
#   state/orig.raft_learner_recovery_rate          restored by step_17 / checked by step_30
#   state/orig.partition_autobalancing_concurrent_moves   restored by step_31
#   state/baseline.client_p99_ms                   test-client end-to-end p99 (go/no-go, rollback)
#   state/baseline.txt                             URP, consumer lag, client stats at this moment
#
# Optional change (runbook step 4): if TEMP_CONCURRENT_MOVES is set in runbook.env,
# lowers partition_autobalancing_concurrent_moves to that value (asks first).
#
# Re-running is safe: saved originals are never overwritten.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"

save_once() {   # save_once <property>
  local f="$RB_STATE_DIR/orig.$1" v; v="$(cfg_get "$1")"
  if [[ -f "$f" ]]; then
    info "$1: original already saved ($(cat "$f")), current value $v"
  else
    echo "$v" > "$f"; ok "saved $1 = $v"
  fi
}

header "Save original values"
save_once raft_learner_recovery_rate
save_once partition_autobalancing_concurrent_moves
[[ "$(cat "$RB_STATE_DIR/orig.raft_learner_recovery_rate")" == 104857600 ]] \
  || warn "raft_learner_recovery_rate is not the default 104857600 - make sure that is intended"

header "Baselines"
{
  echo "time: $(ts)"
  echo "under-replicated partitions: $(urp_count)"
  echo "partition moves in flight: $(moves_in_flight)"
  echo "test clients: $(client_health_line)"          # TEST ONLY: step_00 test clients
  echo "--- consumer groups (lag) ---"
  rpk group list | awk 'NR>1 {print $1}' | head -50 | while read -r g; do
    rpk group describe "$g" 2>/dev/null | awk -v g="$g" '$1=="TOTAL-LAG" {print "  " g ": total lag " $2}'
  done
} | tee "$RB_STATE_DIR/baseline.txt"
# TEST ONLY: the baseline latency comes from the step_00 test clients (used by step_10/11/20)
p99="$(client_p99)"
if [[ -n "$p99" ]]; then
  state_set "$RB_STATE_DIR/baseline.client_p99_ms" "$p99"; ok "baseline client e2e p99 = $p99 ms"
else
  warn "no client latency yet; run step_00 first and wait a minute"
fi
info "Record your production dashboards' produce p99 and disk IOPS now as well (not measurable from here)."

header "Optional: throttle partition moves (runbook step 4)"
if [[ -n "$TEMP_CONCURRENT_MOVES" ]]; then
  cur="$(cfg_get partition_autobalancing_concurrent_moves)"
  if [[ "$cur" == "$TEMP_CONCURRENT_MOVES" ]]; then ok "partition_autobalancing_concurrent_moves already $cur"
  else change "lower partition_autobalancing_concurrent_moves $cur -> $TEMP_CONCURRENT_MOVES" \
         rpk cluster config set partition_autobalancing_concurrent_moves "$TEMP_CONCURRENT_MOVES"; fi
else
  info "TEMP_CONCURRENT_MOVES not set - no change"
fi

# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
rb_summary
