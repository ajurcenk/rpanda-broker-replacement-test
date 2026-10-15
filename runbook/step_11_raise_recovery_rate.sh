#!/usr/bin/env bash
# =============================================================================
# step_11_raise_recovery_rate.sh <swap> [bytes_per_sec] - Phase 2, runbook step 12. OPTIONAL.
# =============================================================================
# Raises raft_learner_recovery_rate (how fast each broker streams data to new replicas)
# for this swap only. Recommended: keep the default in the first swap, compare, then decide.
#
# Safety:
#   * the original value saved by step_02 is restored by step_17 (always run step_17)
#   * a watcher starts in the background: if the test clients' e2e p99 stays above
#     ${ROLLBACK_P99_RATIO}x baseline for $ROLLBACK_SAMPLES samples of 10 s, it restores the
#     original value immediately and records why in state/swap-*/rollback.txt
#
# In the harness test, the auto-rollback fired in both raised swaps on a busy host.
# Watch your production p99 dashboards while raised.
#
# Undo at any time: ./step_17_restore_recovery_rate.sh <swap>
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
rate="${RB_ARGS[1]:-$RAISED_RECOVERY_RATE}"

check "go/no-go passed for this swap (step_10)" test -f "$SWAP_DIR/go.ok"
check "recovery rate currently at original" recovery_rate_is_original
(( RB_FAILS == 0 )) || rb_summary

orig="$(state_get "$RB_STATE_DIR/orig.raft_learner_recovery_rate")"
header "Raise raft_learner_recovery_rate $orig -> $rate"
change "raise raft_learner_recovery_rate to $rate bytes/s" rpk cluster config set raft_learner_recovery_rate "$rate"
(( DRY_RUN )) && exit 0
state_set "$SWAP_DIR/raised" "$rate"
check "new value in effect" [ "$(cfg_get raft_learner_recovery_rate)" = "$rate" ]
check "config status clean (no restart needed)" config_status_clean

header "Start automatic rollback watcher"
setsid nohup "$RB_DIR/tools/recovery_rate_watcher.sh" "$SWAP_DIR" >> "$SWAP_DIR/watcher.log" 2>&1 < /dev/null &
echo $! > "$SWAP_DIR/watcher.pid"
ok "watcher running (pid $!), log: $SWAP_DIR/watcher.log"
rb_summary
