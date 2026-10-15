#!/usr/bin/env bash
# =============================================================================
# tools/recovery_rate_watcher.sh <swap-state-dir> - started by step_11, stopped by step_17
# =============================================================================
# Every 10 seconds: read the test clients' latest end-to-end p99. If it is above
# ROLLBACK_P99_RATIO x baseline for ROLLBACK_SAMPLES samples in a row, set
# raft_learner_recovery_rate back to the original value and write rollback.txt.
# Stops by itself when <swap-state-dir>/raised disappears (step_17 removed it).
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/../lib/common.sh"
dir="$1"
base="$(state_get "$RB_STATE_DIR/baseline.client_p99_ms")"
orig="$(state_get "$RB_STATE_DIR/orig.raft_learner_recovery_rate")"
[[ -n "$base" && -n "$orig" ]] || { echo "$(ts) no baseline or original saved; watcher not running"; exit 1; }
limit=$(awk -v b="$base" -v r="$ROLLBACK_P99_RATIO" 'BEGIN {print b * r}')
echo "$(ts) watching: baseline p99 $base ms, limit $limit ms, $ROLLBACK_SAMPLES samples"
high=0
while [[ -f "$dir/raised" ]]; do
  sleep 10
  p99="$(client_p99)"
  [[ -n "$p99" ]] || continue
  if awk -v p="$p99" -v l="$limit" 'BEGIN {exit !(p > l)}'; then high=$((high + 1)); else high=0; fi
  echo "$(ts) client p99 ${p99} ms, consecutive high samples: $high"
  if (( high >= ROLLBACK_SAMPLES )); then
    rpk cluster config set raft_learner_recovery_rate "$orig"
    echo "$(ts) AUTO-ROLLBACK: client p99 $p99 ms > $limit ms for $high samples; restored $orig" | tee "$dir/rollback.txt"
    echo "$(ts) recovery_rate_watcher CHANGE auto-rollback raft_learner_recovery_rate=$orig" >> "$RB_STATE_DIR/changes.log"
    rm -f "$dir/raised"
    exit 0
  fi
done
echo "$(ts) raised flag removed; watcher exiting"
