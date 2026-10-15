#!/usr/bin/env bash
# =============================================================================
# step_17_restore_recovery_rate.sh <swap> - Phase 2, runbook step 20
# =============================================================================
# ALWAYS run this, even if you skipped step_11: it is a no-op when nothing was raised,
# and it proves the value is back at the original before the DNS repoint and the soak.
#   * stops the rollback watcher (if running)
#   * sets raft_learner_recovery_rate back to the value saved by step_02
#   * checks every broker reports no restart needed
#   * reports whether the watcher rolled the rate back automatically during this swap
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
orig="$(state_get "$RB_STATE_DIR/orig.raft_learner_recovery_rate")"
[[ -n "$orig" ]] || die "original value not saved (step_02)"

header "Rollback watcher"
rm -f "$SWAP_DIR/raised"          # tells the watcher to exit
if [[ -f "$SWAP_DIR/watcher.pid" ]]; then
  kill -TERM -- "-$(cat "$SWAP_DIR/watcher.pid")" 2>/dev/null || true
  rm -f "$SWAP_DIR/watcher.pid"; ok "watcher stopped"
else
  info "no watcher running"
fi
[[ -f "$SWAP_DIR/rollback.txt" ]] && warn "automatic rollback happened during this swap: $(cat "$SWAP_DIR/rollback.txt")"

header "raft_learner_recovery_rate"
cur="$(cfg_get raft_learner_recovery_rate)"
if [[ "$cur" == "$orig" ]]; then
  ok "already at the original value $orig"
else
  change "restore raft_learner_recovery_rate $cur -> $orig" rpk cluster config set raft_learner_recovery_rate "$orig"
fi
check "value is the original" recovery_rate_is_original
check "config status clean on every broker (no restart needed)" config_status_clean
(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/rate_restored.ok" "$(ts)"
rb_summary
