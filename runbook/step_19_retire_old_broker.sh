#!/usr/bin/env bash
# =============================================================================
# step_19_retire_old_broker.sh <swap> - Phase 2, runbook step 22
# =============================================================================
# The old broker has been decommissioned and its DNS name now points at the new broker.
# Only now may the old instance be stopped and terminated. This script:
#   1. re-checks that the old node is gone and DNS no longer points at it
#   2. tells you the command to stop/terminate the old broker; after you confirm, it checks that
#      the old host really stopped answering on 9092 (waits up to 60 s; stops if it still answers)
#   3. walks you through the rest of the clean-up checklist and records each confirmation
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
old_id="$(state_get "$SWAP_DIR/old_id")"

header "Preconditions"
check "DNS repointed (step_18)" test -f "$SWAP_DIR/repointed.ok"
check "old node $old_id is not a member" node_gone "$old_id"
old_ip="$(resolve "$SWAP_OLD")"
# every repointable name (seed names and client bootstrap names, without duplicates)
for n in $(printf '%s\n' "${SEED_DNS_NAMES[@]}" ${RPK_BROKERS//,/ } | sed 's/:[0-9]*$//' | sort -u); do
  check "$n does not resolve to the old broker ($old_ip)" [ "$(resolve "$n")" != "$old_ip" ]
done
(( RB_FAILS == 0 )) || rb_summary

header "Stop and terminate the old broker"
# Do this FIRST, then confirm. The script then checks that the old host really stopped answering.
info "run now: $(host_hint "$STOP_OLD_BROKER_HINT")"
if confirm "Done: old broker $SWAP_OLD stopped and its instance terminated?"; then
  echo "$(ts) confirmed: old broker stopped/terminated" >> "$SWAP_DIR/checklist.log"
else
  die "stop and terminate $SWAP_OLD first, then re-run this step"
fi
port_closed() { ! port_open "$1" "$2"; }
for _ in $(seq 1 12); do port_closed "$SWAP_OLD" 9092 && break; info "$SWAP_OLD:9092 still answers; waiting"; sleep 5; done
check "$SWAP_OLD:9092 no longer answers" port_closed "$SWAP_OLD" 9092
(( RB_FAILS == 0 )) || die "$SWAP_OLD still answers on 9092: it is not stopped yet. Run: $(host_hint "$STOP_OLD_BROKER_HINT")   then re-run this step"

header "Clean-up checklist for $SWAP_OLD"
items=(
  "EC2 stop/termination protection was disabled before terminating $SWAP_OLD"
  "$SWAP_OLD removed from inventory / CMDB"
  "$SWAP_OLD removed from monitoring and alerting"
  "$SWAP_OLD removed from security groups / firewall rules"
)
for it in "${items[@]}"; do
  if confirm "Done: $it ?"; then ok "$it"; echo "$(ts) confirmed: $it" >> "$SWAP_DIR/checklist.log"
  else bad "not done: $it"; fi
done

# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/retired.ok" "$(ts)"
rb_summary
echo
info "Next: step_20_soak_check.sh $SWAP_DNS"
