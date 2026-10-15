#!/usr/bin/env bash
# =============================================================================
# step_15_decommission.sh <swap> [--maintenance] - Phase 2, runbook step 18
# =============================================================================
# Decommissions the OLD broker: Redpanda moves every replica off it, then removes it.
#   --maintenance   first drain leadership off it (rpk cluster maintenance enable --wait)
#
# To ABORT while it is still running (before it completes):
#     rpk redpanda admin brokers recommission <old node id>
# Once a decommission COMPLETES it cannot be undone; the broker cannot rejoin.
#
# Note (25.3.17): for a broker that is OFFLINE, rpk refuses with "version for broker N is
# unknown ... re-run this with --force" and still exits 0. This script detects that and asks
# before using the hidden --force flag.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
maint=0; [[ "${RB_ARGS[1]:-}" == --maintenance ]] && maint=1

old_id="$(state_get "$SWAP_DIR/old_id")"
# Safe to re-run: if an earlier run already completed the decommission, just say so.
if [[ -f "$SWAP_DIR/decommissioned.ok" && -n "$old_id" ]] && node_gone "$old_id"; then
  ok "node $old_id ($SWAP_OLD) was already decommissioned at $(cat "$SWAP_DIR/decommissioned.ok")"
  rb_summary
  info "Next: step_16_verify.sh $SWAP_DNS"
  exit 0
fi
check "cluster settled after the new broker joined (step_14)" test -f "$SWAP_DIR/settled.ok"
check "old node id known (${old_id:-?})" [ -n "$old_id" ]
check "old node $old_id ($SWAP_OLD) is still a member" node_member "${old_id:-x}"
check "old node is the host from the plan" [ "$(broker_host "${old_id:-x}")" = "$SWAP_OLD" ]
(( RB_FAILS == 0 )) || rb_summary

if (( maint )); then
  header "Maintenance drain"
  change "put node $old_id ($SWAP_OLD) in maintenance mode and wait for leadership to drain" \
    rpk cluster maintenance enable "$old_id" --wait
fi

header "Decommission node $old_id ($SWAP_OLD)"
info "to ABORT before completion: rpk redpanda admin brokers recommission $old_id"
if (( DRY_RUN )); then info "dry run: would run rpk redpanda admin brokers decommission $old_id"; exit 0; fi
confirm "Decommission node $old_id ($SWAP_OLD) now?" || die "not confirmed"
out="$(rpk redpanda admin brokers decommission "$old_id" 2>&1 || true)"
echo "$out" | sed 's/^/  /'
if grep -q -- '--force' <<<"$out"; then
  warn "rpk refused: the broker looks offline (its version is unknown)"
  confirm "Use the hidden --force flag to decommission offline node $old_id?" || die "not confirmed"
  rpk redpanda admin brokers decommission "$old_id" --force
fi
echo "$(ts) $RB_SCRIPT CHANGE decommission node $old_id" >> "$RB_STATE_DIR/changes.log"
t0=$(date +%s); state_set "$SWAP_DIR/decom_started" "$t0"

header "Progress"
trap 'echo; info "interrupted. The decommission CONTINUES in the cluster. To abort it: rpk redpanda admin brokers recommission '"$old_id"'"; exit 130' INT
deadline=$(( t0 + DECOM_TIMEOUT_MIN * 60 ))
while node_member "$old_id"; do
  (( $(date +%s) < deadline )) || die "decommission not finished after $DECOM_TIMEOUT_MIN minutes (still running in the cluster)"
  st="$(admin "/v1/brokers/$old_id/decommission" 2>/dev/null || echo '{}')"
  info "replicas left: $(jq -r '.replicas_left // "?"' <<<"$st"), bytes left: $(jq -r '[.partitions[]?.bytes_left_to_move // 0] | add // 0' <<<"$st") | clients: $(client_health_line)"
  sleep "$POLL_SEC"
done
secs=$(( $(date +%s) - t0 ))
ok "node $old_id decommissioned and removed from the cluster in ${secs}s"
state_set "$SWAP_DIR/decom_seconds" "$secs"
state_set "$SWAP_DIR/decommissioned.ok" "$(ts)"
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
rb_summary
echo
info "Next: step_16_verify.sh $SWAP_DNS   (do NOT repoint DNS or stop the old host yet)"
