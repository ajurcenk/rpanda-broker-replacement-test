#!/usr/bin/env bash
# =============================================================================
# step_30_final_check.sh - Phase 3, runbook step 24. READ-ONLY.
# =============================================================================
# After the last swap's soak:
#   * every swap in the plan finished (soak passed)
#   * the brokers are exactly the NEW hosts from SWAP_PLAN, all on $EXPECTED_VERSION
#   * one replica per rack, cluster healthy, no under-replicated partitions
#   * raft_learner_recovery_rate equals the original saved by step_02
#   * each seed DNS name resolves to a NEW broker, and seed_servers is unchanged on every broker
#   * test clients healthy
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"

header "Swaps"
want_hosts=()
for i in "${!SWAP_PLAN[@]}"; do
  read -r d _old new _rack <<<"${SWAP_PLAN[$i]}"
  want_hosts+=("$new")
  check "swap $((i + 1)) ($d) soak passed" test -f "$RB_STATE_DIR/swap-$((i + 1))-${d%%.*}/soak.ok"
done

header "Brokers"
have="$(brokers_json | jq -r '.[].internal_rpc_address' | sort | paste -sd' ')"
want="$(printf '%s\n' "${want_hosts[@]}" | sort | paste -sd' ')"
info "brokers now: $have"
check "brokers are exactly the new hosts" [ "$have" = "$want" ]
check "versions" all_versions_ok
check "healthy" is_healthy
check "no under-replicated partitions" urp_zero
check "one replica per rack" placement_audit

header "Settings and seeds"
check "recovery rate at original" recovery_rate_is_original
check "seed rules (seed_servers unchanged, one live broker per AZ)" seeds_check
for s in "${SEED_DNS_NAMES[@]}"; do
  ip="$(resolve "$s")"; match=""
  for h in "${want_hosts[@]}"; do [[ "$(resolve "$h")" == "$ip" ]] && match="$h"; done
  check "$s resolves to a new broker (${match:-none})" [ -n "$match" ]
done
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
rb_summary
echo
info "Next: step_31_restore_temp_settings.sh, then step_99_stop_test_clients.sh"
