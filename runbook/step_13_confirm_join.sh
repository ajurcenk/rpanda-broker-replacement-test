#!/usr/bin/env bash
# =============================================================================
# step_13_confirm_join.sh <swap> - Phase 2, runbook steps 15 and 16. READ-ONLY.
# =============================================================================
# Run right after starting Redpanda on the new host. Waits (up to 15 minutes) until
# the new broker appears in the broker list, then checks:
#   * it got a NEW node ID, never used before in this cluster (state/node_ids_seen.txt)
#   * rack = $SWAP_RACK, membership active, alive, version $EXPECTED_VERSION
#   * its own config: empty_seed_starts_cluster=false, seed_servers = the seed DNS names,
#     and it does not list itself as a seed
#   * seed rules still hold (Path A: no seed change; if OLD is behind a seed name, that
#     name still points at OLD, which is correct because OLD is still a member)
#
#   * the new broker answers on 9092 and 9644 BY ITS OWN NAME (the name clients will use)
#
# Safe to re-run: a node ID recorded by an earlier run for this same swap is not "reused".
#
# If the broker does not appear: check its log on the host (journalctl -u redpanda).
# Look for "Requesting node ID ... from {host: ...}" lines (seed connection attempts).
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg

header "Waiting for $SWAP_NEW to join"
deadline=$(( $(date +%s) + 900 ))
new_id=""
while :; do
  new_id="$(broker_id_of_host "$SWAP_NEW")"
  if [[ -n "$new_id" ]] && node_active "$new_id" \
     && [[ -n "$(brokers_json | jq -r --argjson id "$new_id" '.[] | select(.node_id==$id) | .version // empty')" ]]; then
    break
  fi
  (( $(date +%s) < deadline )) || die "$SWAP_NEW did not join as an active, alive broker within 15 minutes"
  info "not joined yet (node id: ${new_id:-none}) | clients: $(client_health_line)"
  sleep "$POLL_SEC"
done
ok "$SWAP_NEW joined as node $new_id"

# Safe to re-run: an earlier run of this step for THIS swap may already have recorded the ID.
earlier_id="$(state_get "$SWAP_DIR/new_id")"
never_seen() { ! grep -qx "$1" "$RB_STATE_DIR/node_ids_seen.txt" 2>/dev/null; }

header "New broker identity"
if [[ "$earlier_id" == "$new_id" ]]; then
  ok "node id $new_id is new (recorded for this swap by an earlier run of this step)"
else
  check "node id $new_id was never used before" never_seen "$new_id"
  never_seen "$new_id" && echo "$new_id" >> "$RB_STATE_DIR/node_ids_seen.txt"
  state_set "$SWAP_DIR/new_id" "$new_id"
  state_set "$SWAP_DIR/joined_at" "$(date +%s)"
fi
check "rack is $SWAP_RACK" node_rack_is "$new_id" "$SWAP_RACK"
check "version $EXPECTED_VERSION" all_versions_ok

header "New broker configuration"
cfg="$(admin_on "$SWAP_NEW" /v1/node_config)"
check "empty_seed_starts_cluster = false" jq -e '.empty_seed_starts_cluster == false' <<<"$cfg"
check "no node_id override in config" jq -e '(.node_id_overrides // []) | length == 0' <<<"$cfg"
want="$(printf '%s\n' "${SEED_DNS_NAMES[@]}" | sort | paste -sd,)"
got="$(jq -r '[.seed_servers[].host.address] | sort | join(",")' <<<"$cfg")"
check "seed_servers = $want" [ "$got" = "$want" ]
check "does not list itself ($SWAP_NEW) as a seed" [ "${got/$SWAP_NEW/}" = "$got" ]
check "advertised RPC name is $SWAP_NEW" jq -e --arg h "$SWAP_NEW" '.advertised_rpc_api.address == $h' <<<"$cfg"

header "Reachability through the new broker's own name"
# Clients use the advertised name from the metadata, so these must answer by NAME.
check "$SWAP_NEW resolves to $(resolve "$SWAP_NEW")" [ -n "$(resolve "$SWAP_NEW")" ]
check "$SWAP_NEW:9092 (Kafka) answers" port_open "$SWAP_NEW" 9092
check "$SWAP_NEW:$ADMIN_PORT (Admin API) answers" port_open "$SWAP_NEW" "$ADMIN_PORT"
in_metadata() { rpk cluster info -b | grep -qw "$1"; }
check "$SWAP_NEW is in the metadata clients receive" in_metadata "$SWAP_NEW"
info "from a CLIENT network also run: dig +short $SWAP_NEW   and   nc -zv $SWAP_NEW 9092"

header "Seeds and clients"
check "seed rules" seeds_check
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
rb_summary
echo
info "Next: step_14_wait_settle.sh $SWAP_DNS"
