#!/usr/bin/env bash
# =============================================================================
# step_14_wait_settle.sh <swap> - Phase 2, runbook step 17. READ-ONLY.
# =============================================================================
# After the new broker joins, continuous balancing moves replicas onto it.
# This step waits until that is finished:
#   healthy, balancer idle, no partition moves in flight, no under-replicated partitions,
#   and the new broker holds at least 80% as many replicas as its rack-mates
# (all seen twice in a row, $POLL_SEC s apart), then checks rack placement.
# Progress and test-client health are printed every $POLL_SEC seconds.
# Finally it writes and reads one record through a test partition led by the new broker
# (proves the client path: advertised name, TLS certificate, security groups).
# Gives up after $SETTLE_TIMEOUT_MIN minutes (nothing is changed; investigate and re-run).
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
check "new broker joined (step_13)" test -f "$SWAP_DIR/new_id"
(( RB_FAILS == 0 )) || rb_summary
new_id="$(state_get "$SWAP_DIR/new_id")"

header "Waiting for replicas to move onto node $new_id"
t0=$(date +%s)
wait_settled "$SETTLE_TIMEOUT_MIN" "$new_id" || die "not settled after $SETTLE_TIMEOUT_MIN minutes"
secs=$(( $(date +%s) - t0 ))
ok "settled after ${secs}s ($(( $(date +%s) - $(state_get "$SWAP_DIR/joined_at") ))s since join)"
state_set "$SWAP_DIR/settle_seconds" "$secs"

header "After settling"
check "one replica per rack" placement_audit
check "node $new_id holds its share of replicas ($(replica_count "$new_id"))" has_rack_share "$new_id"
test_header "Client path through the new broker"
# TEST ONLY: proves that a client can write to and read from a partition LED by the new broker,
# through its advertised name, port 9092, TLS certificate and SASL login, before step_15 moves
# real leaderships onto it. It touches only the test topic rb.test.load: it may move the
# leadership of ONE test partition to the new broker and writes one marker record there.
# Business topics are not touched. Requires the test topics from step_00 (skipped otherwise).
new_is_leader() { [ "$(admin "/v1/partitions/kafka/$TEST_TOPIC_LOAD/$1" | jq -r .leader_id)" = "$new_id" ]; }
p="$(admin /v1/cluster/partitions | jq -r --arg t "$TEST_TOPIC_LOAD" --argjson id "$new_id" \
      '[.[] | select(.topic == $t and any(.replicas[]; .node_id == $id))][0].partition_id // empty')"
if [[ -z "$p" ]]; then
  warn "no $TEST_TOPIC_LOAD replica on node $new_id; round-trip test skipped (run step_00 first?)"
else
  if ! new_is_leader "$p"; then
    change "make node $new_id leader of test partition $TEST_TOPIC_LOAD/$p" \
      rpk cluster partitions transfer-leadership --partition "$TEST_TOPIC_LOAD/$p:$new_id"
    for _ in 1 2 3 4 5 6; do new_is_leader "$p" && break; sleep 5; done
  fi
  if (( ! DRY_RUN )); then
    check "node $new_id leads $TEST_TOPIC_LOAD/$p" new_is_leader "$p"
    marker="rb-roundtrip-$(date +%s)-$RANDOM"
    off="$(echo "$marker" | rpk topic produce "$TEST_TOPIC_LOAD" -p "$p" --acks -1 -o '%o\n')"
    got="$(timeout 30 rpk topic consume "$TEST_TOPIC_LOAD" -p "$p" -o "$off" -n 1 -f '%v\n' || true)"
    check "record written to and read back from node $new_id ($TEST_TOPIC_LOAD/$p offset $off)" [ "$got" = "$marker" ]
  fi
fi

# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/settled.ok" "$(ts)"
rb_summary
echo
info "Next: step_15_decommission.sh $SWAP_DNS [--maintenance]"
