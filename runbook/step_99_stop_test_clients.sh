#!/usr/bin/env bash
# =============================================================================
# step_99_stop_test_clients.sh [--delete-topics] - end of the runbook
# =============================================================================
#   1. stops the test producer, load producer and verifier consumer
#   2. verifies the data: every acknowledged sequence number is in $TEST_TOPIC_SEQ,
#      in order; reports gaps seen by the consumer during the whole runbook
#   3. with --delete-topics: deletes the test topics (asks first)
# The test topics also delete their data on their own after $TEST_TOPIC_RETENTION_MS ms.
#
# TEST ONLY: this script only stops and verifies the runbook's own test topics (rb.test.*) and test
# clients. It does not change brokers, cluster settings or business topics.
# Later steps use these clients only for monitoring: the client health line in every step, the
# latency baseline for the step_11 rollback watcher, and the step_14 round-trip test. Without
# them those checks are skipped (or report "not started"); the migration steps still work.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
info "TEST ONLY: manages the runbook test topics and clients (rb.test.*), not the production change"

test_header "Final client status"
"$RB_DIR/tools/test_clients.sh" status || true

test_header "Stop clients"
if (( DRY_RUN )); then info "dry run: clients left running"
else "$RB_DIR/tools/test_clients.sh" stop; sleep 5; fi

test_header "Verify data"
check "every acknowledged message present and in order" "$RB_DIR/tools/test_clients.sh" verify
gaps="$(cat "$CLIENT_DIR/consumer.gaps" 2>/dev/null || echo 0)"
check "no gaps seen by the verifier consumer during the runbook (gaps=$gaps)" [ "$gaps" -eq 0 ]
errs="$(grep -ciE 'error' "$CLIENT_DIR/producer.err" 2>/dev/null || true)"
if [[ "${errs:-0}" -eq 0 ]]; then ok "no producer errors"; else warn "$errs producer error line(s): see $CLIENT_DIR/producer.err"; fi

if [[ " ${RB_ARGS[*]:-} " == *" --delete-topics "* ]]; then
  header "Delete test topics"
  # Group first: deleting the topic also drops the group's offsets, and the empty group vanishes.
  if rpk group describe "$TEST_CONSUMER_GROUP" >/dev/null 2>&1; then
    change "delete consumer group $TEST_CONSUMER_GROUP" rpk group delete "$TEST_CONSUMER_GROUP"
  fi
  change "delete $TEST_TOPIC_SEQ and $TEST_TOPIC_LOAD" rpk topic delete "$TEST_TOPIC_SEQ" "$TEST_TOPIC_LOAD"
fi
rb_summary
