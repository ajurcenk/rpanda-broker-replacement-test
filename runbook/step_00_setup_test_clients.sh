#!/usr/bin/env bash
# =============================================================================
# step_00_setup_test_clients.sh - test topics, test data and test clients
# =============================================================================
# Run ONCE, before step_01. Leave the clients running for the whole runbook.
# Every later step prints the client health line, for example:
#     clients: producer up, consumer up, e2e p99 35 ms, gaps 0, producer errors 0
#
# What it does:
#   1. checks that rpk can reach the cluster with TLS + SCRAM
#   2. creates two test topics (RF=3, Tiered Storage on, 2-day retention):
#        $TEST_TOPIC_SEQ   1 partition   strict ordering / no-gap check
#        $TEST_TOPIC_LOAD  6 partitions  background load
#   3. writes $TEST_PRELOAD_MB MB of test data to the load topic
#   4. starts the producer, load producer and verifier consumer (tools/test_clients.sh)
#   5. waits one minute and shows their health
#
# Safe to re-run: existing topics are kept, running clients are not started twice.
# The SCRAM user needs: create/describe/write/read on rb.test.*, and the consumer group.
#
# Undo: step_99_stop_test_clients.sh (stops clients, verifies data, optionally deletes topics).
#
# TEST ONLY: this script only creates and starts the runbook's own test topics (rb.test.*) and test
# clients. It does not change brokers, cluster settings or business topics.
# Later steps use these clients only for monitoring: the client health line in every step, the
# latency baseline for the step_11 rollback watcher, and the step_14 round-trip test. Without
# them those checks are skipped (or report "not started"); the migration steps still work.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
info "TEST ONLY: manages the runbook test topics and clients (rb.test.*), not the production change"

test_header "1. Cluster connectivity"
check "rpk reaches the Kafka API" rpk cluster info
check "Admin API entry point with an elected controller" admin_entry
ts_enabled="$(cfg_get cloud_storage_enabled)"
info "cloud_storage_enabled=$ts_enabled"
(( RB_FAILS == 0 )) || rb_summary

test_header "2. Test topics"
create_topic() {   # create_topic <name> <partitions>
  local t="$1" p="$2"
  if rpk topic describe "$t" >/dev/null 2>&1; then ok "topic $t already exists"; return; fi
  local cfg=(-c retention.ms="$TEST_TOPIC_RETENTION_MS" -c cleanup.policy=delete)
  # Tiered Storage on the test topics too, so fast commission evidence covers them.
  [[ "$ts_enabled" == true ]] && cfg+=(-c redpanda.remote.write=true -c redpanda.remote.read=true)
  change "create topic $t ($p partitions, RF=3)" rpk topic create "$t" -p "$p" -r 3 "${cfg[@]}"
}
create_topic "$TEST_TOPIC_SEQ" 1
create_topic "$TEST_TOPIC_LOAD" 6

test_header "3. Test data"
hw_total="$(rpk topic describe "$TEST_TOPIC_LOAD" -p | awk 'NR>1 {s+=$NF} END {print s+0}')"
if (( hw_total > 0 )); then
  ok "load topic already holds $hw_total records, skipping preload"
else
  info "writing about $TEST_PRELOAD_MB MB of 1 KiB records to $TEST_TOPIC_LOAD"
  if (( ! DRY_RUN )); then
    # base64 of random bytes, 1024 characters per line = one record per line.
    head -c $(( TEST_PRELOAD_MB * 1024 * 768 )) /dev/urandom | base64 -w 1024 \
      | rpk topic produce "$TEST_TOPIC_LOAD" --acks -1 -z none -o '' >/dev/null
  fi
  ok "preload written ($(rpk topic describe "$TEST_TOPIC_LOAD" -p | awk 'NR>1 {s+=$NF} END {print s+0}') records)"
fi

test_header "4. Start test clients"
if (( DRY_RUN )); then info "dry run: would run tools/test_clients.sh start"
else "$RB_DIR/tools/test_clients.sh" start; fi

test_header "5. Client health after 60 s"
(( DRY_RUN )) || sleep 60
"$RB_DIR/tools/test_clients.sh" status
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok

# The end-to-end p99 now is the reference for later steps (rollback watcher, go/no-go).
p99="$(client_p99)"
[[ -n "$p99" ]] && state_set "$RB_STATE_DIR/baseline.client_p99_ms" "$p99" && info "baseline client e2e p99: $p99 ms"
rb_summary
