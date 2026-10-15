#!/usr/bin/env bash
# Scenario 6: stop both eu-central-1a brokers. Every partition must keep a majority, so produce on
# the sequencer topic with acks=all still succeeds. Then restart them and wait for health.
set -euo pipefail
SC_NAME=s06-rack-failure
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
healthy5 pre
mapfile -t A < <(rack_instances eu-central-1a)
[[ ${#A[@]} == 2 ]] || die "expected 2 brokers in eu-central-1a, found: ${A[*]}"
for i in "${A[@]}"; do "$ROOT/scripts/broker.sh" stop "$i"; done
note "stopped ${A[*]}"
retry_until 120 3 "1a brokers seen as down" bash -c "source '$ROOT/scripts/lib/common.sh'; [[ \$(brokers_json | jq '[.[]|select(.is_alive==false)]|length') == 2 ]]"
sleep 15
lc="$(admin /v1/cluster/health_overview | jq '.leaderless_count')"
note "leaderless partitions with eu-central-1a down: $lc"
[[ "$lc" == 0 ]] || die "$lc partitions lost their leader"
t0=$(epoch)
tools python3 /work/client/producer.py --preload $((200*1024)) > "$RESULTS/raw/s06-produce.log" 2>&1 || die "acks=all produce failed with eu-central-1a down: $(tail -2 "$RESULTS/raw/s06-produce.log")"
note "acks=all produce of 200 sequenced records succeeded with eu-central-1a down ($(( $(epoch) - t0 ))s): $(tail -1 "$RESULTS/raw/s06-produce.log")"
grep -q 'errors 0, unflushed 0' "$RESULTS/raw/s06-produce.log" || die "produce had errors"
for i in "${A[@]}"; do "$ROOT/scripts/broker.sh" start "$i"; done
for i in "${A[@]}"; do wait_back "$i"; done
wait_settled 1800
healthy5 post
sc_result PASS "with ${A[*]} down: 0 leaderless partitions, acks=all produce succeeded; brokers restarted and cluster healthy"
