#!/usr/bin/env bash
# Scenario 1: recommission before a decommission completes; then show that a broker whose
# decommission completed cannot rejoin by restarting its old container (with its old data).
set -euo pipefail
SC_NAME=s01-abort-decommission
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
healthy5 pre
victim="$(alias_target b2)"; vid="$(id_of "$victim")"
note "part 1: decommission $victim (node $vid), then recommission mid-flight"
# Slow recovery so the decommission cannot finish before the recommission (restored below and on exit).
"$ROOT/scripts/recovery-rate.sh" raise $((1 * 1048576))
trap '"$ROOT/scripts/recovery-rate.sh" restore s01-exit || true; sc_trap' EXIT
note "raft_learner_recovery_rate temporarily lowered to 1 MiB/s to keep the decommission in flight"
rpk_exec redpanda admin brokers decommission "$vid" >/dev/null
retry_until 120 2 "decommission in progress" bash -c "source '$ROOT/scripts/lib/common.sh'; admin /v1/brokers/$vid/decommission | jq -e '(.replicas_left // 0) > 0' >/dev/null"
sleep 10
admin "/v1/brokers/$vid/decommission" > "$RESULTS/raw/s01-decom-midflight.json"
note "mid-flight: $(jq -c '{finished, replicas_left}' "$RESULTS/raw/s01-decom-midflight.json")"
rpk_exec redpanda admin brokers recommission "$vid" >/dev/null
note "recommissioned node $vid"
"$ROOT/scripts/recovery-rate.sh" restore s01-part1; trap sc_trap EXIT
retry_until 120 2 "node $vid active again" bash -c "source '$ROOT/scripts/lib/common.sh'; brokers_json | jq -e 'any(.[]; .node_id==$vid and .membership_status==\"active\")' >/dev/null"
wait_settled 1800
healthy5 after-recommission
note "part 2: add sp-1, fully decommission it, then restart its old container"
"$ROOT/scripts/broker.sh" start sp-1
sid="$(wait_joined sp-1 --expect-new)"
wait_settled 1800
decommission_wait "$sid" "$RESULTS/raw/s01-decom-sp1.json"
note "sp-1 (node $sid) decommissioned in $(jq .duration_sec "$RESULTS/raw/s01-decom-sp1.json")s"
docker restart -t 30 "$(cname sp-1)" >/dev/null
note "restarted sp-1's old container with its old data; observing for 90s"
sleep 90
save_logs sp-1
st="$(brokers_json | jq -c --argjson id "$sid" '[.[] | select(.node_id==$id)]')"
all="$(rpk_exec redpanda admin brokers list --include-decommissioned 2>&1 || true)"
echo "$all" > "$RESULTS/raw/s01-brokers-incl-decom.txt"
check_cluster_uuid >/dev/null
rejoin_lines="$(grep -ciE 'decommission|removed from the cluster|not a member|rejected' "$RESULTS/scenarios/$SC_NAME-sp-1.log" || true)"
[[ "$st" == "[]" ]] || die "decommissioned node $sid is back in the broker list: $st"
note "node $sid not in the active broker list after restart (log lines mentioning decommission/removal: $rejoin_lines)"
"$ROOT/scripts/broker.sh" rm sp-1
wait_settled 900
healthy5 post
sc_result PASS "recommission mid-decommission restored $victim (node $vid) to active; decommissioned sp-1 (node $sid) did not rejoin after restart with old data; cluster UUID unchanged"
