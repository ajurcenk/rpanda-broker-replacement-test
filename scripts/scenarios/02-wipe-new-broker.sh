#!/usr/bin/env bash
# Scenario 2 (and 3 with --seed): ephemeral NVMe wipe of a new broker. Remove its container and
# volume, start a replacement with an empty disk (must get a NEW id, no second cluster), decommission
# the dead id, repoint the alias.
set -euo pipefail
SEED=0; [[ "${1:-}" == "--seed" ]] && SEED=1
# --continue <dead-id>: finish a run interrupted after the replacement joined (decommission dead id, repoint)
SC_NAME=$([[ $SEED == 1 ]] && echo s03-wipe-seed-broker || echo s02-wipe-new-broker)
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
CONT=0; [[ "${2:-}" == "--continue" || "${1:-}" == "--continue" ]] && CONT=1
(( CONT )) || healthy5 pre
alias=$([[ $SEED == 1 ]] && echo b1 || echo b2)
repl=$([[ $SEED == 1 ]] && echo sp-4 || echo sp-1)
victim="$(alias_target "$alias")"; vid="$(id_of "$victim")"
if (( CONT )); then
  rid="$(id_of "$repl")"; uuid0="$(cat "$RESULTS/cluster-uuid")"
  note "continuing interrupted run: dead $victim (node $vid), replacement $repl (node $rid)"
else
[[ "${INST_RACK[$victim]}" == "${INST_RACK[$repl]}" ]] || die "replacement rack mismatch"
uuid0="$(check_cluster_uuid)"; topics0="$(rpk_exec topic list | awk 'NR>1 {print $1}' | sort | sha256sum)"
note "wiping $victim (node $vid, behind alias $alias): container and volume removed"
save_logs "$victim"
"$ROOT/scripts/broker.sh" rm "$victim"
retry_until 120 3 "node $vid seen as down" bash -c "source '$ROOT/scripts/lib/common.sh'; brokers_json | jq -e 'any(.[]; .node_id==$vid and .is_alive==false)' >/dev/null"
note "alias $alias now resolves to a removed instance (simulated failure)"
if "$ROOT/scripts/check-seeds.sh" > "$RESULTS/raw/$SC_NAME-check-seeds-during.txt" 2>&1; then
  (( SEED )) && note "check-seeds unexpectedly passed while seed alias $alias points at a removed instance" || note "check-seeds passed during the outage, as expected ($alias is not a seed alias)"
else
  (( SEED )) && note "check-seeds failed during the outage, as expected: $(tail -1 "$RESULTS/raw/$SC_NAME-check-seeds-during.txt" | tr -d '\033' | cut -c1-160)" || note "check-seeds failed during a non-seed outage: $(tail -1 "$RESULTS/raw/$SC_NAME-check-seeds-during.txt" | tr -d '\033' | cut -c1-160)"
fi
t0=$(epoch)
"$ROOT/scripts/broker.sh" start "$repl"
rid="$(wait_joined "$repl" --expect-new)"
note "replacement $repl joined with NEW node id $rid in $(( $(epoch) - t0 ))s (dead id was $vid)"
[[ "$(check_cluster_uuid)" == "$uuid0" ]] || die "cluster UUID changed: second cluster"
[[ "$(admin_on "$repl" /v1/cluster/uuid | jq -r .cluster_uuid)" == "$uuid0" ]] || die "$repl reports a different cluster UUID"
[[ "$(rpk_exec topic list | awk 'NR>1 {print $1}' | sort | sha256sum)" == "$topics0" ]] || die "topic list differs"
n="$(brokers_json | jq length)"; note "broker list: $n entries (5 alive + dead node $vid)"
fi
t1=$(epoch)
# rpk refuses (but exits 0) when the broker's version is unknown, i.e. it is offline; a hidden
# --force flag (mentioned in the help text, not listed under Flags) bypasses the version check.
out="$(rpk_exec redpanda admin brokers decommission "$vid" 2>&1 || true)"
if grep -q -- '--force' <<<"$out"; then
  note "plain 'rpk redpanda admin brokers decommission $vid' refused for the unreachable broker (exit 0): $(tr '\n' ' ' <<<"$out")"
  out="$(rpk_exec redpanda admin brokers decommission "$vid" --force 2>&1)" || die "decommission --force of dead node $vid failed: $out"
  note "accepted with the hidden --force flag: $(tr '\n' ' ' <<<"$out")"
else
  note "plain decommission accepted for the unreachable broker: $(tr '\n' ' ' <<<"$out")"
fi
retry_until 60 2 "node $vid draining" bash -c "source '$ROOT/scripts/lib/common.sh'; brokers_json | jq -e 'any(.[]; .node_id==$vid and .membership_status==\"draining\") or (any(.[]; .node_id==$vid) | not)' >/dev/null"
retry_until "$DECOM_TIMEOUT_SEC" 5 "dead node $vid removed" bash -c "source '$ROOT/scripts/lib/common.sh'; brokers_json | jq -e 'any(.[]; .node_id==$vid) | not' >/dev/null"
note "dead node $vid decommissioned in $(( $(epoch) - t1 ))s"
wait_settled 1800
"$ROOT/scripts/dns-repoint.sh" "$alias" "$repl"
sleep $((DNS_TTL + 1))
healthy5 post
sc_result PASS "wiped $victim (node $vid); $repl joined with new id $rid, same cluster UUID, same topics; dead id decommissioned (offline broker needs the hidden --force flag); $alias repointed to $repl"
