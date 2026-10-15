#!/usr/bin/env bash
# Scenario 7: after all swaps, an empty-disk broker with the unchanged seed list joins through the
# repointed aliases; then again with two of the three seed aliases pointing at an unused IP.
set -euo pipefail
SC_NAME=s07-join-via-repointed-seeds
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
healthy5 pre
note "seed aliases now: b1->$(alias_target b1) b3->$(alias_target b3) b5->$(alias_target b5)"
join_retire() {  # join_retire <label>
  local t0; t0=$(epoch)
  "$ROOT/scripts/broker.sh" start tmp-1
  local id; id="$(wait_joined tmp-1 --expect-new)"
  local dt=$(( $(epoch) - t0 ))
  save_logs tmp-1; cp "$RESULTS/scenarios/$SC_NAME-tmp-1.log" "$RESULTS/scenarios/$SC_NAME-tmp-1-$1.log"
  local via; via="$(grep -oE 'Requesting node ID +for node UUID [^ ]+ from \{host: [^,]+' "$RESULTS/scenarios/$SC_NAME-tmp-1-$1.log" | sed -E 's/.*host: //' | sort | uniq -c | tr '\n' ' ')"
  note "$1: tmp-1 joined as node $id in ${dt}s (node-ID requests per seed: $via)"
  decommission_wait "$id" "$RESULTS/raw/$SC_NAME-decom-$1.json"
  "$ROOT/scripts/broker.sh" rm tmp-1
}
join_retire all-seeds
wait_settled 1800
o1="$(alias_target b1)"; o3="$(alias_target b3)"
"$ROOT/scripts/dns-repoint.sh" b1 --test-ip "$UNUSED_IP" "scenario 7: seed alias unreachable"
"$ROOT/scripts/dns-repoint.sh" b3 --test-ip "$UNUSED_IP" "scenario 7: seed alias unreachable"
note "b1 and b3 temporarily point at $UNUSED_IP; only b5 is reachable"
restore() { "$ROOT/scripts/dns-repoint.sh" b1 "$o1" || true; "$ROOT/scripts/dns-repoint.sh" b3 "$o3" || true; }
trap 'restore; sc_trap' EXIT
join_retire one-seed
restore; trap sc_trap EXIT
wait_settled 1800
healthy5 post
sc_result PASS "tmp-1 joined via repointed aliases, and again with only b5 reachable; aliases restored"
