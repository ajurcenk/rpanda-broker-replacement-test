#!/usr/bin/env bash
# Scenario 8 (runs inside a seed swap, as swap.sh --after-repoint hook): start a temporary broker
# within one TTL of repointing a seed alias. Record whether it joins, how long it takes, and any
# seed connection errors. Then decommission and remove it.
set -euo pipefail
SC_NAME=s08-join-after-repoint
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
alias="${1:-b1}"
last="$(grep ",$alias," "$RESULTS/dns.log" | tail -1)"
rp_ts="$(cut -d, -f1 <<<"$last")"
age=$(( $(epoch) - $(date -d "$rp_ts" +%s) ))
note "starting tmp-1 ${age}s after repoint of $alias ($last); TTL=${DNS_TTL}s"
(( age <= DNS_TTL )) || warn "start is ${age}s after the repoint, more than one TTL"
t0=$(epoch)
"$ROOT/scripts/broker.sh" start tmp-1
id="$(wait_joined tmp-1 --expect-new)"
dt=$(( $(epoch) - t0 ))
save_logs tmp-1
errs="$(grep -iE 'seed|cluster_discovery' "$RESULTS/scenarios/$SC_NAME-tmp-1.log" | grep -ciE 'error|fail|timeout|unavailable' || true)"
note "tmp-1 joined as node $id in ${dt}s; seed/discovery error lines: $errs"
grep -iE 'cluster_discovery' "$RESULTS/scenarios/$SC_NAME-tmp-1.log" | head -10 > "$RESULTS/raw/s08-discovery-lines.txt" || true
decommission_wait "$id" "$RESULTS/raw/s08-decom.json"
"$ROOT/scripts/broker.sh" rm tmp-1
sc_result PASS "tmp-1 started ${age}s after repointing $alias, joined as node $id in ${dt}s, $errs seed error lines; decommissioned and removed"
