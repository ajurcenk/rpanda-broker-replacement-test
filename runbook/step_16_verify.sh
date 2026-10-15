#!/usr/bin/env bash
# =============================================================================
# step_16_verify.sh <swap> - Phase 2, runbook step 19. READ-ONLY.
# =============================================================================
# After the decommission completes:
#   * the old node ID is gone from the broker list
#   * cluster healthy, no under-replicated partitions, one replica per rack
#   * FAST COMMISSION EVIDENCE: for every partition replica on the new broker, compare its
#     local start offset (metric vectorized_cluster_partition_start_offset) with the other
#     replicas of the same partition. A higher start offset means the new replica started
#     from data already in object storage instead of copying the full local log.
#   * local data of the new broker vs the median of the others (vectorized_storage_log_partition_size;
#     note: rpk cluster logdirs describe includes object-storage bytes for Tiered Storage topics)
# Evidence is written to state/swap-*/fast-commission-evidence.txt.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
old_id="$(state_get "$SWAP_DIR/old_id")"; new_id="$(state_get "$SWAP_DIR/new_id")"
check "decommission completed (step_15)" test -f "$SWAP_DIR/decommissioned.ok"
(( RB_FAILS == 0 )) || rb_summary

header "Cluster after decommission"
check "old node $old_id is gone from the broker list" node_gone "$old_id"
check "new node $new_id active and alive" node_active "$new_id"
info "waiting for the balancer to finish any follow-up moves"
wait_settled 60 || warn "balancer still moving partitions after 60 min"
check "healthy" is_healthy
check "no under-replicated partitions" urp_zero
check "one replica per rack" placement_audit

header "Fast commission evidence (node $new_id)"
ev="$SWAP_DIR/fast-commission-evidence.txt"
tmp="$(mktemp -d)"
# 1. From every broker's /metrics, per replica: local start offset and local size in bytes.
#    Output lines: "<node> <topic> <partition> <start offset> <local bytes>"
while read -r id host; do
  admin_on "$host" /metrics 2>/dev/null | awk -v id="$id" '
    /namespace="kafka"/ && (/^vectorized_cluster_partition_start_offset\{/ || /^vectorized_storage_log_partition_size\{/) {
      match($0, /topic="[^"]*"/);     t = substr($0, RSTART + 7, RLENGTH - 8)
      match($0, /partition="[^"]*"/); p = substr($0, RSTART + 11, RLENGTH - 12)
      if ($0 ~ /^vectorized_cluster/) start[t " " p] = int($NF); else size[t " " p] = int($NF) }
    END { for (k in start) print id, k, start[k], size[k] + 0 }' >> "$tmp/replicas.txt"
done < <(brokers_json | jq -r '.[] | "\(.node_id) \(.internal_rpc_address)"')
# 2. Tiered Storage flag of every topic that has a replica on the new broker: "<topic> <true|false>"
for t in $(admin /v1/cluster/partitions | jq -r --argjson id "$new_id" '[.[] | select(any(.replicas[]; .node_id==$id)) | .topic] | unique[]'); do
  rw="$(rpk topic describe "$t" -c | awk '$1=="redpanda.remote.write" {print $2}')"
  echo "$t ${rw:-false}" >> "$tmp/topics.txt"
done
# 3. Per topic: replicas on the new broker; how many start ABOVE every other replica of the
#    same partition ("advantaged"); average local bytes per replica, new broker vs the others.
awk -v new="$new_id" '
  NR == FNR { ts[$1] = $2; next }                                  # first file: topics.txt
  { st[$2 " " $3, $1] = $4; sz[$2 " " $3, $1] = $5; parts[$2 " " $3] = 1; nodes[$1] = 1 }
  END {
    for (tp in parts) {
      if (!((tp, new) in st)) continue
      split(tp, a, " "); t = a[1]; if (!(t in ts)) continue
      mx = -1; ob = 0; on = 0
      for (n in nodes) if (n != new && (tp, n) in st) { if (st[tp, n] > mx) mx = st[tp, n]; ob += sz[tp, n]; on++ }
      if (mx < 0) continue
      total[t]++; newb[t] += sz[tp, new]; oldb[t] += (on ? ob / on : 0)
      if (st[tp, new] > mx) adv[t]++
    }
    for (t in total)
      printf "%-36s TS=%-5s replicas_on_new=%-4d advantaged=%-4d local_bytes_per_replica new=%-10d others=%d\n",
             t, ts[t], total[t], adv[t] + 0, newb[t] / total[t], oldb[t] / total[t]
  }' "$tmp/topics.txt" "$tmp/replicas.txt" | sort > "$ev"
sed 's/^/  /' "$ev"
sum_col() { awk -v want="$1" -v col="$2" '($2 == "TS=" want) == 1 {split($col, x, "="); s += x[2]} END {print s + 0}' "$ev"; }
ts_adv="$(sum_col true 4)"; ts_tot="$(sum_col true 3)"; nots_adv="$(sum_col false 4)"
check "Tiered Storage replicas on the new broker that started from object storage: $ts_adv / $ts_tot" [ "$ts_adv" -gt 0 ]
info "non-Tiered-Storage replicas showing an advantage (expected 0): $nots_adv"

header "Local data per broker (metric vectorized_storage_log_partition_size)"
awk '{b[$1] += $5} END {for (k in b) print k, b[k]}' "$tmp/replicas.txt" | sort -n > "$tmp/disk.txt"
awk '{printf "  node %-4s %8.1f MiB\n", $1, $2 / 1048576}' "$tmp/disk.txt"
new_b="$(awk -v n="$new_id" '$1==n {print $2}' "$tmp/disk.txt")"
med="$(awk -v n="$new_id" '$1!=n {print $2}' "$tmp/disk.txt" | sort -n | awk '{a[NR]=$1} END {print (NR ? a[int((NR+1)/2)] : 0)}')"
[[ -n "$new_b" && "${med:-0}" -gt 0 ]] && info "new broker holds $(awk -v a="$new_b" -v b="$med" 'BEGIN {printf "%.0f", 100*a/b}')% of the other brokers' median local data"
{ echo; echo "local bytes per node:"; cat "$tmp/disk.txt"; } >> "$ev"
rm -rf "$tmp"

header "Clients"
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/verified.ok" "$(ts)"
rb_summary
echo
info "Next: step_17_restore_recovery_rate.sh $SWAP_DNS (always run it), then step_18_dns_repoint.sh"
