# Operations shared by swap.sh, the fast commission flow and scenarios.
# Requires common.sh and cluster.sh.

# decommission_wait <node_id> <out_json> [timeout]: start a decommission and poll until the broker
# is gone from the broker list. Writes {duration_sec, bytes_moved, polls} to out_json.
decommission_wait() {
  local id="$1" out="$2" timeout="${3:-$DECOM_TIMEOUT_SEC}"
  local t0; t0=$(epoch)
  rpk_exec redpanda admin brokers decommission "$id" >/dev/null || die "decommission of node $id rejected"
  log "decommissioning node $id"
  local moved=0 polls=0 st deadline=$(( t0 + timeout ))
  while brokers_json | jq -e --argjson id "$id" 'any(.[]; .node_id==$id)' >/dev/null; do
    (( $(epoch) >= deadline )) && die "decommission of node $id did not complete within ${timeout}s"
    st="$(admin "/v1/brokers/$id/decommission" 2>/dev/null || true)"
    if [[ -n "$st" ]]; then
      local m; m="$(jq '[.partitions[]?.bytes_moved // 0] | add // 0' <<<"$st" 2>/dev/null || echo 0)"
      (( m > moved )) && moved=$m
      echo "$st" > "$RESULTS/raw/decom-$id-last.json"
    fi
    polls=$((polls+1)); sleep 5
  done
  echo "$id" >> "$RESULTS/decommissioned.txt"
  local d=$(( $(epoch) - t0 ))
  jq -n --argjson d "$d" --argjson b "$moved" --argjson p "$polls" '{duration_sec:$d, bytes_moved_max_observed:$b, polls:$p}' > "$out"
  log "node $id decommissioned in ${d}s (bytes moved observed: $moved)"
}

# du_data <instance>: data directory size in bytes
du_data() { docker exec "$(cname "$1")" du -sb /var/lib/redpanda/data 2>/dev/null | cut -f1; }

# wait_settled_sampling <new-instance> <out_json>: wait_settled while sampling the new broker's
# data growth (peak MB/s) and the recovery bandwidth metric.
wait_settled_sampling() {
  local inst="$1" out="$2" timeout="${3:-$SETTLE_TIMEOUT_SEC}"
  local t0; t0=$(epoch)
  local prev; prev="$(du_data "$inst")"; local prev_t; prev_t=$(epoch)
  local peak=0 bw_peak=0 ok=0 deadline=$(( t0 + timeout ))
  while (( ok < 2 )); do
    (( $(epoch) >= deadline )) && die "cluster did not settle within ${timeout}s after $inst joined"
    sleep 10
    local cur now; cur="$(du_data "$inst")"; now=$(epoch)
    if (( now > prev_t )); then
      local r=$(( (cur - prev) / (now - prev_t) )); (( r > peak )) && peak=$r
    fi
    prev=$cur; prev_t=$now
    local bw; bw="$(admin_on "$inst" /metrics 2>/dev/null | awk '/^vectorized_raft_recovery_partition_movement_assigned_bandwidth/ {s+=$NF} END {printf "%d", s}')"
    (( ${bw:-0} > bw_peak )) && bw_peak=${bw:-0}
    if settled; then ok=$((ok+1)); else ok=0; fi
  done
  local d=$(( $(epoch) - t0 ))
  jq -n --argjson d "$d" --argjson p "$peak" --argjson bw "$bw_peak" --argjson final "$(du_data "$inst")" \
    '{settle_sec:$d, peak_ingest_bytes_per_sec:$p, peak_assigned_recovery_bandwidth:$bw, final_data_bytes:$final}' > "$out"
  log "settled after ${d}s (peak ingest on $inst: $((peak/1048576)) MiB/s)"
}

# full_gate <context>: the standard healthy-cluster gate used before and after operations
full_gate() {
  GATE_CTX="$1"
  gate "healthy" is_healthy
  gate "balancer idle" balancer_idle
  gate "no maintenance" no_maintenance
  gate "URP=0" urp_zero
  gate "versions" assert_versions
  gate "cluster uuid" check_cluster_uuid
  gate "placement audit" "$ROOT/scripts/audit-placement.sh" --label "$1"
  gate "seeds" "$ROOT/scripts/check-seeds.sh"
}

# baseline_from_csv <label> <phase>: median produce p99 / e2e p99 / consumer MB/s of complete windows
baseline_from_csv() {
  python3 - "$RESULTS/latency.csv" "$1" "$2" <<'EOF'
import csv, json, statistics, sys
f, label, phase = sys.argv[1:4]
rows = [r for r in csv.DictReader(open(f)) if r["label"] == label and r["phase"] == phase and r["phase_end"] == phase and r["status"] == "ok"]
def med(tool, col):
    v = [float(r[col]) for r in rows if r["tool"] == tool and r[col]]
    return statistics.median(v) if v else None
print(json.dumps({"produce_p99": med("produce", "p99_ms"), "e2e_p99": med("e2e", "p99_ms"),
                  "consume_mb_s": med("consume", "cons_mb_s"),
                  "windows": len({r["window_id"] for r in rows if r["tool"] == "produce"})}))
EOF
}

# windows_done <label> <phase>: number of complete produce windows for label that started and ended in phase
windows_done() {
  [[ -f "$RESULTS/latency.csv" ]] || { echo 0; return; }
  awk -F, -v l="$1" -v p="$2" 'NR>1 && $3==l && $4==p && $5==p && $6=="produce" && $NF=="ok" {n++} END {print n+0}' "$RESULTS/latency.csv"
}
