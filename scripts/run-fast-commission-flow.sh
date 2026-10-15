#!/usr/bin/env bash
# run-fast-commission-flow.sh: prove fast commission is OFF, measure a broker join without it
# (Phase A), enable it, and prove the next join copies far less (Phase B). Uses tmp-1.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"
source "$ROOT/scripts/lib/ops.sh"
mkdir -p "$RESULTS/raw"
T=tmp-1
fcs() { swaps_csv "fc,$T,$T,$1,${2:-}"; log "[fc] $1 ${2:-}"; }
cleanup() { local rc=$?; "$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true; (( rc == 0 )) || fcs aborted "rc=$rc"; }
trap cleanup EXIT

ts_topics() { rpk_exec topic list | awk 'NR>1 && $1!="'"$CONTROL_TOPIC"'" && $1 !~ /^_/ {print $1}'; }

# join_measure <label> <expect-advantage>
join_measure() {
  local label="$1" adv="$2"
  phase join
  docker volume inspect "$(vname $T)" >/dev/null 2>&1 && die "$T volume exists; must start empty"
  local t0; t0=$(epoch)
  "$ROOT/scripts/broker.sh" start "$T"
  local id; id="$(wait_joined "$T" --expect-new)"
  fcs "$label-joined" "node_id=$id"
  phase settle
  wait_settled_sampling "$T" "$RESULTS/raw/settle-$label.json"
  local t1; t1=$(epoch)
  fcs "$label-settled" "join_to_settled_sec=$((t1 - t0))"
  echo "$id" > "$RESULTS/raw/fc-$label-node-id"
  echo "$((t1 - t0))" > "$RESULTS/raw/fc-$label-join-to-settled"
}
# retire_tmp <label>
retire_tmp() {
  local label="$1" id; id="$(id_of "$T")"
  phase decommission
  decommission_wait "$id" "$RESULTS/raw/decom-$label.json"
  "$ROOT/scripts/broker.sh" rm "$T"
  fcs "$label-retired" "$(jq -c . "$RESULTS/raw/decom-$label.json" | tr ',' ';')"
  phase post-verify
  wait_settled 900
  full_gate "fc-$label-retired"
}
timings_json() {  # timings_json <label>
  jq -n --argjson js "$(cat "$RESULTS/raw/fc-$1-join-to-settled")" --slurpfile s "$RESULTS/raw/settle-$1.json" \
    '{join_to_settled_sec:$js, settle:$s[0]}' > "$RESULTS/raw/timings-fc-$1.json"
}

# ---------------- Phase A: fast commission disabled ----------------
GATE_CTX="fc-A"
if [[ ! -f "$RESULTS/fast-commission-baseline.json" || ! -f "$RESULTS/raw/decom-baseline.json" ]]; then
  phase baseline
  full_gate "fc-A-pre"
  gate "TS check pre-a" "$ROOT/scripts/check-tiered-storage.sh" --label pre-a
  gate "fast commission disabled" "$ROOT/scripts/check-fast-commission.sh" --config --expect disabled --label baseline
  "$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true
  "$ROOT/scripts/perf-latency.sh" --background --label fc-disabled
  retry_until 900 10 "2 baseline windows" bash -c "source '$ROOT/scripts/lib/common.sh'; source '$ROOT/scripts/lib/ops.sh'; (( \$(windows_done fc-disabled baseline) >= 2 ))"
  if ! container_running "$T"; then join_measure baseline no; fi
  timings_json baseline
  gate "evidence baseline (no advantage)" "$ROOT/scripts/check-fast-commission.sh" --evidence "$T" --label baseline --expect-advantage no --timings "$RESULTS/raw/timings-fc-baseline.json"
  # record earliest-read digests (served from MinIO) for step 13
  mapfile -t TPS < <(for t in $(ts_topics); do echo "$t/0"; done)
  py read_check.py record pre-b "${TPS[@]}" > /dev/null
  retire_tmp baseline
  "$ROOT/scripts/perf-latency.sh" --stop
fi

# ---------------- Phase B: enable and verify ----------------
GATE_CTX="fc-B"
phase baseline
cur="$(cfg_get initial_retention_local_target_bytes_default)"
if [[ "$cur" != "$FC_TARGET_BYTES" ]]; then
  rpk_exec cluster config set initial_retention_local_target_bytes_default "$FC_TARGET_BYTES" >/dev/null
  echo "$(ts),set,initial_retention_local_target_bytes_default,$FC_TARGET_BYTES" >> "$RESULTS/fast-commission-timeline.csv"
  fcs "B-enabled" "initial_retention_local_target_bytes_default=$FC_TARGET_BYTES"
fi
gate "config status clean" config_status_clean
gate "fast commission enabled" "$ROOT/scripts/check-fast-commission.sh" --config --expect enabled --label enabled
gate "TS check pre-b" "$ROOT/scripts/check-tiered-storage.sh" --label pre-b
"$ROOT/scripts/perf-latency.sh" --background --label fc-enabled
retry_until 900 10 "2 baseline windows" bash -c "source '$ROOT/scripts/lib/common.sh'; source '$ROOT/scripts/lib/ops.sh'; (( \$(windows_done fc-enabled baseline) >= 2 ))"
if ! container_running "$T"; then join_measure enabled yes; fi
[[ "$(cat "$RESULTS/raw/fc-enabled-node-id")" != "$(cat "$RESULTS/raw/fc-baseline-node-id")" ]] || die "tmp-1 reused its Phase A node id"
timings_json enabled
gate "evidence enabled (advantage)" "$ROOT/scripts/check-fast-commission.sh" --evidence "$T" --label enabled --expect-advantage yes --timings "$RESULTS/raw/timings-fc-enabled.json"

# step 12: compare with Phase A
cmp="$(jq -n --slurpfile a "$RESULTS/fast-commission-baseline.json" --slurpfile b "$RESULTS/fast-commission-enabled.json" \
  --slurpfile da "$RESULTS/raw/decom-baseline.json" --argjson thr "$FC_MAX_BYTES_RATIO" '
  ($a[0].summary) as $A | ($b[0].summary) as $B |
  {baseline_ts_bytes_per_replica:$A.ts_local_bytes_per_replica, enabled_ts_bytes_per_replica:$B.ts_local_bytes_per_replica,
   ratio:(if $A.ts_local_bytes_per_replica > 0 then $B.ts_local_bytes_per_replica / $A.ts_local_bytes_per_replica else null end),
   baseline_join_to_settled:$a[0].timings.join_to_settled_sec, enabled_join_to_settled:$b[0].timings.join_to_settled_sec,
   baseline_decom_sec:$da[0].duration_sec, threshold:$thr}
  | . + {passed: (.ratio != null and .ratio < $thr)}')"
echo "$cmp" > "$RESULTS/fast-commission-comparison.json"
log "[fc] Phase A vs B: $(jq -c . <<<"$cmp")"
[[ "$(jq -r .passed <<<"$cmp")" == true ]] || die "fast commission did not reduce local bytes below ${FC_MAX_BYTES_RATIO} of Phase A: $(jq -c . <<<"$cmp")"

# step 13: make tmp-1 leader of one partition per TS topic and read from earliest (served from MinIO)
rpk_exec cluster config set enable_leader_balancer false >/dev/null
echo "$(ts),set,enable_leader_balancer,false (step 13)" >> "$RESULTS/fast-commission-timeline.csv"
tid="$(id_of "$T")"
TPS=()
for t in $(ts_topics); do
  p="$(admin "/v1/cluster/partitions" | jq -r --arg t "$t" --argjson id "$tid" '[.[] | select(.topic==$t and any(.replicas[]; .node_id==$id))][0].partition_id // empty')"
  [[ -n "$p" ]] || { warn "tmp-1 holds no replica of $t"; continue; }
  for attempt in 1 2 3 4 5; do   # transfers can be refused while the replica catches up; verify and retry
    rpk_exec cluster partitions transfer-leadership --partition "$t/$p:$tid" >/dev/null 2>&1 || true
    sleep 2
    admin "/v1/partitions/kafka/$t/$p" | jq -e --argjson id "$tid" '.leader_id==$id' >/dev/null && break
  done
  TPS+=("$t/$p")
done
sleep 5
# compare with Phase A digests where the partition matches; record fresh ones otherwise
known="$(jq -r 'keys[]' "$RESULTS/raw/earliest-digests.json")"
V=(); R=(); for tp in "${TPS[@]}"; do if grep -qx "$tp" <<<"$known"; then V+=("$tp"); else R+=("$tp"); fi; done
leaders_ok="$(for tp in "${TPS[@]}"; do admin "/v1/partitions/kafka/${tp%/*}/${tp##*/}" | jq -r --argjson id "$tid" 'select(.leader_id==$id) | 1'; done | wc -l)"
log "[fc] tmp-1 leads $leaders_ok/${#TPS[@]} chosen partitions"
((${#V[@]})) && py read_check.py verify tmp1-leader "${V[@]}" > "$RESULTS/raw/read-check-tmp1.log"
((${#R[@]})) && py read_check.py record tmp1-leader-new "${R[@]}" >> "$RESULTS/raw/read-check-tmp1.log"
rpk_exec cluster config set enable_leader_balancer true >/dev/null
echo "$(ts),set,enable_leader_balancer,true (restored)" >> "$RESULTS/fast-commission-timeline.csv"
fcs "B-read-from-tmp1" "leads=$leaders_ok/${#TPS[@]}"

# step 14: retire tmp-1
retire_tmp enabled
jq --slurpfile d "$RESULTS/raw/decom-enabled.json" '. + {enabled_decom_sec:$d[0].duration_sec}' "$RESULTS/fast-commission-comparison.json" > "$RESULTS/fast-commission-comparison.json.tmp" \
  && mv "$RESULTS/fast-commission-comparison.json.tmp" "$RESULTS/fast-commission-comparison.json"
"$ROOT/scripts/perf-latency.sh" --stop
fcs done
log "fast commission flow complete: $(jq -c . "$RESULTS/fast-commission-comparison.json")"
