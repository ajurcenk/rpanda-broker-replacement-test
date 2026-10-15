#!/usr/bin/env bash
# seed-data.sh: create topics and preload them so that Tiered Storage topics hold data that
# exists only in MinIO (local start offset > 0), before any broker is replaced.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"

TS=(-c redpanda.remote.write=true -c redpanda.remote.read=true
    -c segment.bytes="$SEGMENT_BYTES" -c retention.local.target.bytes="$LOCAL_TARGET_BYTES"
    -c retention.bytes=-1 -c retention.ms=-1 -c cleanup.policy=delete -c write.caching=false)
NOTS=(-c redpanda.remote.write=false -c redpanda.remote.read=false
      -c segment.bytes="$SEGMENT_BYTES" -c retention.bytes=-1 -c retention.ms=-1
      -c cleanup.policy=delete -c write.caching=false)

existing="$(rpk_exec topic list | awk 'NR>1 {print $1}')"
mk() {  # mk <topic> <partitions> <config...>
  local t="$1" p="$2"; shift 2
  if grep -qx "$t" <<<"$existing"; then log "topic $t exists"; return; fi
  rpk_exec topic create "$t" -p "$p" -r 3 "$@" >/dev/null
  log "created $t (p=$p rf=3)"
}
BG=(); for i in $(seq 0 $((BG_TOPICS-1))); do BG+=("$(printf 'bg.%02d' "$i")"); done
for t in "${BG[@]}"; do mk "$t" "$BG_PARTITIONS" "${TS[@]}"; done
mk "$SEQ_TOPIC" 1 "${TS[@]}"
mk "$PERF_TOPIC" 6 "${TS[@]}" -c min.insync.replicas=2
mk "$E2E_TOPIC" 1 "${TS[@]}" -c min.insync.replicas=2
mk "$CONTROL_TOPIC" 6 "${NOTS[@]}"

# Preload (skipped per topic if it already holds the target volume).
need_load() {  # topic -> true if its partition 0 holds less than 90% of the target
  local hw; hw="$(rpk_exec topic describe "$1" -p | awk 'NR==2 {print $NF}')"
  (( hw * 1024 < PRELOAD_BYTES_PER_PARTITION * 9 / 10 ))
}
todo=(); for t in "${BG[@]}" "$CONTROL_TOPIC" "$PERF_TOPIC" "$E2E_TOPIC"; do need_load "$t" && todo+=("$t"); done
if ((${#todo[@]})); then
  log "preloading ${#todo[@]} topics with $((PRELOAD_BYTES_PER_PARTITION/MiB)) MiB per partition (4 parallel loaders)"
  pids=()
  for k in 0 1 2 3; do
    chunk=(); for j in "${!todo[@]}"; do (( j % 4 == k )) && chunk+=("${todo[$j]}"); done
    ((${#chunk[@]})) || continue
    py preload.py "$PRELOAD_BYTES_PER_PARTITION" "${chunk[@]}" > "$RESULTS/preload-$k.log" 2>&1 & pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || die "preload failed, see results/preload-*.log"; done
fi
# Sequencer topic: preloaded with real sequence numbers (they enter acked.log like any other).
if need_load "$SEQ_TOPIC"; then
  log "preloading $SEQ_TOPIC with sequenced records"
  tools python3 /work/client/producer.py --preload "$PRELOAD_BYTES_PER_PARTITION" > "$RESULTS/preload-seq.log" 2>&1 \
    || die "sequencer preload failed: $(tail -3 "$RESULTS/preload-seq.log")"
fi

# Wait for uploads and local trimming: every TS replica's local start offset > 0.
log "waiting for upload + local trim (upload interval ${UPLOAD_INTERVAL_SEC}s, trim interval 30s)"
trimmed() {
  local i
  for i in $(running_instances); do
    docker exec "$(cname "$i")" curl -sf localhost:9644/metrics | awk -v ctl="$CONTROL_TOPIC" '
      /^vectorized_cluster_partition_start_offset\{namespace="kafka"/ {
        match($0,/topic="[^"]+"/); t=substr($0,RSTART+7,RLENGTH-8)
        if (t==ctl || t ~ /^_/) next
        if ($NF+0 == 0) { bad=1 } }
      END { exit bad }' || return 1
  done
}
retry_until 900 15 "every Tiered Storage replica trimmed locally (start offset > 0)" trimmed
log "seed-data done"
