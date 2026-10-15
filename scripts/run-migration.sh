#!/usr/bin/env bash
# run-migration.sh: the full run, in the order of the test spec. Re-runnable: completed stages
# (seeded data, fast commission flow, finished swaps) are detected and skipped.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"
source "$ROOT/scripts/lib/dns.sh"
source "$ROOT/scripts/lib/ops.sh"
S="$ROOT/scripts"

# Swap plan: old new rack alias [extra flags]. Recovery rate raised for swaps 2 and 5.
SWAPS=(
  "io-b2 nv-n2 eu-central-1a b2"
  "io-b4 nv-n4 eu-central-1b b4 --raise-recovery-rate"
  "io-b1 nv-n1 eu-central-1a b1 --after-repoint scripts/scenarios/08-join-after-repoint.sh"
  "io-b3 nv-n3 eu-central-1b b3"
  "io-b5 nv-n5 eu-central-1c b5 --raise-recovery-rate"
)
EXTRA_SWAP_ARGS="${EXTRA_SWAP_ARGS:-}"     # e.g. "--maintenance"

log "=== 1. up + configure (fast commission left disabled) ==="
"$S/up.sh"
"$S/configure-cluster.sh"

log "=== 2. seed data + Tiered Storage check ==="
"$S/seed-data.sh"
[[ -f "$RESULTS/tiered-storage-check-post-seed.json" ]] && jq -e .passed "$RESULTS/tiered-storage-check-post-seed.json" >/dev/null \
  || "$S/check-tiered-storage.sh" --label post-seed

log "=== 3. fast commission flow ==="
if ! jq -e '.passed and (.enabled_decom_sec != null)' "$RESULTS/fast-commission-comparison.json" >/dev/null 2>&1; then
  "$S/run-fast-commission-flow.sh"
fi

log "=== 4. test clients + cluster baseline ==="
"$S/clients.sh" start
if [[ ! -f "$RESULTS/raw/group-offset-pre-swaps" ]]; then
  # let the verifier catch up from the earliest offset (served from MinIO), then record its committed offset
  retry_until 900 10 "verifier consumer caught up" bash -c "
    st=\$(cat '$RESULTS/consumer.state' 2>/dev/null) || exit 1
    off=\$(jq .offset <<<\"\$st\"); hw=\$(docker exec rp-\$(source '$ROOT/scripts/lib/common.sh'; any_broker) rpk topic describe '$SEQ_TOPIC' -p | awk 'NR==2 {print \$NF}')
    (( hw - off < 2000 ))"
  rpk_exec group describe verifier > "$RESULTS/raw/group-verifier-pre-swaps.txt"
  awk -v t="$SEQ_TOPIC" '$1==t {print $3}' "$RESULTS/raw/group-verifier-pre-swaps.txt" | head -1 > "$RESULTS/raw/group-offset-pre-swaps"
  [[ -s "$RESULTS/raw/group-offset-pre-swaps" ]] || die "could not read verifier committed offset"
  log "verifier committed offset before swaps: $(cat "$RESULTS/raw/group-offset-pre-swaps")"
fi
grep -q ',cluster-baseline,' "$RESULTS/latency.csv" 2>/dev/null || "$S/perf-latency.sh" --label cluster-baseline --phase baseline

log "=== 5. swaps ==="
n=0
for sw in "${SWAPS[@]}"; do
  n=$((n+1))
  read -r old new rack alias extra <<<"$sw"
  extra="${extra//scripts\//$S/}"
  if grep -q "^[^,]*,swap-$n,$old,$new,done," "$RESULTS/swaps.csv" 2>/dev/null; then log "swap $n ($old -> $new) already done"; continue; fi
  if grep -q "^[^,]*,swap-$n,$old,$new,7-decommissioned," "$RESULTS/swaps.csv" 2>/dev/null; then
    log "swap $n was interrupted after its decommission completed: resuming from step 8"
    extra="$(sed -E 's/--raise-recovery-rate//' <<<"${extra:-}") --resume-from 8"
  fi
  log "--- swap $n: $old -> $new ($rack, alias $alias) ${extra:-} ---"
  # shellcheck disable=SC2086
  "$S/swap.sh" "$old" "$new" "$rack" "$alias" --n "$n" ${extra:-} $EXTRA_SWAP_ARGS
done

log "=== 6. final check (runbook step 24) ==="
GATE_CTX=final
want="$(printf '%s\n' nv-n1 nv-n2 nv-n3 nv-n4 nv-n5)"
have="$(for id in $(brokers_json | jq -r '.[].node_id'); do awk -v i="$id" '$1==i {print $2}' "$RESULTS/node_ids.txt"; done | sort)"
[[ "$have" == "$want" ]] || die "brokers are not nv-n1..nv-n5: $(echo $have)"
for a in "${ALIASES[@]}"; do
  t="$(alias_target "$a")"; [[ "$t" == "nv-n${a#b}" ]] || die "alias $a points at $t"
  [[ "$(dig_alias "$a")" == "${INST_IP[nv-n${a#b}]}" ]] || die "alias $a does not resolve to nv-n${a#b}"
done
full_gate final
gate "recovery rate = setup value" "$S/recovery-rate.sh" assert-setup
[[ ! -f "$RESULTS/recovery-rate.orig" ]] || die "recovery-rate.orig still present"

log "=== 7. restore temporary settings (runbook step 25) ==="
orig_moves="$(cat "$RESULTS/raw/orig-concurrent-moves")"
rpk_exec cluster config set partition_autobalancing_concurrent_moves "$orig_moves" >/dev/null
log "partition_autobalancing_concurrent_moves restored to $orig_moves (no leader pinning was configured)"
echo "$(ts),restore,partition_autobalancing_concurrent_moves,$orig_moves" >> "$RESULTS/settings-timeline.csv"

log "=== 8. data verification ==="
"$S/clients.sh" stop
"$S/verify-data.sh"
log "=== migration complete ==="
