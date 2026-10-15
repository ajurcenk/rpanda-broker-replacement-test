#!/usr/bin/env bash
# swap.sh <old-instance> <new-instance> <rack> <alias> --n <swap#> [--raise-recovery-rate [bytes]]
#         [--maintenance] [--soak <sec>] [--after-repoint <cmd>] [--resume-from 8]
# --resume-from 8 continues an interrupted swap whose decommission already completed (steps 8-10).
# Replace one broker: add the new broker, let it settle, decommission the old one, then repoint
# the DNS alias, retire the old instance and soak. Every step has a gate; timestamps go to
# results/swaps.csv and the current phase to results/current-phase (used by perf rows).
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"
source "$ROOT/scripts/lib/dns.sh"
source "$ROOT/scripts/lib/ops.sh"

old="${1:?old}"; new="${2:?new}"; rack="${3:?rack}"; alias="${4:?alias}"; shift 4
n=""; raise=""; maint=0; soak="$SOAK_SEC"; after_repoint=""; resume=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --n) n="$2"; shift 2 ;;
    --raise-recovery-rate) if [[ "${2:-}" =~ ^[0-9]+$ ]]; then raise="$2"; shift 2; else raise="$RAISED_RECOVERY_RATE"; shift; fi ;;
    --maintenance) maint=1; shift ;;
    --soak) soak="$2"; shift 2 ;;
    --after-repoint) after_repoint="$2"; shift 2 ;;
    --resume-from) resume="$2"; shift 2 ;;
    *) die "bad arg $1" ;;
  esac
done
[[ -n "$n" ]] || die "--n <swap number> required"
S="swap-$n"; GATE_CTX="$S"
[[ "${INST_RACK[$old]}" == "$rack" && "${INST_RACK[$new]}" == "$rack" ]] || die "rack mismatch: $old=${INST_RACK[$old]} $new=${INST_RACK[$new]} want $rack"
[[ "$(alias_target "$alias")" == "$old" ]] || die "alias $alias points at $(alias_target "$alias"), expected $old"
[[ "$resume" == 0 || "$resume" == 8 ]] || die "--resume-from supports only 8"
mkdir -p "$RESULTS/raw"
step() { swaps_csv "$S,$old,$new,$1,${2:-}"; log "[$S] $1 ${2:-}"; }

raised=0; watcher=""
cleanup() {
  local rc=$?
  [[ -n "$watcher" ]] && kill "$watcher" 2>/dev/null || true
  if (( raised )) && [[ -f "$RESULTS/recovery-rate.orig" ]]; then
    warn "[$S] exit with recovery rate raised: restoring"
    "$ROOT/scripts/recovery-rate.sh" restore "trap-exit-rc-$rc" || warn "RESTORE FAILED: run scripts/recovery-rate.sh restore"
  fi
  "$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true
  (( rc == 0 )) || step "aborted" "rc=$rc"
}
trap cleanup EXIT

if (( resume == 8 )); then
  # Resume after a completed decommission: reload state recorded by the interrupted run.
  step_ts() { grep -E "^[^,]*,$S,$old,$new,$1," "$RESULTS/swaps.csv" | tail -1 | cut -d, -f1; }
  for st in 2-provision 3-joined 5-settled 7-decommissioned; do [[ -n "$(step_ts "$st")" ]] || die "cannot resume: step $st not recorded for $S"; done
  t_start=$(date -d "$(step_ts 2-provision)" +%s); t_joined=$(date -d "$(step_ts 3-joined)" +%s)
  t_settled=$(date -d "$(step_ts 5-settled)" +%s); t_decom=$(date -d "$(step_ts 7-decommissioned)" +%s)
  old_id="$(id_of "$old")"; new_id="$(id_of "$new")"
  brokers_json | jq -e --argjson id "$old_id" 'any(.[]; .node_id==$id) | not' >/dev/null || die "cannot resume: old node $old_id is still a member"
  baseline="$(cat "$RESULTS/raw/baseline-$S.json")"
  step "resume-from-8" "old=$old_id new=$new_id"
  "$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true
  "$ROOT/scripts/perf-latency.sh" --background --label "$S"
  dphase() { echo "$1"; }
else
# ---------- 1. pre-checks ----------
phase baseline; step "1-prechecks-start"
full_gate "$S-pre"
gate "fast commission enabled" "$ROOT/scripts/check-fast-commission.sh" --config --expect enabled --label "$S-pre"
gate "tiered storage" "$ROOT/scripts/check-tiered-storage.sh" --label "$S-pre"
gate "recovery rate at original" "$ROOT/scripts/recovery-rate.sh" assert-setup
[[ ! -f "$RESULTS/recovery-rate.orig" ]] || die "recovery-rate.orig exists: an earlier swap did not restore"
"$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true
"$ROOT/scripts/perf-latency.sh" --background --label "$S"
log "[$S] waiting for 2 complete baseline perf windows"
retry_until 900 10 "2 baseline perf windows" bash -c "source '$ROOT/scripts/lib/common.sh'; source '$ROOT/scripts/lib/ops.sh'; (( \$(windows_done '$S' baseline) >= 2 ))"
baseline="$(baseline_from_csv "$S" baseline)"
prod_base="$(awk -F, 'NR>1 && $1>=s {print $6}' s="$(sed -n "/$S,$old,$new,1-prechecks-start/p" "$RESULTS/swaps.csv" | tail -1 | cut -d, -f1)" "$RESULTS/producer-stats.csv" 2>/dev/null | sort -n | awk '{a[NR]=$1} END {if (NR) print a[int((NR+1)/2)]; else print ""}')"
baseline="$(jq --arg tp "${prod_base:-}" '. + {test_producer_p99: (if $tp == "" then null else ($tp|tonumber) end)}' <<<"$baseline")"
echo "$baseline" > "$RESULTS/raw/baseline-$S.json"
step "1-prechecks-done" "$(jq -c . <<<"$baseline" | tr ',' ';')"

# ---------- 1b. raise recovery rate (optional) ----------
if [[ -n "$raise" ]]; then
  "$ROOT/scripts/recovery-rate.sh" raise "$raise"; raised=1
  step "1b-recovery-rate-raised" "$raise"
  # Automatic rollback watcher: 3 consecutive samples of p99 > ROLLBACK_RATIO x baseline -> restore.
  (
    trap - ERR; set +e
    bp="$(jq -r '.produce_p99 // empty' <<<"$baseline")"; btp="$(jq -r '.test_producer_p99 // empty' <<<"$baseline")"
    seen=""; bad=0
    while [[ -f "$RESULTS/recovery-rate.orig" ]]; do
      sleep 10
      hit=0; why=""
      last="$(awk -F, -v l="$S" 'NR>1 && $3==l && $6=="produce" && $NF=="ok" {w=$2; p=$13} END {print w" "p}' "$RESULTS/latency.csv")"
      w="${last% *}"; p="${last#* }"
      if [[ -n "$bp" && -n "$p" && "$w" != "$seen" ]]; then
        seen="$w"; awk -v p="$p" -v b="$bp" -v r="$ROLLBACK_RATIO" 'BEGIN {exit !(p > r*b)}' && { hit=1; why="perf p99 $p > ${ROLLBACK_RATIO}x $bp"; }
      fi
      tp="$(tail -1 "$RESULTS/producer-stats.csv" 2>/dev/null | cut -d, -f6)"
      if [[ -n "$btp" && -n "$tp" ]] && awk -v p="$tp" -v b="$btp" -v r="$ROLLBACK_RATIO" 'BEGIN {exit !(p > r*b)}'; then
        hit=1; why="${why:+$why; }test producer p99 $tp > ${ROLLBACK_RATIO}x $btp"
      fi
      if (( hit )); then bad=$((bad+1)); else bad=0; fi
      echo "$(ts),$S,rollback-sample,$bad,${why:-ok}" >> "$RESULTS/recovery-rate.log"
      if (( bad >= ROLLBACK_SAMPLES )); then
        echo "$(ts),$S,auto-rollback,$why" >> "$RESULTS/recovery-rate.log"
        echo "$why" > "$RESULTS/raw/rollback-$S.txt"
        "$ROOT/scripts/recovery-rate.sh" restore "auto-rollback-$S"
        exit 0
      fi
    done
  ) &
  watcher=$!
fi
dphase() { if (( raised )) && [[ -f "$RESULTS/recovery-rate.orig" ]]; then echo "$1-raised"; else echo "$1"; fi; }

# ---------- 2. provision ----------
phase join; step "2-provision"
docker volume inspect "$(vname "$new")" >/dev/null 2>&1 && die "volume for $new already exists; new brokers must start empty"
"$ROOT/scripts/broker.sh" start "$new"

# ---------- 3. join and confirm ----------
t_start=$(epoch)
new_id="$(wait_joined "$new" --expect-new)"
assert_versions
t_joined=$(epoch)
step "3-joined" "node_id=$new_id join_sec=$((t_joined - t_start))"

# ---------- 4. seed lists: unchanged by design ----------
gate "seeds after join" "$ROOT/scripts/check-seeds.sh"
step "4-seeds-ok"

# ---------- 5. settle ----------
phase "$(dphase settle)"; step "5-settle-start"
wait_settled_sampling "$new" "$RESULTS/raw/settle-$S.json"
t_settled=$(epoch)
step "5-settled" "settle_sec=$((t_settled - t_joined)) join_to_settled_sec=$((t_settled - t_start))"
GATE_CTX="$S-settled"; gate "placement after settle" "$ROOT/scripts/audit-placement.sh" --label "$S-settled"

# ---------- 6. optional drain ----------
old_id="$(id_of "$old")"; [[ -n "$old_id" ]] || die "no node id for $old"
phase "$(dphase decommission)"
if (( maint )); then
  step "6-maintenance-start" "node=$old_id"
  rpk_exec cluster maintenance enable "$old_id" --wait
  step "6-maintenance-done"
fi

# ---------- 7. decommission ----------
step "7-decommission-start" "node=$old_id"
decommission_wait "$old_id" "$RESULTS/raw/decom-$S.json"
t_decom=$(epoch)
step "7-decommissioned" "$(jq -c . "$RESULTS/raw/decom-$S.json" | tr ',' ';')"

fi

# ---------- 8. verify ----------
phase "$(dphase post-verify)"; step "8-verify"
brokers_json | jq -e --argjson id "$old_id" 'any(.[]; .node_id==$id) | not' >/dev/null || die "old node $old_id still listed"
wait_settled 900
GATE_CTX="$S-post"
gate "healthy" is_healthy
gate "URP=0" urp_zero
gate "placement audit" "$ROOT/scripts/audit-placement.sh" --label "$S-post"
jq -n --argjson j "$((t_joined - t_start))" --argjson s "$((t_settled - t_joined))" --argjson js "$((t_settled - t_start))" \
      --argjson d "$((t_decom - t_settled))" --slurpfile dj "$RESULTS/raw/decom-$S.json" --slurpfile sj "$RESULTS/raw/settle-$S.json" \
      '{join_sec:$j, settle_sec:$s, join_to_settled_sec:$js, decommission_sec:$d, decommission:$dj[0], settle:$sj[0]}' > "$RESULTS/raw/timings-$S.json"
gate "fast commission evidence" "$ROOT/scripts/check-fast-commission.sh" --evidence "$new" --label "$S" --expect-advantage yes --timings "$RESULTS/raw/timings-$S.json"

# ---------- 8b. restore recovery rate ----------
if (( raised )); then
  if [[ -f "$RESULTS/recovery-rate.orig" ]]; then
    "$ROOT/scripts/recovery-rate.sh" restore "$S-step-8b"
    step "8b-recovery-rate-restored"
  else
    step "8b-recovery-rate-already-restored" "auto-rollback: $(cat "$RESULTS/raw/rollback-$S.txt" 2>/dev/null || echo unknown)"
  fi
  [[ -n "$watcher" ]] && { kill "$watcher" 2>/dev/null || true; watcher=""; }
  raised=0
  gate "recovery rate restored" "$ROOT/scripts/recovery-rate.sh" assert-setup
fi

# ---------- 9. DNS repoint (only now: decommission completed and verify passed) ----------
step "9-repoint-start" "$alias -> $new"
"$ROOT/scripts/dns-repoint.sh" "$alias" "$new"
# scenario 8 hook: must start within one TTL of the repoint, so it runs before the TTL wait
[[ -n "$after_repoint" ]] && { step "9-after-repoint-hook" "$after_repoint"; bash -c "$after_repoint"; }
sleep $((DNS_TTL + 1))
want="${INST_IP[$new]}"
got_b="$(docker exec "$(cname "$new")" getent hosts "$alias.$ZONE" | awk '{print $1}')"
got_c="$(tools dig +short "$alias.$ZONE" | head -1)"
[[ "$got_b" == "$want" ]] || die "$alias.$ZONE resolves to '$got_b' inside broker $new, want $want"
[[ "$got_c" == "$want" ]] || die "$alias.$ZONE resolves to '$got_c' inside a client container, want $want"
gate "seeds after repoint" "$ROOT/scripts/check-seeds.sh"
step "9-repointed" "broker=$got_b client=$got_c"

# ---------- 10. retire and soak ----------
"$ROOT/scripts/broker.sh" rm "$old"
step "10-retired" "$old container and volume removed"
"$ROOT/scripts/clients.sh" restart
phase soak; step "10-soak-start" "${soak}s"
end=$(( $(epoch) + soak ))
while (( $(epoch) < end )); do
  is_healthy || die "cluster unhealthy during soak"
  urp_zero >/dev/null || die "under-replicated partitions during soak: $(urp_count)"
  sleep 15
done
# make sure at least one full soak window exists
retry_until 300 10 "a complete soak perf window" bash -c "source '$ROOT/scripts/lib/common.sh'; source '$ROOT/scripts/lib/ops.sh'; (( \$(windows_done '$S' soak) >= 1 ))"
"$ROOT/scripts/perf-latency.sh" --stop
soakb="$(baseline_from_csv "$S" soak)"
rec="$(jq -n --argjson b "$baseline" --argjson s "$soakb" --argjson r "$SOAK_RECOVERY_RATIO" \
  '{baseline:$b, soak:$s, produce_ratio:(if $b.produce_p99 and $s.produce_p99 then ($s.produce_p99/$b.produce_p99) else null end),
    e2e_ratio:(if $b.e2e_p99 and $s.e2e_p99 then ($s.e2e_p99/$b.e2e_p99) else null end)}
   | . + {recovered: ((.produce_ratio // 0) <= $r and (.e2e_ratio // 0) <= $r)}')"
echo "$rec" > "$RESULTS/raw/soak-$S.json"
if [[ "$(jq -r .recovered <<<"$rec")" == true ]]; then step "10-soak-done" "latency recovered"; else
  warn "[$S] latency did not recover: $(jq -c '{produce_ratio,e2e_ratio}' <<<"$rec")"; step "10-soak-done" "latency did not recover"; fi
full_gate "$S-done"
step "done" "$old -> $new (node $old_id -> $new_id)"
