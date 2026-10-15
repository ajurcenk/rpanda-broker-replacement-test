#!/usr/bin/env bash
# recovery-rate.sh save | raise [bytes_per_sec] | restore | assert-setup
# Temporarily raises raft_learner_recovery_rate and restores the exact original value.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
ORIG="$RESULTS/recovery-rate.orig"
LOGF="$RESULTS/recovery-rate.log"
K=raft_learner_recovery_rate

status_clean() {  # every broker: no restart needed, nothing invalid/unknown
  local st; st="$(rpk_exec cluster config status)"
  awk 'NR>1 && ($3=="true" || $4!="[]" || $5!="[]") {bad=1} END {exit bad}' <<<"$st" || { echo "$st" >&2; return 1; }
}
# value as seen through each running broker's own admin API
all_brokers_report() {
  local want="$1" i v
  for i in $(running_instances); do
    v="$(admin_on "$i" "/v1/cluster_config?key=$K" | jq -r ".$K")" || return 1
    [[ "$v" == "$want" ]] || { echo "$i reports $K=$v, want $want" >&2; return 1; }
  done
}
save() {
  local cur; cur="$(cfg_get "$K")"
  if [[ -f "$ORIG" ]]; then
    [[ "$(cat "$ORIG")" == "$cur" ]] || die "$ORIG holds $(cat "$ORIG") but current value is $cur: an earlier run did not restore it"
  else
    echo "$cur" > "$ORIG"
  fi
  echo "$(ts),save,$cur" >> "$LOGF"
}
case "${1:?save|raise|restore|assert-setup}" in
  save) save; log "saved $K=$(cat "$ORIG")" ;;
  raise)
    v="${2:-$RAISED_RECOVERY_RATE}"
    [[ -f "$ORIG" ]] || save
    rpk_exec cluster config set "$K" "$v" >/dev/null
    [[ "$(cfg_get "$K")" == "$v" ]] || die "$K did not take value $v"
    retry_until 30 2 "all brokers report $K=$v" all_brokers_report "$v"
    status_clean || die "config status not clean after raising $K"
    echo "$(ts),raise,$(cat "$ORIG"),$v" >> "$LOGF"
    log "raised $K to $v (original $(cat "$ORIG"))"
    ;;
  restore)
    [[ -f "$ORIG" ]] || { log "no saved $K, nothing to restore"; exit 0; }
    o="$(cat "$ORIG")"
    rpk_exec cluster config set "$K" "$o" >/dev/null
    [[ "$(cfg_get "$K")" == "$o" ]] || die "$K restore to $o failed"
    retry_until 30 2 "all brokers report $K=$o" all_brokers_report "$o"
    status_clean || die "config status not clean after restoring $K"
    echo "$(ts),restore,$o,${2:-}" >> "$LOGF"
    rm -f "$ORIG"
    log "restored $K to $o"
    ;;
  assert-setup)
    s="$(cat "$RESULTS/recovery-rate.setup")"; c="$(cfg_get "$K")"
    [[ "$c" == "$s" ]] || die "$K is $c, setup value was $s"
    all_brokers_report "$s" || die "not every broker reports the setup value $s"
    log "$K equals the setup value $s on all brokers"
    ;;
  *) die "unknown mode $1" ;;
esac
