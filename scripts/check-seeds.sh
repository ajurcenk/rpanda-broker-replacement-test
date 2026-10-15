#!/usr/bin/env bash
# check-seeds.sh: seed and DNS guardrails, run at every gate.
#  1. each seed alias resolves (from inside a broker) to an instance that is active+alive
#  2. the three seed aliases resolve to three different racks
#  3. the seed_servers block of every running broker is byte-identical to the original,
#     and the broker-reported effective seed list matches too
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/cluster.sh"
source "$ROOT/scripts/lib/dns.sh"
b="$(any_broker)" || die "no live broker"
bj="$(brokers_json)"
declare -A ip2inst; for i in "${!INST_IP[@]}"; do ip2inst[${INST_IP[$i]}]=$i; done
racks=()
for a in "${SEED_ALIASES[@]}"; do
  ip="$(docker exec "$(cname "$b")" getent hosts "$a.$ZONE" | awk '{print $1}' | head -1)"
  [[ -n "$ip" ]] || die "seed alias $a.$ZONE does not resolve inside $b"
  inst="${ip2inst[$ip]:-}"
  [[ -n "$inst" ]] || die "seed alias $a.$ZONE resolves to $ip, which is not a known instance"
  nid="$(id_of "$inst")"
  [[ -n "$nid" ]] || die "seed alias $a -> $inst, which never joined the cluster"
  st="$(jq -r --argjson id "$nid" '.[] | select(.node_id==$id) | "\(.membership_status) \(.is_alive)"' <<<"$bj")"
  [[ "$st" == "active true" ]] || die "seed alias $a -> $inst (id $nid) is '${st:-not a member}', must be 'active true'"
  racks+=("${INST_RACK[$inst]}")
  echo "seed $a -> $inst ($ip) id=$nid rack=${INST_RACK[$inst]} active+alive"
done
[[ "$(printf '%s\n' "${racks[@]}" | sort -u | wc -l)" == 3 ]] || die "seed aliases do not span 3 racks: ${racks[*]}"
orig="$RESULTS/seed-block.orig.yaml"; [[ -f "$orig" ]] || die "no recorded original seed block"
for i in $(running_instances); do
  admin_on "$i" /v1/status/ready >/dev/null 2>&1 || continue
  grep -qx "$(id_of "$i")" "$RESULTS/decommissioned.txt" 2>/dev/null && continue   # retired, awaiting removal
  diff -q <(seed_block_of "$i") "$orig" >/dev/null || die "seed_servers block on $i differs from the original: $(seed_block_of "$i" | tr -s ' \n' ' ')"
  [[ "$(seed_effective_of "$i")" == "$(cat "$RESULTS/seed-effective.orig.json")" ]] || die "effective seed list on $i differs: $(seed_effective_of "$i")"
done
echo "seed blocks identical on: $(running_instances | tr '\n' ' ')"
