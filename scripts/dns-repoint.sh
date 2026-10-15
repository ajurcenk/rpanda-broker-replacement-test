#!/usr/bin/env bash
# dns-repoint.sh <alias> <instance>
#   Repoint a client/seed alias to a new instance. Refuses unless the target is an active,
#   alive broker and the instance currently behind the alias is no longer a cluster member
#   (its decommission completed) or is the same instance.
# dns-repoint.sh <alias> --test-ip <ip> <reason>
#   Test-only override (scenario 7): point an alias at a raw IP. Logged as such.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/dns.sh"
a="${1:?usage: dns-repoint.sh <alias> <instance>|--test-ip <ip> <reason>}"
grep -qE "^[0-9.]+ $a\.${ZONE} " "$HOSTS_FILE" || die "unknown alias $a"
old="$(alias_target "$a")"
if [[ "${2:-}" == "--test-ip" ]]; then
  ip="${3:?ip}"; reason="${4:?reason}"
  set_alias "$a" "$ip" "ip:$ip"
  label="ip:$ip (TEST OVERRIDE: $reason)"
else
  new="${2:?instance}"
  [[ -n "${INST_IP[$new]:-}" ]] || die "unknown instance $new"
  ip="${INST_IP[$new]}"
  # Target must be an active, alive member.
  nid="$(awk -v i="$new" '$2==i {print $1}' "$RESULTS/node_ids.txt" | tail -1)"
  [[ -n "$nid" ]] || die "no node id recorded for $new"
  st="$(brokers_json | jq -r --argjson id "$nid" '.[] | select(.node_id==$id) | "\(.membership_status) \(.is_alive)"')"
  [[ "$st" == "active true" ]] || die "refusing repoint: target $new (id $nid) is '$st', not 'active true'"
  # Previous target must be fully decommissioned (not in the broker list), unless it is a raw IP.
  if [[ "$old" != "$new" && "$old" != ip:* ]]; then
    oid="$(awk -v i="$old" '$2==i {print $1}' "$RESULTS/node_ids.txt" | tail -1)"
    if [[ -n "$oid" ]] && brokers_json | jq -e --argjson id "$oid" 'any(.[]; .node_id==$id)' >/dev/null; then
      die "refusing repoint: $a still points at $old (id $oid), which is still a cluster member; decommission must complete first"
    fi
  fi
  set_alias "$a" "$ip" "$new"
  label="$new"
fi
t0=$(epoch)
retry_until 30 1 "CoreDNS to serve $a -> $ip" bash -c "[[ \"\$(dig +short +time=1 +tries=1 @$DNS_IP $a.$ZONE A | head -1)\" == '$ip' ]]"
echo "$(ts),$a,$old,$label,$ip,served_after_$(( $(epoch) - t0 ))s" >> "$RESULTS/dns.log"
log "repointed $a.$ZONE: $old -> $label ($ip)"
