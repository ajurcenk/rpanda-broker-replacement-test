# DNS zone helpers. The hosts file is the single source of truth for alias targets.
HOSTS_FILE="$ROOT/coredns/rp.test.hosts"

# write_hosts_initial: instance A records, aliases at their initial (old) instances,
# minio, and dead* names (unreachable IP) used by the guardrail scenario.
write_hosts_initial() {
  local tmp="$HOSTS_FILE.tmp" i a
  {
    echo "# rp.test zone. Managed by scripts/dns-repoint.sh. Format: IP NAME  # role"
    echo "${MINIO_IP} minio.${ZONE}"
    for i in $(printf '%s\n' "${!INST_IP[@]}" | sort); do echo "${INST_IP[$i]} ${i}.${ZONE}  # instance"; done
    for a in "${ALIASES[@]}"; do echo "${INST_IP[${ALIAS_INITIAL[$a]}]} ${a}.${ZONE}  # alias -> ${ALIAS_INITIAL[$a]}"; done
    for a in dead1 dead2 dead3; do echo "${UNUSED_IP} ${a}.${ZONE}  # unreachable"; done
  } > "$tmp"
  chmod 644 "$tmp"; mv "$tmp" "$HOSTS_FILE"
}

# alias_target <alias>: instance name currently behind the alias (or "ip:<addr>" if not an instance)
alias_target() {
  local line; line="$(grep -E "^[0-9.]+ $1\.${ZONE} " "$HOSTS_FILE")" || return 1
  local t; t="$(sed -nE 's/.*# alias -> (.*)$/\1/p' <<<"$line")"
  echo "$t"
}
alias_ip() { awk -v n="$1.${ZONE}" '$2==n {print $1}' "$HOSTS_FILE"; }

# set_alias <alias> <ip> <label>: atomic rewrite of one alias line
set_alias() {
  local a="$1" ip="$2" label="$3" tmp="$HOSTS_FILE.tmp"
  awk -v n="$a.${ZONE}" -v ip="$ip" -v lbl="$label" '
    $2==n { print ip " " n "  # alias -> " lbl; next } { print }' "$HOSTS_FILE" > "$tmp"
  chmod 644 "$tmp"; mv "$tmp" "$HOSTS_FILE"
}

# dig_alias <alias>: what CoreDNS answers right now
dig_alias() { dig +short +time=1 +tries=1 @"$DNS_IP" "$1.${ZONE}" A 2>/dev/null | head -1; }

# ensure_instance_record <instance>: add the instance A record if the zone lacks it
ensure_instance_record() {
  local i="$1"
  grep -qE "^[0-9.]+ ${i}\.${ZONE} " "$HOSTS_FILE" && return 0
  local tmp="$HOSTS_FILE.tmp"
  { cat "$HOSTS_FILE"; echo "${INST_IP[$i]} ${i}.${ZONE}  # instance"; } > "$tmp"
  chmod 644 "$tmp"; mv "$tmp" "$HOSTS_FILE"
  retry_until 20 1 "CoreDNS serving $i" bash -c "dig +short +time=1 +tries=1 @$DNS_IP $i.$ZONE | grep -q ."
}
