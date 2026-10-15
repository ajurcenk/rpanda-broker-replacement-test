# Cluster state helpers (Admin API based).

health_json() { admin /v1/cluster/health_overview; }
is_healthy() { health_json | jq -e '.is_healthy == true' >/dev/null; }

# Sum of under-replicated replicas over all running brokers (internal metric).
urp_count() {
  local i total=0 n
  for i in $(running_instances); do
    n="$(docker exec "$(cname "$i")" curl -sf -m 10 localhost:9644/metrics 2>/dev/null \
        | awk '/^vectorized_cluster_partition_under_replicated_replicas\{/ {s+=$NF} END {printf "%d", s}')" || n=0
    total=$(( total + ${n:-0} ))
  done
  echo "$total"
}

balancer_json() { admin /v1/cluster/partition_balancer/status; }
reconfig_count() { admin /v1/partitions/reconfigurations | jq 'length'; }
balancer_idle() {
  local b; b="$(balancer_json)" || return 1
  jq -e '(.status == "ready" or .status == "off") and ((.current_reassignments_count // 0) == 0)' <<<"$b" >/dev/null || return 1
  [[ "$(reconfig_count)" == "0" ]]
}
no_maintenance() { brokers_json | jq -e 'all(.[]; (.maintenance_status.draining // false) == false)' >/dev/null; }

# settled: healthy, balancer idle, no URP.
settled() { is_healthy && balancer_idle && [[ "$(urp_count)" == "0" ]]; }
wait_settled() {
  local timeout="${1:-$SETTLE_TIMEOUT_SEC}"
  log "waiting for cluster to settle (healthy, balancer idle, URP=0; timeout ${timeout}s)"
  # require two consecutive settled observations 10s apart, so a balancer tick between moves is not mistaken for idle
  local deadline=$(( $(epoch) + timeout )) ok=0
  while (( ok < 2 )); do
    (( $(epoch) >= deadline )) && die "cluster did not settle within ${timeout}s (health=$(health_json | jq -c '{is_healthy,nodes_down,leaderless_count:(.leaderless_partitions|length)}') balancer=$(balancer_json | jq -c '{status,current_reassignments_count}') urp=$(urp_count))"
    if settled; then ok=$((ok+1)); else ok=0; fi
    sleep 10
  done
}

# Version gate: every broker reports exactly v$RP_VERSION.
assert_versions() {
  local bad
  bad="$(brokers_json | jq -r --arg v "v${RP_VERSION}" '.[] | select(((.version // "") | split(" ")[0]) != $v) | "\(.node_id)=\(.version)"')"
  [[ -z "$bad" ]] || die "broker version mismatch (want v${RP_VERSION}): $bad"
  log "all $(brokers_json | jq length) brokers report v${RP_VERSION}"
}

# Cluster UUID must never change (no second cluster formed).
check_cluster_uuid() {
  local u; u="$(admin /v1/cluster/uuid | jq -r .cluster_uuid)"
  [[ -n "$u" && "$u" != null ]] || die "cannot read cluster uuid"
  if [[ -f "$RESULTS/cluster-uuid" ]]; then
    [[ "$(cat "$RESULTS/cluster-uuid")" == "$u" ]] || die "cluster UUID changed: was $(cat "$RESULTS/cluster-uuid"), now $u (second cluster?)"
  else
    echo "$u" > "$RESULTS/cluster-uuid"
  fi
  echo "$u"
}

# record_node_id <instance>: register the node id of a freshly joined instance.
# Fails if the id was ever used by a different instance (node ids must never be reused).
record_node_id() {
  local inst="$1" nid prev
  nid="$(admin_on "$inst" /v1/node_config | jq -r '.node_id')"
  [[ "$nid" =~ ^[0-9]+$ ]] || die "no node id yet for $inst"
  touch "$RESULTS/node_ids.txt"
  prev="$(awk -v id="$nid" '$1==id {print $2}' "$RESULTS/node_ids.txt" | tail -1)"
  if [[ -n "$prev" ]]; then
    [[ "$prev" == "$inst" ]] || die "node id $nid of $inst was previously used by $prev"
    [[ "${2:-}" == "--expect-new" ]] && die "node id $nid of $inst is not new"
  else
    echo "$nid $inst $(ts)" >> "$RESULTS/node_ids.txt"
  fi
  echo "$nid"
}
id_of() { awk -v i="$1" '$2==i {print $1}' "$RESULTS/node_ids.txt" | tail -1; }

# wait_joined <instance> [--expect-new]: active+alive with expected rack and version; prints node id
wait_joined() {
  local inst="$1" flag="${2:-}" deadline=$(( $(epoch) + 300 )) nid=""
  while :; do
    nid="$(admin_on "$inst" /v1/node_config 2>/dev/null | jq -r '.node_id // empty' 2>/dev/null || true)"
    if [[ -n "$nid" && "$nid" != "-1" ]] && brokers_json | jq -e --argjson id "$nid" \
        'any(.[]; .node_id==$id and .membership_status=="active" and .is_alive==true and ((.version // "") | length > 0))' >/dev/null 2>&1; then break; fi
    (( $(epoch) >= deadline )) && die "$inst did not join as active+alive within 300s (logs: docker logs $(cname "$inst"))"
    sleep 3
  done
  nid="$(record_node_id "$inst" $flag)"
  local rack ver
  rack="$(admin_on "$inst" /v1/node_config | jq -r '.rack // empty')"
  [[ "$rack" == "${INST_RACK[$inst]}" ]] || die "$inst (id $nid) has rack '$rack', expected ${INST_RACK[$inst]}"
  ver="$(brokers_json | jq -r --argjson id "$nid" '.[] | select(.node_id==$id) | (.version // "") | split(" ")[0]')"
  [[ "$ver" == "v${RP_VERSION}" ]] || die "$inst (id $nid) runs $ver, expected v${RP_VERSION}"
  log "$inst joined: node id $nid, rack $rack, $ver, active+alive"
  echo "$nid"
}

# all_alive: every broker in the list is alive and its own admin API is ready.
all_alive() { brokers_json | jq -e 'length > 0 and all(.[]; .is_alive == true)' >/dev/null; }
# wait_back <instance>: after a restart, wait until it serves admin and every broker is alive.
wait_back() {
  local inst="$1"
  retry_until 180 2 "$inst admin API ready" admin_on "$inst" /v1/status/ready
  retry_until 180 2 "all brokers alive after $inst restart" all_alive
  retry_until 300 3 "cluster healthy after $inst restart" is_healthy
}

# seed_block_of <instance>: the seed_servers block of the running redpanda.yaml (rpk-normalised)
seed_block_of() {
  docker exec "$(cname "$1")" cat /etc/redpanda/redpanda.yaml | awk '
    /^ *seed_servers:/ { on=1; ind=match($0,/[^ ]/); print; next }
    on { if (match($0,/[^ ]/) <= ind && $0 ~ /:/) exit; print }'
}
# effective seed list as the broker itself reports it
seed_effective_of() { admin_on "$1" /v1/node_config | jq -c '.seed_servers'; }
urp_zero() { local n; n="$(urp_count)"; [[ "$n" == 0 ]] || { echo "under-replicated replicas: $n"; return 1; }; }
# config_status_clean: no broker needs a restart or reports invalid/unknown properties
config_status_clean() {
  local st; st="$(rpk_exec cluster config status)"
  awk 'NR>1 && ($3=="true" || $4!="[]" || $5!="[]") {bad=1} END {exit bad}' <<<"$st" || { echo "$st"; return 1; }
}
