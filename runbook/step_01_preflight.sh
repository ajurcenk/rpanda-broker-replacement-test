#!/usr/bin/env bash
# =============================================================================
# step_01_preflight.sh - Phase 0 checks (runbook steps 1-5). READ-ONLY.
# =============================================================================
# Confirms the cluster is in the state the runbook assumes before anything changes:
#   * $EXPECTED_BROKER_COUNT brokers, all on $EXPECTED_VERSION, active, alive, none in maintenance
#   * every broker has rack = an AZ name, and the layout covers all racks
#   * every broker: empty_seed_starts_cluster=false, seed_servers = the seed DNS names (Path A)
#   * seed DNS names: one per AZ, resolve to live brokers, TTL <= $MAX_DNS_TTL
#   * client DNS names (RPK_BROKERS) all resolve
#   * cluster settings: rack awareness on, continuous balancing, Tiered Storage on,
#     write caching off by default
#   * one replica per rack for every partition; balancer idle
#   * important topics have Tiered Storage enabled and no upload backlog
#   * the enterprise licence is valid
#
# Also records the list of node IDs in use today (state/node_ids_seen.txt), so later
# steps can prove that new brokers get NEW node IDs.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"

header "Brokers"
bj="$(brokers_json)"
jq -r '.[] | "  node \(.node_id)  \(.internal_rpc_address)  rack=\(.rack // "NONE")  \(.membership_status)  alive=\(.is_alive)  \((.version // "?") | split(" ")[0])"' <<<"$bj"
check "broker count is $EXPECTED_BROKER_COUNT" [ "$(jq length <<<"$bj")" -eq "$EXPECTED_BROKER_COUNT" ]
check "all brokers active and alive" jq -e 'all(.[]; .membership_status=="active" and .is_alive)' <<<"$bj"
check "versions" all_versions_ok
check "no broker in maintenance mode" no_maintenance
for r in "${RACKS[@]}"; do
  n="$(jq --arg r "$r" '[.[] | select(.rack==$r)] | length' <<<"$bj")"
  check "rack $r has $n broker(s)" [ "$n" -ge 1 ]
done
check "no broker without a rack" jq -e 'all(.[]; (.rack // "") != "")' <<<"$bj"
# Remember every node ID in use now: new brokers must never reuse one.
jq -r '.[].node_id' <<<"$bj" >> "$RB_STATE_DIR/node_ids_seen.txt"
sort -un -o "$RB_STATE_DIR/node_ids_seen.txt" "$RB_STATE_DIR/node_ids_seen.txt"

header "Node configuration (per broker)"
while read -r id host; do
  cfg="$(admin_on "$host" /v1/node_config)" || { bad "cannot read node_config from node $id ($host:$ADMIN_PORT)"; continue; }
  check "node $id empty_seed_starts_cluster=false" jq -e '.empty_seed_starts_cluster == false' <<<"$cfg"
  check "node $id rack set ($(jq -r .rack <<<"$cfg"))" jq -e '(.rack // "") != ""' <<<"$cfg"
done < <(jq -r '.[] | "\(.node_id) \(.internal_rpc_address)"' <<<"$bj")

header "Seeds and DNS (Path A)"
check "seed rules (one per AZ, live, identical on all brokers)" seeds_check
for name in "${SEED_DNS_NAMES[@]}"; do
  ttl="$(dns_ttl "$name")"
  check "TTL of $name is ${ttl:-?}s (max $MAX_DNS_TTL)" [ "${ttl:-999999}" -le "$MAX_DNS_TTL" ]
done
for hp in ${RPK_BROKERS//,/ }; do
  h="${hp%:*}"
  ip="$(resolve "$h")"
  check "client bootstrap name $h resolves (${ip:-nothing})" [ -n "$ip" ]
done

header "Cluster settings"
expect_cfg() {   # expect_cfg <property> <expected value> [warn]
  local v; v="$(cfg_get "$1")"
  if [[ "$v" == "$2" ]]; then ok "$1 = $v"
  elif [[ "${3:-}" == warn ]]; then warn "$1 = $v (expected $2)"
  else bad "$1 = $v (expected $2)"; fi
}
expect_cfg enable_rack_awareness true
expect_cfg partition_autobalancing_mode continuous
expect_cfg cloud_storage_enabled true
expect_cfg write_caching_default false warn
# With retention_local_strict=false, Tiered Storage partitions may keep more local data than
# retention.local.target.* while disk is free. Not wrong, but a new replica then copies more.
info "retention_local_strict = $(cfg_get retention_local_strict) (informational)"
info "raft_learner_recovery_rate = $(cfg_get raft_learner_recovery_rate)"
info "partition_autobalancing_concurrent_moves = $(cfg_get partition_autobalancing_concurrent_moves)"

header "Placement and balancer"
check "one replica per rack, every partition" placement_audit
check "balancer idle" balancer_idle
check "cluster healthy" is_healthy
check "no under-replicated partitions" urp_zero

header "Tiered Storage coverage of important topics ($IMPORTANT_TOPICS_REGEX)"
mapfile -t topics < <(important_topics)
(( ${#topics[@]} )) || warn "no topics match IMPORTANT_TOPICS_REGEX"
for t in "${topics[@]}"; do
  rw="$(rpk topic describe "$t" -c | awk '$1=="redpanda.remote.write" {print $2}')"
  rr="$(rpk topic describe "$t" -c | awk '$1=="redpanda.remote.read" {print $2}')"
  check "$t remote.write=$rw remote.read=$rr" [ "$rw" = true -a "$rr" = true ]
  # Upload backlog: local last offset minus last offset already in object storage.
  info "  $t: largest upload backlog over its partitions = $(ts_backlog_max "$t") offsets"
done

header "Licence"
lic="$(rpk cluster license info 2>&1 || true)"
grep -E 'status|Organization|Expires' <<<"$lic" | sed 's/^/  /'
check "licence valid (enterprise features: Tiered Storage, continuous balancing)" grep -qiE 'status:\s+valid' <<<"$lic"

test_header "Test clients"
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
rb_summary
