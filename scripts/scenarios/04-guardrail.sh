#!/usr/bin/env bash
# Scenario 4: a fresh broker with an empty disk whose seeds resolve to nothing reachable must not
# form a new cluster (empty_seed_starts_cluster=false). It must fail or keep retrying.
set -euo pipefail
SC_NAME=s04-guardrail
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
healthy5 pre
uuid0="$(check_cluster_uuid)"; n0="$(brokers_json | jq length)"
"$ROOT/scripts/broker.sh" start sp-2 --seeds "dead1 dead2 dead3"
note "started sp-2 with seeds dead1/dead2/dead3.rp.test (-> $UNUSED_IP, unreachable); observing 120s"
sleep 120
save_logs sp-2
running="$(docker inspect -f '{{.State.Running}}' "$(cname sp-2)")"
u="$(docker exec "$(cname sp-2)" curl -s -m 5 localhost:9644/v1/cluster/uuid 2>/dev/null || true)"
ready="$(docker exec "$(cname sp-2)" curl -s -m 5 -o /dev/null -w '%{http_code}' localhost:9644/v1/status/ready 2>/dev/null || true)"
tries="$(grep -c 'Requesting node ID' "$RESULTS/scenarios/$SC_NAME-sp-2.log" || true)"
note "sp-2 running=$running, ready-endpoint=$ready, cluster-uuid-response='${u:-none}', node-ID requests in log=$tries"
grep -qiE '"cluster_uuid": *"[0-9a-f-]{36}"' <<<"$u" && die "sp-2 formed or joined a cluster: $u"
[[ "$(check_cluster_uuid)" == "$uuid0" ]] || die "main cluster UUID changed"
[[ "$(brokers_json | jq length)" == "$n0" ]] || die "main cluster broker count changed"
"$ROOT/scripts/broker.sh" rm sp-2
healthy5 post
sc_result PASS "sp-2 with unreachable seeds kept retrying ($tries node-ID requests in 120s), never formed a cluster; main cluster unchanged"
