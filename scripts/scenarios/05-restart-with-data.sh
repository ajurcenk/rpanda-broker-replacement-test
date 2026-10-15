#!/usr/bin/env bash
# Scenario 5: restart a surviving broker with its data intact after all aliases were repointed.
set -euo pipefail
SC_NAME=s05-restart-with-data
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
healthy5 pre
inst="$(alias_target b4)"; id0="$(id_of "$inst")"
docker restart -t 30 "$(cname "$inst")" >/dev/null
note "restarted $inst (node $id0) with data intact"
sleep 3; wait_back "$inst"
id1="$(admin_on "$inst" /v1/node_config | jq -r .node_id)"
[[ "$id1" == "$id0" ]] || die "$inst came back with node id $id1, expected $id0"
wait_settled 900
healthy5 post
sc_result PASS "$inst rejoined with the same node id $id0"
