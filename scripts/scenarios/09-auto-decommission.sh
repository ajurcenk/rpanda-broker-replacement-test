#!/usr/bin/env bash
# Scenario 9 (optional): automatic decommission of a dead broker, if 25.3.17 has the property.
set -euo pipefail
SC_NAME=s09-auto-decommission
source "$(dirname "$0")/../lib/common.sh"; source "$ROOT/scripts/lib/scenario.sh"
P=partition_autobalancing_node_autodecommission_timeout_sec
out="$(rpk_exec cluster config get "$P" 2>&1 || true)"
if grep -qi 'unknown property' <<<"$out"; then
  note "$P does not exist in v${RP_VERSION}: $out"
  sc_result SKIPPED "property $P not present in v${RP_VERSION}"
  exit 0
fi
die "$P exists in this version; implement the timed test before relying on it ($out)"
