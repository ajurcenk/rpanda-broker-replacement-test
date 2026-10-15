#!/usr/bin/env bash
# run-scenarios.sh [N...]: run scenarios (default 1-7 and 9; 8 runs inside swap 3 of run-migration.sh)
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
list=("$@"); ((${#list[@]})) || list=(1 2 3 4 5 6 7 9)
for n in "${list[@]}"; do
  f="$(ls "$ROOT"/scripts/scenarios/0"$n"-*.sh)"
  log "=== scenario $n: $(basename "$f") ==="
  "$f" 2>&1 | tee "$RESULTS/scenarios/run-0$n.log"
done
