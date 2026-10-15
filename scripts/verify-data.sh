#!/usr/bin/env bash
# verify-data.sh: no acked message lost or reordered on the sequencer topic; group offset preserved.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
rc=0; py verify.py /work/results/raw/group-offset-pre-swaps > "$RESULTS/raw/verify-data.log" 2>&1 || rc=$?
cat "$RESULTS/raw/verify-data.log"
(( rc == 0 )) || die "data verification failed (results/verify-data.json)"
viol="$(grep -vc 'resume-offset-mismatch' "$RESULTS/violations.log" 2>/dev/null || true)"
log "data verification PASS; consumer-side violations logged during the run: ${viol:-0} (results/violations.log)"
