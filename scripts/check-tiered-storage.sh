#!/usr/bin/env bash
# check-tiered-storage.sh [--label <phase>] : prove TS topics hold data only in MinIO (see tools/ts_check.py)
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
label="adhoc"; [[ "${1:-}" == "--label" ]] && label="$2"
mkdir -p "$RESULTS/raw"
ls_file="raw/mc-ls-${label}.json"
mc_run ls --recursive --json "local/${S3_BUCKET}" > "$RESULTS/$ls_file" || die "mc ls failed"
RUNNING="$(running_instances | paste -sd' ')"
docker run --rm -i --network "$NET" --dns "$DNS_IP" --user "$(id -u):$(id -g)" \
  -e BOOTSTRAP="$BOOTSTRAP" -e RUNNING="$RUNNING" -e SEGMENT_BYTES="$SEGMENT_BYTES" \
  -e UPLOAD_INTERVAL_SEC="$UPLOAD_INTERVAL_SEC" -e CONTROL_TOPIC="$CONTROL_TOPIC" \
  -e TS_SKIP_READS="${TS_SKIP_READS:-0}" \
  -v "$ROOT:/work" -w /work/tools "$TOOLS_IMAGE" python3 ts_check.py "$label" "/work/results/$ls_file" \
  || die "tiered storage check '$label' failed (results/tiered-storage-check-${label}.json)"
