#!/usr/bin/env bash
# report.sh: build REPORT.md and charts from results/
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
mkdir -p "$RESULTS/raw"
# final recovery-rate state, if a cluster is up
if any_broker >/dev/null 2>&1 && [[ -f "$RESULTS/recovery-rate.setup" ]]; then
  v="$(cfg_get raft_learner_recovery_rate)"; s="$(cat "$RESULTS/recovery-rate.setup")"
  jq -n --arg v "$v" --arg s "$s" '{value:$v, setup:$s, equal:($v==$s)}' > "$RESULTS/raw/final-recovery-rate.json"
fi
envs="WARN_RATIO=$WARN_RATIO;ALERT_RATIO=$ALERT_RATIO;SOAK_RECOVERY_RATIO=$SOAK_RECOVERY_RATIO;PERF_RATE=$PERF_RATE;PERF_WINDOW_SEC=$PERF_WINDOW_SEC;PERF_E2E_COUNT=$PERF_E2E_COUNT;SEGMENT_BYTES=$SEGMENT_BYTES;LOCAL_TARGET_BYTES=$LOCAL_TARGET_BYTES;PRELOAD_BYTES_PER_PARTITION=$PRELOAD_BYTES_PER_PARTITION;FC_TARGET_BYTES=$FC_TARGET_BYTES;RAISED_RECOVERY_RATE=$RAISED_RECOVERY_RATE;SOAK_SEC=$SOAK_SEC"
docker run --rm --user "$(id -u):$(id -g)" -e REPORT_ENV="$envs" -v "$ROOT:/work" -w /work/tools "$TOOLS_IMAGE" python3 report.py
