#!/usr/bin/env bash
# perf-latency.sh --label L --phase P            one window of producer/consumer/e2e perf tools in parallel
# perf-latency.sh --background --label L         loop windows until --stop; phase read from results/current-phase
# perf-latency.sh --stop                         stop the background loop (finishes nothing, kills the window)
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
PIDF="$RESULTS/perf-bg.pid"
mkdir -p "$RESULTS/perf"
label=""; phase=""; mode=once
while [[ $# -gt 0 ]]; do
  case "$1" in
    --label) label="$2"; shift 2 ;;
    --phase) phase="$2"; shift 2 ;;
    --background) mode=bg; shift ;;
    --stop) mode=stop; shift ;;
    --loop-internal) mode=loop; shift ;;
    *) die "bad arg $1" ;;
  esac
done

kafka() {  # kafka <name> <cmd...>
  docker run --rm --name "$1" --network "$NET" --dns "$DNS_IP" --memory 400m \
    -e KAFKA_HEAP_OPTS="-Xms128m -Xmx256m" --entrypoint /opt/kafka/bin/"$2" "$KAFKA_IMAGE" "${@:3}" \
    || { echo "tool $2 exited $?"; return 0; }
}
window() {
  local label="$1" ph="${2:-}"
  [[ -n "$ph" ]] || ph="$(cat "$RESULTS/current-phase" 2>/dev/null || echo none)"
  local wid; wid="${label}-$(date -u +%Y%m%dT%H%M%SZ)"
  local start; start="$(ts)"
  local n=$(( PERF_RATE * PERF_WINDOW_SEC ))
  local raw="$RESULTS/perf/$wid"
  kafka "rp-perf-cons-$wid" kafka-consumer-perf-test.sh --bootstrap-server "$BOOTSTRAP" --topic "$PERF_TOPIC" \
    --messages "$n" --group "perf-consumer-$wid" --from-latest --show-detailed-stats \
    --reporting-interval 5000 --timeout 60000 > "$raw-cons.log" 2>&1 & local cp=$!
  sleep 5   # consumer joins before data flows
  kafka "rp-perf-prod-$wid" kafka-producer-perf-test.sh --topic "$PERF_TOPIC" --num-records "$n" \
    --record-size 1024 --throughput "$PERF_RATE" --print-metrics \
    --producer-props bootstrap.servers="$BOOTSTRAP" acks=all enable.idempotence=true linger.ms=3 batch.size=4096 \
    > "$raw-prod.log" 2>&1 & local pp=$!
  kafka "rp-perf-e2e-$wid" kafka-e2e-latency.sh "$BOOTSTRAP" "$E2E_TOPIC" "$PERF_E2E_COUNT" all 1024 > "$raw-e2e.log" 2>&1 & local ep=$!
  wait "$pp" || true; wait "$ep" || true; wait "$cp" || true
  local phe; phe="$(cat "$RESULTS/current-phase" 2>/dev/null || echo none)"
  [[ -n "${2:-}" ]] && phe="$ph"
  docker run --rm --user "$(id -u):$(id -g)" -v "$ROOT:/work" -w /work/tools "$TOOLS_IMAGE" python3 perf_parse.py \
    "$wid" "$label" "$ph" "$phe" "$start" "/work/results/perf/$wid-prod.log" "/work/results/perf/$wid-cons.log" \
    "/work/results/perf/$wid-e2e.log" "$PERF_RATE"
}
case "$mode" in
  once) [[ -n "$label" ]] || die "--label required"; window "$label" "$phase" ;;
  bg)
    [[ -n "$label" ]] || die "--label required"
    if [[ -f "$PIDF" ]] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then die "background perf loop already running (pid $(cat "$PIDF"))"; fi
    nohup "$0" --loop-internal --label "$label" >> "$RESULTS/perf/bg-$label.log" 2>&1 &
    echo $! > "$PIDF"
    log "background perf loop '$label' started (pid $!)"
    ;;
  loop)
    trap 'exit 0' TERM INT
    while :; do window "$label" "" || true; done
    ;;
  stop)
    if [[ -f "$PIDF" ]]; then
      kill "$(cat "$PIDF")" 2>/dev/null || true
      rm -f "$PIDF"
    fi
    for c in $(docker ps --format '{{.Names}}' | grep '^rp-perf-' || true); do docker rm -f "$c" >/dev/null 2>&1 || true; done
    log "background perf loop stopped"
    ;;
esac
