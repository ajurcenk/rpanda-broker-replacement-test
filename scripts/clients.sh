#!/usr/bin/env bash
# clients.sh start|stop|restart|status : the long-running test producer and verifier consumer.
# Both bootstrap through the aliases b1..b5 only.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
bg_topics="$(seq -f 'bg.%02g' 0 $((BG_TOPICS-1)) | head -3 | paste -sd,)"
start_one() {
  local name="$1"; shift
  if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" == true ]]; then log "$name running"; return; fi
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --network "$NET" --dns "$DNS_IP" --user "$(id -u):$(id -g)" \
    --memory 256m -e BOOTSTRAP="$BOOTSTRAP" -v "$ROOT:/work" -w /work "$TOOLS_IMAGE" "$@" >/dev/null
  log "started $name"
}
stop_one() {
  docker stop -t 60 "$1" >/dev/null 2>&1 || true
  docker logs "$1" >> "$RESULTS/$1.log" 2>&1 || true
  docker rm -f "$1" >/dev/null 2>&1 || true
  log "stopped $1"
}
case "${1:?start|stop|restart|status}" in
  start)
    start_one rp-client-producer python3 /work/client/producer.py --rate "$PRODUCER_RATE" --bg-topics "$bg_topics" --bg-rate "$BG_PRODUCER_RATE"
    start_one rp-client-consumer python3 /work/client/consumer.py ;;
  stop) stop_one rp-client-producer; stop_one rp-client-consumer ;;
  restart) "$0" stop; "$0" start ;;
  status) for c in rp-client-producer rp-client-consumer; do echo "$c $(docker inspect -f '{{.State.Status}}' $c 2>/dev/null || echo absent)"; done ;;
esac
