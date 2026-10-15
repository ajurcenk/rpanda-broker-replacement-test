#!/usr/bin/env bash
# broker.sh start <instance> [--seeds "names"]    start with a fresh or existing volume (idempotent)
#                                                 the config is re-copied from config/rendered/ at every start
# broker.sh stop|kill <instance>                  stop the container, keep the volume
# broker.sh rm <instance>                          remove container AND delete its volume (NVMe wipe)
# broker.sh restart <instance>                     restart the existing container (data intact)
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/dns.sh"
cmd="${1:?usage}"; inst="${2:?instance}"; shift 2
[[ -n "${INST_IP[$inst]:-}" ]] || die "unknown instance $inst"
c="$(cname "$inst")"; v="$(vname "$inst")"
case "$cmd" in
  start)
    if container_running "$inst"; then log "$inst already running"; exit 0; fi
    if docker inspect "$c" >/dev/null 2>&1; then
      log "starting existing container $c"; docker start "$c" >/dev/null; exit 0
    fi
    "$ROOT/scripts/render-config.sh" "$inst" "$@" >/dev/null
    ensure_instance_record "$inst"
    docker volume create "$v" >/dev/null
    log "starting $inst (${INST_IP[$inst]}, rack ${INST_RACK[$inst]}, volume $v)"
    docker run -d --name "$c" --hostname "$inst" \
      --network "$NET" --ip "${INST_IP[$inst]}" --dns "$DNS_IP" \
      --memory "$BROKER_MEM_LIMIT" \
      --label rpbr.instance="$inst" \
      -v "$v:/var/lib/redpanda/data" \
      -v "$ROOT/config/rendered:/etc/redpanda-src:ro" \
      -v "$ROOT/config/bootstrap.yaml:/etc/redpanda-bootstrap.yaml:ro" \
      --entrypoint /bin/bash "$RP_IMAGE" -c \
      'cp /etc/redpanda-src/'"$inst"'.yaml /etc/redpanda/redpanda.yaml &&
       cp /etc/redpanda-bootstrap.yaml /etc/redpanda/.bootstrap.yaml &&
       exec rpk redpanda start --smp '"$RP_SMP"' --memory '"$RP_MEMORY"' --reserve-memory 0M --overprovisioned' >/dev/null
    ;;
  stop) docker stop -t 30 "$c" >/dev/null 2>&1 || true; log "stopped $inst" ;;
  kill) docker kill "$c" >/dev/null 2>&1 || true; log "killed $inst" ;;
  restart) docker restart -t 30 "$c" >/dev/null; log "restarted $inst" ;;
  rm)
    docker rm -f "$c" >/dev/null 2>&1 || true
    docker volume rm "$v" >/dev/null 2>&1 || true
    docker volume inspect "$v" >/dev/null 2>&1 && die "volume $v still exists"
    log "removed $inst container and volume $v"
    ;;
  *) die "unknown command $cmd" ;;
esac
