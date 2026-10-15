#!/usr/bin/env bash
# down.sh [--purge-results]: assert the recovery rate is back at its setup value (if the cluster
# is up), then remove all brokers, volumes, clients, DNS and MinIO.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/dns.sh"
"$ROOT/scripts/perf-latency.sh" --stop >/dev/null 2>&1 || true
if any_broker >/dev/null 2>&1 && [[ -f "$RESULTS/recovery-rate.setup" ]]; then
  [[ -f "$RESULTS/recovery-rate.orig" ]] && "$ROOT/scripts/recovery-rate.sh" restore down
  if ! "$ROOT/scripts/recovery-rate.sh" assert-setup; then
    die "raft_learner_recovery_rate is not back at its setup value. If a runbook rehearsal raised it
       (step_11 without step_17), restore it first, e.g.:
         docker exec $(cname "$(any_broker)") rpk cluster config set raft_learner_recovery_rate $(cat "$RESULTS/recovery-rate.setup")
       then run down.sh again."
  fi
fi
"$ROOT/scripts/clients.sh" stop >/dev/null 2>&1 || true
for c in $(docker ps -a --format '{{.Names}}' | grep -E '^rp-(io|nv|tmp|sp|client|perf|g)' || true); do docker rm -f "$c" >/dev/null; done
for v in $(docker volume ls --format '{{.Name}}' | grep -E '^rpbr-.*-data$' | grep -v minio || true); do docker volume rm "$v" >/dev/null; done
docker compose -f "$ROOT/docker-compose.yml" down -v >/dev/null 2>&1 || true
write_hosts_initial
if [[ "${1:-}" == "--purge-results" ]]; then
  find "$RESULTS" -mindepth 1 -delete
  log "results purged"
fi
log "environment removed"
