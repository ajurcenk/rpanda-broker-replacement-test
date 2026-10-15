#!/usr/bin/env bash
# =============================================================================
# rehearsal/bastion.sh - a Docker "bastion host" for rehearsing the runbook
# =============================================================================
#   rehearsal/bastion.sh start     build the image (once) and start the bastion container
#   rehearsal/bastion.sh shell     open an interactive shell in it (cd /runbook, then run steps)
#   rehearsal/bastion.sh run <step> [args]   run one step from the host, e.g. run step_01_preflight.sh
#   rehearsal/bastion.sh status    show whether it is running and what it can reach
#   rehearsal/bastion.sh stop      stop the test clients (if running) and remove the container
#   rehearsal/bastion.sh reset     also delete rehearsal state and logs (start a rehearsal from scratch)
#
# Requires the test harness to be up (../scripts/up.sh): the network "rpnet", CoreDNS at
# 172.30.0.53 and the brokers. The container:
#   * joins rpnet and uses the harness CoreDNS, so b1..b5.rp.test resolve like on the brokers
#   * mounts this runbook directory at /runbook (the scripts you edit are the scripts you run)
#   * mounts rehearsal/runbook.env OVER /runbook/runbook.env (inside the container only)
#   * mounts rehearsal/aws as the aws CLI (Route 53 calls edit ../coredns/rp.test.hosts)
#   * keeps state and logs in rehearsal/state and rehearsal/logs
# =============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
RUNBOOK="$(dirname "$HERE")"
HARNESS="$(dirname "$RUNBOOK")"
NAME=rb-bastion
IMAGE=rb-bastion:25.3.17

running() { [[ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null)" == true ]]; }

case "${1:-}" in
  start)
    docker network inspect rpnet >/dev/null 2>&1 || { echo "harness network rpnet not found: run ../scripts/up.sh first"; exit 1; }
    if running; then echo "$NAME already running"; exit 0; fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$HERE" >/dev/null
    mkdir -p "$HERE/state" "$HERE/logs"
    chmod +x "$HERE/aws" "$RUNBOOK"/step_*.sh "$RUNBOOK"/tools/*.sh
    docker run -d --name "$NAME" --hostname rb-bastion \
      --network rpnet --dns 172.30.0.53 \
      --user "$(id -u):$(id -g)" -e HOME=/tmp \
      -e RB_ALLOW_NO_AUTH=1 -e RB_STATE_DIR=/rehearsal/state -e RB_LOG_DIR=/rehearsal/logs \
      -v "$RUNBOOK:/runbook" \
      -v "$HERE/runbook.env:/runbook/runbook.env:ro" \
      -v "$HERE:/rehearsal" \
      -v "$HERE/aws:/usr/local/bin/aws:ro" \
      -v "$HARNESS/coredns:/coredns" \
      "$IMAGE" >/dev/null
    echo "$NAME started. Open a shell with: $0 shell"
    ;;
  shell)
    running || { echo "$NAME is not running: $0 start"; exit 1; }
    exec docker exec -it -w /runbook "$NAME" bash
    ;;
  run)
    shift
    running || { echo "$NAME is not running: $0 start"; exit 1; }
    step="${1:?which step? e.g. step_01_preflight.sh}"; shift
    # -it so the y/N confirmations work; drop it (use --yes) when there is no terminal
    if [[ -t 0 ]]; then exec docker exec -it -w /runbook "$NAME" "./$step" "$@"
    else exec docker exec -w /runbook "$NAME" "./$step" "$@"; fi
    ;;
  status)
    if running; then
      echo "$NAME running"
      docker exec "$NAME" bash -c 'rpk version | head -1; for n in b1 b2 b3 b4 b5; do printf "  %s.rp.test -> %s\n" "$n" "$(dig +short $n.rp.test | tail -1)"; done'
    else
      echo "$NAME not running"
    fi
    ;;
  stop)
    if running; then docker exec -w /runbook "$NAME" ./tools/test_clients.sh stop 2>/dev/null || true; fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "$NAME removed"
    ;;
  reset)
    # reset deletes the rehearsal state, including the saved original recovery rate that
    # step_17 needs. Refuse while a swap still has the rate raised (step_11 without step_17).
    raised="$(ls "$HERE"/state/swap-*/raised 2>/dev/null || true)"
    if [[ -n "$raised" && "${2:-}" != --force ]]; then
      echo "refusing: the recovery rate is still raised by a rehearsal swap:"
      echo "$raised" | sed 's/^/  /'
      echo "first run:  $0 run step_17_restore_recovery_rate.sh <swap> --yes"
      echo "(or: $0 reset --force, then restore it by hand; see ../scripts/down.sh)"
      exit 1
    fi
    "$0" stop
    rm -rf "$HERE/state" "$HERE/logs"
    echo "rehearsal state and logs deleted"
    ;;
  *)
    sed -n '3,10p' "$0"; exit 2 ;;
esac
