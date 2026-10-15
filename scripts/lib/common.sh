# Shared helpers. Source after `set -euo pipefail`.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=../../env.sh
source "$ROOT/env.sh"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"
# No silent exits: report the failing command and location on any unhandled error.
set -E
trap 'rc=$?; printf "\033[1;31m[%s] FAIL: %s:%s: command exited %s: %s\033[0m\n" "$(date -u +%H:%M:%S)" "${BASH_SOURCE[0]##*/}" "$LINENO" "$rc" "$BASH_COMMAND" >&2' ERR

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
epoch() { date +%s; }
log() { printf '\033[36m[%s]\033[0m %s\n' "$(ts)" "$*" >&2; }
warn() { printf '\033[33m[%s] WARN:\033[0m %s\n' "$(ts)" "$*" >&2; }
die() { printf '\033[1;31m[%s] FAIL: %s\033[0m\n' "$(ts)" "$*" >&2; exit 1; }

# gate <name> <cmd...>: run a check, record PASS/FAIL in results/gates.csv, fail loudly.
gate() {
  local name="$1"; shift
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  local detail; detail="$(printf '%s' "$out" | sed 's/\x1b\[[0-9;]*m//g' | tail -n 3 | tr '\n,' ' ;')"
  if [[ $rc -eq 0 ]]; then
    echo "$(ts),${GATE_CTX:-},${name},PASS,${detail}" >> "$RESULTS/gates.csv"
    log "GATE PASS: $name"
  else
    echo "$(ts),${GATE_CTX:-},${name},FAIL,${detail}" >> "$RESULTS/gates.csv"
    printf '%s\n' "$out" >&2
    die "gate '$name' failed (context: ${GATE_CTX:-none})"
  fi
}

# retry_until <timeout_sec> <interval_sec> <description> <cmd...>
retry_until() {
  local timeout="$1" interval="$2" desc="$3"; shift 3
  local deadline=$(( $(epoch) + timeout ))
  until "$@" >/dev/null 2>&1; do
    (( $(epoch) >= deadline )) && die "timed out after ${timeout}s waiting for: $desc"
    sleep "$interval"
  done
}

cname() { echo "rp-$1"; }              # container name for an instance
vname() { echo "rpbr-$1-data"; }       # data volume for an instance
container_running() { [[ "$(docker inspect -f '{{.State.Running}}' "$(cname "$1")" 2>/dev/null)" == "true" ]]; }

# Instances whose containers are currently running.
running_instances() {
  local i
  for i in "${!INST_IP[@]}"; do if container_running "$i"; then echo "$i"; fi; done | sort
}

# Any running broker that is still a cluster member, used as the execution point for rpk /
# Admin API calls. Brokers whose decommission completed (results/decommissioned.txt) are skipped:
# until their container is removed they still answer, with a stale view (no controller).
any_broker() {
  local i id
  for i in $(running_instances); do
    id="$(awk -v i="$i" '$2==i {print $1}' "$RESULTS/node_ids.txt" 2>/dev/null | tail -1)"
    [[ -n "$id" ]] && grep -qx "$id" "$RESULTS/decommissioned.txt" 2>/dev/null && continue
    if docker exec "$(cname "$i")" curl -sf -m 3 localhost:9644/v1/status/ready >/dev/null 2>&1; then echo "$i"; return 0; fi
  done
  return 1
}

rpk_exec() { local b; b="$(any_broker)" || die "no live broker for rpk"; docker exec "$(cname "$b")" rpk "$@"; }
rpk_on() { local inst="$1"; shift; docker exec "$(cname "$inst")" rpk "$@"; }

# admin <path> [curl args...]: Admin API call through a live broker.
admin() {
  local path="$1"; shift
  local b; b="$(any_broker)" || die "no live broker for admin API"
  docker exec "$(cname "$b")" curl -sf -m 30 "$@" "http://localhost:9644${path}"
}
admin_on() { local inst="$1" path="$2"; shift 2; docker exec "$(cname "$inst")" curl -sf -m 30 "$@" "http://localhost:9644${path}"; }

# tools <cmd...>: run in the tools container on the test network (DNS via CoreDNS).
tools() {
  docker run --rm -i --network "$NET" --dns "$DNS_IP" --user "$(id -u):$(id -g)" \
    -e BOOTSTRAP="$BOOTSTRAP" -v "$ROOT:/work" -w /work "$TOOLS_IMAGE" "$@"
}
py() { docker run --rm -i --network "$NET" --dns "$DNS_IP" --user "$(id -u):$(id -g)" -e BOOTSTRAP="$BOOTSTRAP" -e RUNNING="$(running_instances | paste -sd" ")" -v "$ROOT:/work" -w /work/tools "$TOOLS_IMAGE" python3 "$@"; }

mc_run() {
  docker run --rm --network "$NET" --dns "$DNS_IP" \
    -e MC_HOST_local="http://${S3_ACCESS_KEY}:${S3_SECRET_KEY}@minio.${ZONE}:9000" "$MC_IMAGE" "$@"
}

phase() { echo "$1" > "$RESULTS/current-phase.tmp" && mv "$RESULTS/current-phase.tmp" "$RESULTS/current-phase"; log "phase -> $1"; }

# Cluster config property value (raw rpk output, quotes stripped).
cfg_get() { rpk_exec cluster config get "$1" | tr -d '"'; }

# Broker list as JSON (Admin API).
brokers_json() { admin /v1/brokers; }

# node id of an instance via its own admin API
node_id_of() { admin_on "$1" /v1/node_config | jq -r '.node_id'; }

swaps_csv() { echo "$(ts),$*" >> "$RESULTS/swaps.csv"; }
