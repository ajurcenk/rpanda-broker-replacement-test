# =============================================================================
# lib/common.sh - shared helpers for the runbook step scripts
# =============================================================================
# Every step script starts with:
#
#     source "$(dirname "$0")/lib/common.sh"
#     rb_init "$@"            # loads runbook.env, starts logging, parses --yes / --dry-run
#
# Conventions used by all steps:
#   * check "description" <command...>   read-only check, prints PASS/FAIL, counts failures
#   * change "description" <command...>  a change to the cluster: shows the command and asks y/N
#   * rb_summary                         prints the result and exits 1 if any check failed
#
# Global flags (accepted by every step):
#   --yes       answer "yes" to every confirmation (use with care)
#   --dry-run   print changes instead of executing them
# =============================================================================

set -euo pipefail

RB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../runbook.env
source "$RB_DIR/runbook.env"
# Host-action hints (step_12 / step_19). Defaults are set here, not inside ${VAR:-...}, because the
# {NEW}/{OLD} placeholders contain braces that would end such an expansion early.
[[ -n "${START_NEW_BROKER_HINT:-}" ]] || START_NEW_BROKER_HINT='on {NEW}: sudo systemctl start redpanda'
[[ -n "${STOP_OLD_BROKER_HINT:-}" ]]  || STOP_OLD_BROKER_HINT='on {OLD}: sudo systemctl stop redpanda, then terminate the EC2 instance'
# rpk treats an exported-but-empty RPK_* variable as set (e.g. an empty RPK_USER makes it
# attempt SASL). Remove the empty ones.
for _v in $(compgen -v RPK_); do [[ -z "${!_v}" ]] && unset "$_v"; done

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then C_RED=$'\e[31m'; C_GRN=$'\e[32m'; C_YEL=$'\e[33m'; C_BLU=$'\e[36m'; C_B=$'\e[1m'; C_0=$'\e[0m'
else C_RED=""; C_GRN=""; C_YEL=""; C_BLU=""; C_B=""; C_0=""; fi

ts()      { date -u +%Y-%m-%dT%H:%M:%SZ; }
header()  { printf '\n%s== %s ==%s\n' "$C_B" "$*" "$C_0"; }
# test_header: section that only TESTS the migration (test topics/clients); changes nothing in production data
test_header() { printf '\n%s== %s [TEST ONLY] ==%s\n' "$C_B" "$*" "$C_0"; }
info()    { printf '%s[%s]%s %s\n' "$C_BLU" "$(ts)" "$C_0" "$*"; }
ok()      { printf '  %sPASS%s  %s\n' "$C_GRN" "$C_0" "$*"; }
warn()    { printf '  %sWARN%s  %s\n' "$C_YEL" "$C_0" "$*"; RB_WARNS=$((RB_WARNS + 1)); }
bad()     { printf '  %sFAIL%s  %s\n' "$C_RED" "$C_0" "$*"; RB_FAILS=$((RB_FAILS + 1)); }
die()     { printf '\n%sSTOP: %s%s\n' "$C_RED$C_B" "$*" "$C_0" >&2; exit 1; }

RB_FAILS=0
RB_WARNS=0
ASSUME_YES=0
DRY_RUN=0
RB_ARGS=()

# ---------------------------------------------------------------------------
# rb_init "$@": load settings, check tools, start the log file, parse global flags.
# The remaining (script specific) arguments are left in the array RB_ARGS.
# ---------------------------------------------------------------------------
rb_init() {
  local a
  for a in "$@"; do
    case "$a" in
      --yes) ASSUME_YES=1 ;;
      --dry-run) DRY_RUN=1 ;;
      *) RB_ARGS+=("$a") ;;
    esac
  done
  mkdir -p "$RB_STATE_DIR" "$RB_LOG_DIR"
  RB_SCRIPT="$(basename "$0" .sh)"
  RB_LOG="$RB_LOG_DIR/$(date -u +%Y%m%dT%H%M%SZ)-$RB_SCRIPT.log"
  # Everything printed from here on is also written to the log file.
  exec > >(tee -a "$RB_LOG") 2>&1
  info "$RB_SCRIPT started by ${USER:-?} on $(hostname) (log: $RB_LOG)"
  (( DRY_RUN )) && info "DRY RUN: changes are printed, not executed"
  local tool
  for tool in rpk jq curl dig awk; do
    command -v "$tool" >/dev/null || die "required tool '$tool' is not installed on this host"
  done
  # TEST ONLY: RB_ALLOW_NO_AUTH=1 is set by the Docker rehearsal (rehearsal/bastion.sh) for a
  # cluster without SASL. Never set it in production.
  if [[ "${RB_ALLOW_NO_AUTH:-0}" != 1 ]]; then
    [[ -n "$RPK_USER" && -n "$RPK_PASS" ]] || die "export RB_SASL_USER and RB_SASL_PASSWORD first (see runbook.env)"
  fi
  # Unexpected errors must never be silent.
  trap 'printf "%sERROR in %s line %s: %s%s\n" "$C_RED" "$RB_SCRIPT" "$LINENO" "$BASH_COMMAND" "$C_0" >&2' ERR
}

# ---------------------------------------------------------------------------
# check / change / confirm
# ---------------------------------------------------------------------------

# check "description" <command...>
#   Runs a read-only check. Prints PASS or FAIL (with the command's output on failure).
check() {
  local desc="$1"; shift
  local out
  if out="$("$@" 2>&1)"; then
    [[ "$out" == true ]] && out=""          # jq -e prints "true"; not worth showing
    ok "$desc${out:+  ($(head -c 300 <<<"$out" | tr '\n' ' '))}"
  else
    bad "$desc"
    [[ -n "$out" ]] && sed 's/^/        /' <<<"$out"
  fi
  return 0
}

# confirm "question": ask y/N on the terminal. Returns 0 only on "y".
confirm() {
  (( ASSUME_YES )) && { info "auto-confirmed (--yes): $1"; return 0; }
  local reply
  read -r -p "$(printf '%s%s [y/N] %s' "$C_YEL" "$1" "$C_0")" reply </dev/tty
  [[ "$reply" == y || "$reply" == Y ]]
}

# change "description" <command...>
#   Shows exactly what will run, asks for confirmation, then runs it (or prints it with --dry-run).
change() {
  local desc="$1"; shift
  printf '\n  %sCHANGE%s %s\n         command: %s\n' "$C_YEL" "$C_0" "$desc" "$(printf '%q ' "$@")"
  if (( DRY_RUN )); then info "dry run: not executed"; return 0; fi
  confirm "Run this change?" || die "change not confirmed: $desc"
  "$@"
  echo "$(ts) $RB_SCRIPT CHANGE $desc :: $(printf '%q ' "$@")" >> "$RB_STATE_DIR/changes.log"
}

# rb_summary: final line of every step. Exit code 1 if any check failed.
rb_summary() {
  header "Result: $RB_SCRIPT"
  if (( RB_FAILS > 0 )); then
    printf '%s%d check(s) FAILED, %d warning(s). Do not continue until resolved.%s\n' "$C_RED$C_B" "$RB_FAILS" "$RB_WARNS" "$C_0"
    exit 1
  fi
  printf '%sAll checks passed%s (%d warning(s)).\n' "$C_GRN$C_B" "$C_0" "$RB_WARNS"
}

# ---------------------------------------------------------------------------
# Admin API access (curl with the same TLS + SCRAM credentials as rpk)
# ---------------------------------------------------------------------------
_curl_opts() {
  local o=(-sS --fail --max-time 30)
  [[ "$RPK_ADMIN_TLS_ENABLED" == true && -n "$RPK_ADMIN_TLS_CA" ]] && o+=(--cacert "$RPK_ADMIN_TLS_CA")
  [[ -n "$ADMIN_USER" ]] && o+=(-u "$ADMIN_USER:$ADMIN_PASSWORD")
  printf '%s\n' "${o[@]}"
}
_scheme() { [[ "$RPK_ADMIN_TLS_ENABLED" == true ]] && echo https || echo http; }

# admin_on <host[:port]> <path> [extra curl args]: call one specific broker's Admin API.
admin_on() {
  local host="$1" path="$2"; shift 2
  [[ "$host" == *:* ]] || host="$host:$ADMIN_PORT"
  local opts; mapfile -t opts < <(_curl_opts)
  curl "${opts[@]}" "$@" "$(_scheme)://$host$path"
}

# admin_entry: the first Admin API entry point that answers WITH an elected controller.
# (A broker whose decommission has completed still answers, but with a stale view.)
admin_entry() {
  local h
  for h in ${RPK_ADMIN_HOSTS//,/ }; do
    if admin_on "$h" /v1/cluster/health_overview 2>/dev/null | jq -e '.controller_id >= 0' >/dev/null 2>&1; then
      echo "$h"; return 0
    fi
  done
  return 1
}

# admin <path> [extra curl args]: Admin API call through a healthy entry point.
admin() {
  local e; e="$(admin_entry)" || die "no Admin API entry point answered with an elected controller (check RPK_ADMIN_HOSTS, TLS, credentials)"
  admin_on "$e" "$@"
}

# ---------------------------------------------------------------------------
# Cluster state
# ---------------------------------------------------------------------------
brokers_json()      { admin /v1/brokers; }
health_json()       { admin /v1/cluster/health_overview; }
cfg_get()           { rpk cluster config get "$1" | tr -d '"'; }

# Host name a broker advertises (internal RPC address), and the reverse lookup.
broker_host()       { brokers_json | jq -r --argjson id "$1" '.[] | select(.node_id==$id) | .internal_rpc_address'; }
broker_id_of_host() { brokers_json | jq -r --arg h "$1" '.[] | select(.internal_rpc_address==$h) | .node_id'; }

is_healthy()      { health_json | jq -e '.is_healthy == true' >/dev/null; }
urp_count()       { health_json | jq -r '.under_replicated_count // 0'; }
urp_zero()        { local n; n="$(urp_count)"; [[ "$n" == 0 ]] || { echo "under-replicated partitions: $n"; return 1; }; }
moves_in_flight() { admin /v1/partitions/reconfigurations | jq 'length'; }
balancer_idle() {
  local b; b="$(admin /v1/cluster/partition_balancer/status)"
  jq -e '(.status == "ready" or .status == "off") and ((.current_reassignments_count // 0) == 0)' <<<"$b" >/dev/null \
    || { jq -c '{status, current_reassignments_count}' <<<"$b"; return 1; }
  local m; m="$(moves_in_flight)"; [[ "$m" == 0 ]] || { echo "partition moves in flight: $m"; return 1; }
}
no_maintenance() {
  local d; d="$(brokers_json | jq -r '[.[] | select(.maintenance_status.draining == true) | .node_id] | join(",")')"
  [[ -z "$d" ]] || { echo "brokers in maintenance mode: $d"; return 1; }
}
all_versions_ok() {
  local bad_v; bad_v="$(brokers_json | jq -r --arg v "$EXPECTED_VERSION" '.[] | select(((.version // "") | split(" ")[0]) != $v) | "\(.node_id)=\(.version // "unknown")"')"
  [[ -z "$bad_v" ]] || { echo "brokers not on $EXPECTED_VERSION: $bad_v"; return 1; }
  echo "all brokers on $EXPECTED_VERSION"
}

# replica_count <node id>: number of partition replicas the node holds
replica_count() { admin /v1/cluster/partitions | jq --argjson id "$1" '[.[] | select(any(.replicas[]; .node_id==$id))] | length'; }

# has_rack_share <node id>: the node holds at least 80% as many replicas as the emptiest other
# active broker in its rack. Right after a join the balancer may not have started yet, and
# "idle" alone would wrongly look settled.
has_rack_share() {
  local id="$1" rack mates min n
  rack="$(brokers_json | jq -r --argjson id "$id" '.[] | select(.node_id==$id) | .rack')"
  mates="$(brokers_json | jq -r --argjson id "$id" --arg r "$rack" '.[] | select(.rack==$r and .node_id!=$id and .membership_status=="active") | .node_id')"
  [[ -n "$mates" ]] || return 0
  min=""; for m in $mates; do n="$(replica_count "$m")"; [[ -z "$min" || "$n" -lt "$min" ]] && min=$n; done
  n="$(replica_count "$id")"
  (( n * 10 >= min * 8 )) || { echo "node $id holds $n replicas, rack-mates at least $min"; return 1; }
}

# wait_settled <timeout_min> [new node id]: wait until healthy + balancer idle + no URP
# (and, if given, the new node holds its share of replicas), seen twice in a row.
wait_settled() {
  local deadline=$(( $(date +%s) + $1 * 60 )) good=0 new="${2:-}"
  while :; do
    if is_healthy && balancer_idle >/dev/null && urp_zero >/dev/null \
       && { [[ -z "$new" ]] || has_rack_share "$new" >/dev/null; }; then
      good=$((good + 1))
      (( good >= 2 )) && return 0
    else
      good=0
    fi
    (( $(date +%s) < deadline )) || return 1
    info "settling: moves in flight=$(moves_in_flight), URP=$(urp_count)${new:+, node $new replicas=$(replica_count "$new")} | clients: $(client_health_line)"
    sleep "$POLL_SEC"
  done
}

# ---------------------------------------------------------------------------
# Placement audit: every partition must have exactly one replica in each rack.
# ---------------------------------------------------------------------------
placement_audit() {
  local racks parts
  racks="$(brokers_json | jq '[.[] | {key: (.node_id|tostring), value: (.rack // "none")}] | from_entries')"
  parts="$(admin '/v1/cluster/partitions?with_internal=true')"
  local nracks=${#RACKS[@]}
  local viol
  viol="$(jq -r --argjson r "$racks" --argjson n "$nracks" '
      .[] | . as $p | [.replicas[].node_id | tostring | $r[.]] as $rk
      | select(($rk | length) != $n or ($rk | unique | length) != $n)
      | "\($p.ns)/\($p.topic)/\($p.partition_id) replicas=\([$p.replicas[].node_id]) racks=\($rk)"' <<<"$parts")"
  local total; total="$(jq length <<<"$parts")"
  if [[ -n "$viol" ]]; then
    echo "$(wc -l <<<"$viol") of $total partitions violate one-replica-per-rack:"; head -20 <<<"$viol"; return 1
  fi
  echo "$total partitions, one replica per rack"
}

# ---------------------------------------------------------------------------
# DNS
# ---------------------------------------------------------------------------
# resolve <name>: the IPv4 address the system resolver returns now (follows CNAMEs).
resolve() { dig +short "$1" A | grep -E '^[0-9.]+$' | tail -1; }
# dns_ttl <name>: TTL of the final A record as seen by the resolver.
dns_ttl() { dig +noall +answer "$1" A | awk '$4=="A" {print $2}' | tail -1; }

# seeds_check: Path A seed rules.
#   * every seed DNS name resolves to an active, alive broker
#   * the seed names cover every rack exactly once
#   * every broker's seed_servers equals SEED_DNS_NAMES, and no broker lists its own host
seeds_check() {
  local bj; bj="$(brokers_json)"
  local fails=0 racks_seen=() name ip id rack host
  for name in "${SEED_DNS_NAMES[@]}"; do
    ip="$(resolve "$name")"
    [[ -n "$ip" ]] || { echo "seed $name does not resolve"; fails=1; continue; }
    id=""
    while read -r bid host; do
      [[ "$(resolve "$host")" == "$ip" ]] && id="$bid"
    done < <(jq -r '.[] | "\(.node_id) \(.internal_rpc_address)"' <<<"$bj")
    [[ -n "$id" ]] || { echo "seed $name -> $ip is not a current cluster member"; fails=1; continue; }
    local st; st="$(jq -r --argjson id "$id" '.[] | select(.node_id==$id) | "\(.membership_status) \(.is_alive) \(.rack // "none")"' <<<"$bj")"
    read -r mem alive rack <<<"$st"
    [[ "$mem $alive" == "active true" ]] || { echo "seed $name -> node $id is $mem/alive=$alive"; fails=1; }
    racks_seen+=("$rack")
    echo "seed $name -> $ip -> node $id ($rack) $mem"
  done
  [[ "$(printf '%s\n' "${racks_seen[@]}" | sort -u | wc -l)" == "${#RACKS[@]}" ]] \
    || { echo "seed names do not cover every rack once: ${racks_seen[*]}"; fails=1; }
  local want; want="$(printf '%s\n' "${SEED_DNS_NAMES[@]}" | sort | paste -sd,)"
  while read -r bid host; do
    local cfg; cfg="$(admin_on "$host" /v1/node_config 2>/dev/null)" || { echo "cannot read node_config of node $bid ($host)"; fails=1; continue; }
    local got; got="$(jq -r '[.seed_servers[].host.address] | sort | join(",")' <<<"$cfg")"
    [[ "$got" == "$want" ]] || { echo "node $bid seed_servers = $got (expected $want)"; fails=1; }
    jq -e --arg h "$host" 'any(.seed_servers[]; .host.address == $h)' <<<"$cfg" >/dev/null \
      && { echo "node $bid lists its own host $host as a seed"; fails=1; }
  done < <(jq -r '.[] | select(.membership_status=="active") | "\(.node_id) \(.internal_rpc_address)"' <<<"$bj")
  return $fails
}

# ---------------------------------------------------------------------------
# Swap plan lookup
# ---------------------------------------------------------------------------
# swap_lookup <dns-name>: sets SWAP_DNS SWAP_OLD SWAP_NEW SWAP_RACK SWAP_NEW_IP SWAP_NO SWAP_DIR
swap_lookup() {
  local want="$1" i=0 line
  for line in "${SWAP_PLAN[@]}"; do
    i=$((i + 1))
    read -r SWAP_DNS SWAP_OLD SWAP_NEW SWAP_RACK SWAP_NEW_IP _ <<<"$line"
    [[ "${SWAP_NEW_IP:-}" == - ]] && SWAP_NEW_IP=""
    if [[ "$SWAP_DNS" == "$want" || "$i" == "$want" ]]; then
      SWAP_NO=$i
      SWAP_DIR="$RB_STATE_DIR/swap-$i-${SWAP_DNS%%.*}"
      mkdir -p "$SWAP_DIR"
      info "swap $SWAP_NO: $SWAP_DNS  $SWAP_OLD -> $SWAP_NEW  ($SWAP_RACK)"
      return 0
    fi
  done
  die "'$want' is not in SWAP_PLAN (give the DNS name or the swap number 1-${#SWAP_PLAN[@]})"
}
# require_swap_arg: every per-swap step takes the swap's DNS name or number as first argument.
require_swap_arg() {
  [[ ${#RB_ARGS[@]} -ge 1 ]] || die "usage: $0 <swap DNS name | swap number> [--yes] [--dry-run]"
  swap_lookup "${RB_ARGS[0]}"
}

# Small key=value state files, e.g. state_set "$SWAP_DIR/go" ok
state_set() { mkdir -p "$(dirname "$1")"; echo "$2" > "$1"; }
state_get() { cat "$1" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# Test client health (see tools/test_clients.sh). Used by every step.
# ---------------------------------------------------------------------------
CLIENT_DIR="$RB_STATE_DIR/clients"
# client_health_line: one-line summary, e.g. "producer up, consumer up, e2e p99 35 ms, gaps 0, errors 0"
client_health_line() {
  [[ -d "$CLIENT_DIR" ]] || { echo "test clients not started"; return 0; }
  local pu=down cu=down
  [[ -f "$CLIENT_DIR/producer.pid" ]] && kill -0 "$(cat "$CLIENT_DIR/producer.pid")" 2>/dev/null && pu=up
  [[ -f "$CLIENT_DIR/consumer.pid" ]] && kill -0 "$(cat "$CLIENT_DIR/consumer.pid")" 2>/dev/null && cu=up
  local last; last="$(tail -1 "$CLIENT_DIR/consumer-stats.csv" 2>/dev/null || true)"
  local p99="?" gaps=0
  [[ -n "$last" ]] && IFS=, read -r _ _ _ p99 _ gaps _ <<<"$last"
  local errs; errs="$(grep -ciE 'error|fail|timeout' "$CLIENT_DIR/producer.err" 2>/dev/null || true)"
  echo "producer $pu, consumer $cu, e2e p99 ${p99} ms, gaps ${gaps:-0}, producer errors ${errs:-0}"
}
# client_ok: fails if a client is down, any gap was seen, or the producer logged errors.
client_ok() {
  local l; l="$(client_health_line)"
  [[ "$l" == "test clients not started" ]] && { echo "$l (start them with step_00)"; return 0; }
  echo "$l"
  [[ "$l" == *"producer up, consumer up"* && "$l" == *"gaps 0,"* && "$l" == *"producer errors 0" ]]
}
# client_p99: latest end-to-end p99 in ms (empty if unknown)
client_p99() { tail -1 "$CLIENT_DIR/consumer-stats.csv" 2>/dev/null | cut -d, -f4; }

# ---------------------------------------------------------------------------
# Tiered Storage upload backlog
# ---------------------------------------------------------------------------
# ts_backlog_max <topic>: largest (local last offset - uploaded last offset) over the topic's partitions.
ts_backlog_max() {
  local t="$1" p s lag max=0
  while read -r p; do
    s="$(admin "/v1/cloud_storage/status/$t/$p" 2>/dev/null)" || continue
    lag=$(jq '(.local_log_last_offset - .cloud_log_last_offset) | if . < 0 then 0 else . end' <<<"$s")
    (( lag > max )) && max=$lag
  done < <(rpk topic describe "$t" -p | awk 'NR>1 {print $1}')
  echo "$max"
}
important_topics() { rpk topic list | awk 'NR>1 {print $1}' | grep -E "$IMPORTANT_TOPICS_REGEX" || true; }

# recovery_rate_is_original: current raft_learner_recovery_rate equals the value saved by step_02
recovery_rate_is_original() {
  local o; o="$(state_get "$RB_STATE_DIR/orig.raft_learner_recovery_rate")"
  [[ -n "$o" ]] || { echo "original not saved yet (run step_02)"; return 1; }
  local c; c="$(cfg_get raft_learner_recovery_rate)"
  [[ "$c" == "$o" ]] || { echo "raft_learner_recovery_rate is $c, original $o"; return 1; }
  echo "$c (original)"
}
# config_status_clean: no broker needs a restart or reports invalid/unknown properties
config_status_clean() {
  local st; st="$(rpk cluster config status)"
  awk 'NR>1 && ($3=="true" || $4!="[]" || $5!="[]") {bad=1} END {exit bad}' <<<"$st" || { echo "$st"; return 1; }
}
# p99_within <ratio>: client e2e p99 is at most ratio x baseline (passes if no baseline)
p99_within() {
  local b; b="$(state_get "$RB_STATE_DIR/baseline.client_p99_ms")"; local c; c="$(client_p99)"
  [[ -z "$b" || -z "$c" ]] && { echo "no baseline or no current value"; return 0; }
  awk -v c="$c" -v b="$b" -v r="$1" 'BEGIN {exit !(c <= r * (b > 0 ? b : 1))}' || { echo "client p99 $c ms > ${1}x baseline $b ms"; return 1; }
  echo "client p99 $c ms (baseline $b ms)"
}

# ---------------------------------------------------------------------------
# Small named checks (readable arguments for `check`)
# ---------------------------------------------------------------------------
node_active()   { brokers_json | jq -e --argjson id "$1" 'any(.[]; .node_id==$id and .membership_status=="active" and .is_alive)' >/dev/null; }
node_member()   { brokers_json | jq -e --argjson id "$1" 'any(.[]; .node_id==$id)' >/dev/null; }
node_gone()     { ! node_member "$1"; }
node_rack_is()  { local r; r="$(brokers_json | jq -r --argjson id "$1" '.[] | select(.node_id==$id) | .rack')"; [ "$r" = "$2" ] || { echo "rack is $r"; return 1; }; }
not_member()    { local id; id="$(broker_id_of_host "$1")"; [ -z "$id" ] || { echo "$1 is already node $id"; return 1; }; }
dns_points_to() { local a b; a="$(resolve "$1")"; b="$(resolve "$2")"; [ -n "$a" ] && [ "$a" = "$b" ] || { echo "$1 -> ${a:-nothing}, $2 -> ${b:-nothing}"; return 1; }; }

# ---------------------------------------------------------------------------
# Route 53 helpers (aws CLI)
# ---------------------------------------------------------------------------
# r53_value <zone id> <name>: current value of the A or CNAME record (empty if none)
r53_value() {
  local n="${2%.}."
  aws route53 list-resource-record-sets --hosted-zone-id "$1" --start-record-name "$n" --max-items 1 \
    --query "ResourceRecordSets[?Name=='$n'] | [0].ResourceRecords[0].Value" --output text 2>/dev/null \
    | grep -v '^None$' || true
}
# r53_upsert <zone id> <name> <type> <value> <ttl>: asks for confirmation, applies, waits for INSYNC
r53_upsert() {
  local zone="$1" n="${2%.}." type="$3" value="$4" ttl="$5"
  local f; f="$(mktemp)"
  jq -n --arg n "$n" --arg t "$type" --arg v "$value" --argjson ttl "$ttl" \
    '{Comment: "broker replacement", Changes: [{Action: "UPSERT", ResourceRecordSet: {Name: $n, Type: $t, TTL: $ttl, ResourceRecords: [{Value: $v}]}}]}' > "$f"
  printf '\n  %sCHANGE%s Route 53 UPSERT %s %s %s (TTL %s) in zone %s\n' "$C_YEL" "$C_0" "$n" "$type" "$value" "$ttl" "$zone"
  if (( DRY_RUN )); then info "dry run: not applied"; rm -f "$f"; return 0; fi
  confirm "Apply this DNS change?" || die "not confirmed"
  local id; id="$(aws route53 change-resource-record-sets --hosted-zone-id "$zone" --change-batch "file://$f" --query ChangeInfo.Id --output text)"
  rm -f "$f"
  echo "$(ts) $RB_SCRIPT CHANGE route53 UPSERT $n $type $value ($id)" >> "$RB_STATE_DIR/changes.log"
  info "waiting for Route 53 change $id to be INSYNC"
  aws route53 wait resource-record-sets-changed --id "$id"
}

# port_open <host> <port>: TCP connect within 3 s
port_open() { timeout 3 bash -c "exec 3<>/dev/tcp/$1/$2" 2>/dev/null; }

# host_hint <template>: fill {NEW}/{OLD} (and {NEW%.suffix}/{OLD%.suffix}) with this swap's hosts
host_hint() {
  local t="$1" v
  while [[ "$t" =~ \{(NEW|OLD)%([^}]*)\} ]]; do
    [[ "${BASH_REMATCH[1]}" == NEW ]] && v="$SWAP_NEW" || v="$SWAP_OLD"
    t="${t/"${BASH_REMATCH[0]}"/${v%"${BASH_REMATCH[2]}"}}"
  done
  t="${t//\{NEW\}/$SWAP_NEW}"; t="${t//\{OLD\}/$SWAP_OLD}"
  echo "$t"
}
