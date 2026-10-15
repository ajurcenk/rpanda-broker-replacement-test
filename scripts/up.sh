#!/usr/bin/env bash
# up.sh: DNS, MinIO, bucket, then the 5 original brokers (3 seeds together, then io-b2/io-b4).
# Idempotent: re-running against a healthy cluster only re-checks it.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
source "$ROOT/scripts/lib/dns.sh"
source "$ROOT/scripts/lib/cluster.sh"

for img in "$MINIO_IMAGE" "$MC_IMAGE" "$TOOLS_IMAGE"; do
  docker image inspect "$img" >/dev/null 2>&1 || {
    log "building $img"
    case "$img" in
      "$TOOLS_IMAGE") docker build -q -t "$TOOLS_IMAGE" "$ROOT/client" >/dev/null ;;
      *) docker compose -f "$ROOT/docker-compose.yml" build minio mc-init >/dev/null ;;
    esac; }
done
docker image inspect "$RP_IMAGE" >/dev/null 2>&1 || docker pull -q "$RP_IMAGE"

[[ -f "$HOSTS_FILE" ]] || write_hosts_initial
docker compose -f "$ROOT/docker-compose.yml" up -d coredns minio >/dev/null
retry_until 30 1 "CoreDNS answering" bash -c "dig +short +time=1 +tries=1 @$DNS_IP minio.$ZONE | grep -q ."
retry_until 60 2 "MinIO ready" mc_run ready local
docker compose -f "$ROOT/docker-compose.yml" run --rm mc-init >/dev/null
log "bucket $S3_BUCKET ready"


# Re-run against an existing cluster (e.g. resuming run-migration.sh mid-migration): verify only.
if [[ -f "$RESULTS/cluster-uuid" && -f "$RESULTS/seed-block.orig.yaml" ]] && any_broker >/dev/null 2>&1 && is_healthy; then
  log "cluster already exists: verifying only (uuid $(check_cluster_uuid), $(brokers_json | jq length) brokers)"
  assert_versions
  # A swap interrupted between decommission (step 7) and repoint (step 9) legitimately leaves an
  # alias on a decommissioned broker; swap.sh --resume-from 8 finishes it. Warn, do not fail.
  "$ROOT/scripts/check-seeds.sh" || warn "seed check failed on re-run (expected only while a swap is between steps 7 and 9)"
  exit 0
fi

FOUNDERS=(io-b1 io-b3 io-b5)
formed() { brokers_json 2>/dev/null | jq -e '[.[]|select(.is_alive)]|length >= 3' >/dev/null 2>&1; }
if ! any_broker >/dev/null 2>&1 || ! formed; then
  # Initial formation: the three seeds start together.
  # With empty_seed_starts_cluster=false a broker is a founder only if its own advertised RPC
  # address appears literally in seed_servers (cluster_discovery.cc compares the unresolved
  # strings). Alias seeds (b1/b3/b5) never match the instance names, so founders bootstrap with
  # instance-name seeds, then are re-rendered to the alias list and restarted one by one.
  # This happens before any test step and before the seed fingerprint is recorded.
  for i in "${FOUNDERS[@]}"; do
    "$ROOT/scripts/broker.sh" start "$i" --seeds "${FOUNDERS[*]}" --founder
  done
  retry_until 300 3 "3-seed cluster formed" formed
  log "cluster formed by founders (instance-name seeds)"
  for i in "${FOUNDERS[@]}"; do wait_joined "$i" >/dev/null; done
  echo "$(ts) founders ${FOUNDERS[*]} formed cluster with instance-name seeds; switched to alias seeds" >> "$RESULTS/bootstrap.log"
fi
for i in "${FOUNDERS[@]}"; do
  if ! docker exec "$(cname "$i")" grep -qE "address: b1\.${ZONE}" /etc/redpanda/redpanda.yaml; then
    "$ROOT/scripts/render-config.sh" "$i" >/dev/null
    "$ROOT/scripts/broker.sh" restart "$i"
    sleep 3; wait_back "$i"
    log "$i now runs with alias seeds"
  fi
done
for i in io-b2 io-b4; do "$ROOT/scripts/broker.sh" start "$i"; done
for i in io-b2 io-b4; do wait_joined "$i" >/dev/null; done

n="$(brokers_json | jq length)"
[[ "$n" == 5 ]] || die "expected 5 brokers, have $n"
assert_versions
log "cluster uuid $(check_cluster_uuid)"
retry_until 120 5 "cluster healthy" is_healthy

# Seed block fingerprint, taken from the running brokers after the alias switch.
# It must never change for the rest of the test (check-seeds.sh compares against it).
if [[ ! -f "$RESULTS/seed-block.orig.yaml" ]]; then
  seed_block_of io-b2 > "$RESULTS/seed-block.orig.yaml"
  seed_effective_of io-b2 > "$RESULTS/seed-effective.orig.json"
  log "recorded original seed block: $(tr -s ' \n' ' ' < "$RESULTS/seed-block.orig.yaml")"
fi
"$ROOT/scripts/check-seeds.sh"

# License (enterprise features: Tiered Storage, continuous balancing).
lic="$(rpk_exec cluster license info 2>&1 || true)"
echo "$lic" > "$RESULTS/license-info.txt"
grep -qE 'License status:\s+valid' <<<"$lic" || die "no valid Redpanda license/trial. Provide one as REDPANDA_LICENSE and run: rpk cluster license set"
log "license: $(grep -E 'Organization|Expires' <<<"$lic" | tr -s ' ' | tr '\n' ' ')"
log "up: 5 brokers, healthy"
