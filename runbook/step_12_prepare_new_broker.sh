#!/usr/bin/env bash
# =============================================================================
# step_12_prepare_new_broker.sh <swap> - Phase 2, runbook steps 13 and 14
# =============================================================================
# Prepares the NEW broker host. Most of this happens ON the new host, so the script:
#   1. makes sure the new broker's OWN DNS name (NEW_HOST in SWAP_PLAN) exists and resolves.
#      Kafka clients get every broker's advertised name from the cluster metadata and connect
#      to it directly, so this name must work from all client networks BEFORE the broker
#      joins. If the record is missing and CREATE_INSTANCE_RECORDS=true, it creates the A record
#      (NEW_IP from SWAP_PLAN) in INSTANCE_HOSTED_ZONE_ID, after asking.
#      (The repointable name, e.g. b2..., is NOT touched here; that is step_18.)
#   2. writes the node-specific redpanda.yaml settings to state/swap-*/new-broker-redpanda.yaml
#   3. walks you through the host checklist and records each confirmation
#
# Copy your existing broker config (TLS listeners, SASL, tuning) and change ONLY the keys
# in the generated file. Critical rules:
#   * rack = the OLD broker's AZ
#   * empty_seed_starts_cluster: false
#   * seed_servers = the SAME DNS names as every other broker (never this host's own name)
#   * advertised names = this host's own unique name, NOT the repointable DNS name
#   * NO node_id (the cluster assigns a new one)
#   * a fresh, empty data directory
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg

header "New host"
check "go/no-go passed (step_10)" test -f "$SWAP_DIR/go.ok"
check "$SWAP_NEW is not a cluster member" not_member "$SWAP_NEW"
for s in $(printf '%s\n' "${SEED_DNS_NAMES[@]}" ${RPK_BROKERS//,/ } | sed 's/:[0-9]*$//' | sort -u); do
  check "$SWAP_NEW is not a repointable name ($s)" [ "$SWAP_NEW" != "$s" ]
done
(( RB_FAILS == 0 )) || rb_summary

header "DNS record of the new broker itself ($SWAP_NEW)"
# Clients reach each broker by its advertised name. Without this record, a client can bootstrap
# but fails as soon as the new broker leads a partition it reads or writes.
current=""
if command -v aws >/dev/null; then
  current="$(r53_value "$INSTANCE_HOSTED_ZONE_ID" "$SWAP_NEW")"
  info "Route 53 ($INSTANCE_HOSTED_ZONE_ID): ${current:-no record}"
else
  warn "aws CLI not installed: cannot read or create the record in Route 53; checking resolution only"
fi
if [[ -z "$current" && -z "$(resolve "$SWAP_NEW")" ]]; then
  if [[ "$CREATE_INSTANCE_RECORDS" == true && -n "$SWAP_NEW_IP" ]] && command -v aws >/dev/null; then
    r53_upsert "$INSTANCE_HOSTED_ZONE_ID" "$SWAP_NEW" A "$SWAP_NEW_IP" "$INSTANCE_DNS_TTL"
  else
    bad "no DNS record for $SWAP_NEW: create an A record -> the new instance's private IP (or set NEW_IP in SWAP_PLAN and CREATE_INSTANCE_RECORDS=true)"
  fi
elif [[ -n "$current" && -n "$SWAP_NEW_IP" && "$current" != "$SWAP_NEW_IP" ]]; then
  bad "$SWAP_NEW points at $current in Route 53, but SWAP_PLAN says NEW_IP=$SWAP_NEW_IP - fix one of them"
fi
if (( RB_FAILS == 0 && ! DRY_RUN )); then
  deadline=$(( $(date +%s) + 300 ))
  until [[ -n "$(resolve "$SWAP_NEW")" ]] || (( $(date +%s) > deadline )); do
    info "waiting for $SWAP_NEW to resolve (negative DNS cache can delay a new name)"; sleep 10
  done
  new_ip="$(resolve "$SWAP_NEW")"
  check "$SWAP_NEW resolves from this host (${new_ip:-nothing})" [ -n "$new_ip" ]
  [[ -n "$SWAP_NEW_IP" ]] && check "$SWAP_NEW resolves to NEW_IP $SWAP_NEW_IP" [ "$new_ip" = "$SWAP_NEW_IP" ]
fi
(( RB_FAILS == 0 )) || rb_summary

header "Node settings for $SWAP_NEW"
out="$SWAP_DIR/new-broker-redpanda.yaml"
{
  echo "# Node-specific settings for $SWAP_NEW (swap $SWAP_NO, replaces $SWAP_OLD)"
  echo "# Merge into the host's /etc/redpanda/redpanda.yaml. Keep your TLS/SASL listener blocks."
  echo "redpanda:"
  echo "  rack: \"$SWAP_RACK\""
  echo "  empty_seed_starts_cluster: false"
  echo "  seed_servers:"
  for s in "${SEED_DNS_NAMES[@]}"; do
    echo "    - host:"
    echo "        address: $s"
    echo "        port: $SEED_RPC_PORT"
  done
  echo "  advertised_rpc_api:"
  echo "    address: $SWAP_NEW"
  echo "    port: $SEED_RPC_PORT"
  echo "  advertised_kafka_api:"
  echo "    - address: $SWAP_NEW        # keep the listener name your config uses"
  echo "      port: 9092"
  echo "  # no node_id: the cluster assigns a NEW id on first start"
} > "$out"
sed 's/^/  /' "$out"
ok "written to $out"

header "Host checklist (runbook step 13) - confirm each item"
items=(
  "EC2 stop protection (and termination protection) enabled on $SWAP_NEW"
  "data disk formatted XFS, mounted by UUID with 'nofail', and Redpanda unit has RequiresMountsFor=<data dir>"
  "data directory is EMPTY (no leftover data from a previous attempt)"
  "Redpanda ${EXPECTED_VERSION#v} installed (not 25.3.16): 'rpk version' on the host"
  "'rpk redpanda mode production' and 'rpk redpanda tune all' done"
  "'rpk iotune' done and its io-config.yaml in place"
  "redpanda.yaml merged with the settings above (rack, seeds, advertised name, no node_id)"
  "TLS certificate on $SWAP_NEW includes the name $SWAP_NEW (and the repointable name $SWAP_DNS)"
  "$SWAP_NEW resolves from EVERY client network / VPC (dig +short $SWAP_NEW from a client host returns $SWAP_NEW_IP)"
  "security groups allow 33145 and 9644 between $SWAP_NEW and all brokers"
  "security groups / firewalls allow 9092 from ALL client networks to $SWAP_NEW (same rules as the old brokers)"
)
for it in "${items[@]}"; do
  if confirm "Done: $it ?"; then ok "$it"; echo "$(ts) confirmed: $it" >> "$SWAP_DIR/checklist.log"
  else bad "not done: $it"; fi
done

(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/prepared.ok" "$(ts)"
rb_summary
echo
info "Next: START the new broker -> $(host_hint "$START_NEW_BROKER_HINT")"
info "      then run: ./step_13_confirm_join.sh $SWAP_DNS"
