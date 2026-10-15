#!/usr/bin/env bash
# =============================================================================
# step_18_dns_repoint.sh <swap> - Phase 2, runbook step 21 (Path A)
# =============================================================================
# Repoints the DNS name of this swap (Route 53) from the OLD broker to the NEW broker.
# This applies to seed names and client-only names alike. The seed_servers lists in
# redpanda.yaml never change.
#
# Preconditions (refuses otherwise):
#   * step_16 verified the swap, and the old node is no longer a member (decommission complete)
#   * raft_learner_recovery_rate is back at the original (step_17)
#
# Then: UPSERT the record (type $DNS_RECORD_TYPE, TTL $DNS_TTL), wait until Route 53 reports
# INSYNC, wait one TTL, check with dig that the name resolves to the new broker, and re-check
# the seed rules.
#
# Undo (only if something is wrong and the OLD broker is still running and still a member,
# which is not the case after step_15): set the record back to the old value in Route 53.
# =============================================================================
source "$(dirname "$0")/lib/common.sh"
rb_init "$@"
require_swap_arg
command -v aws >/dev/null || die "aws CLI not installed on this host"
old_id="$(state_get "$SWAP_DIR/old_id")"

header "Preconditions"
check "swap verified (step_16)" test -f "$SWAP_DIR/verified.ok"
check "old node $old_id no longer a member (decommission complete)" node_gone "$old_id"
check "recovery rate restored (step_17)" test -f "$SWAP_DIR/rate_restored.ok"
check "recovery rate at original" recovery_rate_is_original
(( RB_FAILS == 0 )) || rb_summary

header "Route 53 change"
name="${SWAP_DNS%.}."
if [[ "$DNS_RECORD_TYPE" == A ]]; then value="$(resolve "$SWAP_NEW")"; else value="${SWAP_NEW%.}."; fi
[[ -n "$value" ]] || die "cannot determine the new value for $SWAP_DNS ($SWAP_NEW does not resolve)"
current="$(aws route53 list-resource-record-sets --hosted-zone-id "$ROUTE53_HOSTED_ZONE_ID" \
            --start-record-name "$name" --max-items 1 \
            --query "ResourceRecordSets[?Name=='$name'] | [0].{type: Type, ttl: TTL, values: ResourceRecords[].Value}" --output json)"
info "current record: $(jq -c . <<<"$current")"
info "new record:     $SWAP_DNS $DNS_RECORD_TYPE $value (TTL $DNS_TTL)"
batch="$(jq -n --arg n "$name" --arg t "$DNS_RECORD_TYPE" --arg v "$value" --argjson ttl "$DNS_TTL" \
  '{Comment: "broker replacement", Changes: [{Action: "UPSERT", ResourceRecordSet: {Name: $n, Type: $t, TTL: $ttl, ResourceRecords: [{Value: $v}]}}]}')"
echo "$batch" > "$SWAP_DIR/route53-change.json"
echo "$current" > "$SWAP_DIR/route53-previous.json"
printf '\n  CHANGE UPSERT %s -> %s in hosted zone %s\n' "$SWAP_DNS" "$value" "$ROUTE53_HOSTED_ZONE_ID"
if (( DRY_RUN )); then info "dry run: change batch saved to $SWAP_DIR/route53-change.json, not applied"; exit 0; fi
confirm "Apply this DNS change?" || die "not confirmed"
change_id="$(aws route53 change-resource-record-sets --hosted-zone-id "$ROUTE53_HOSTED_ZONE_ID" \
              --change-batch "file://$SWAP_DIR/route53-change.json" --query ChangeInfo.Id --output text)"
echo "$(ts) $RB_SCRIPT CHANGE route53 UPSERT $SWAP_DNS -> $value ($change_id)" >> "$RB_STATE_DIR/changes.log"
info "waiting for Route 53 change $change_id to be INSYNC"
aws route53 wait resource-record-sets-changed --id "$change_id"
ok "Route 53 change INSYNC"
echo "$(ts),$SWAP_DNS,$SWAP_OLD,$SWAP_NEW,$value" >> "$RB_STATE_DIR/dns_repoints.csv"

header "Wait one TTL ($DNS_TTL s) and check resolution"
sleep "$DNS_TTL"
deadline=$(( $(date +%s) + 300 ))
until [[ "$(resolve "$SWAP_DNS")" == "$(resolve "$SWAP_NEW")" ]]; do
  (( $(date +%s) < deadline )) || break
  info "$SWAP_DNS still resolves to $(resolve "$SWAP_DNS"); waiting (resolver cache)"
  sleep 10
done
check "$SWAP_DNS resolves to the new broker ($(resolve "$SWAP_NEW"))" dns_points_to "$SWAP_DNS" "$SWAP_NEW"
info "also check from a client host: dig +short $SWAP_DNS"

header "Seeds and clients"
check "seed rules (seed_servers unchanged, names -> live brokers, one per AZ)" seeds_check
# TEST ONLY: health of the step_00 test clients (topics rb.test.*). It shows whether clients
# see problems during this step; it is not part of the production change itself.
check "[TEST ONLY] test clients healthy" client_ok
(( RB_FAILS == 0 )) && state_set "$SWAP_DIR/repointed.ok" "$(ts)"
rb_summary
echo
info "Next: step_19_retire_old_broker.sh $SWAP_DNS"
