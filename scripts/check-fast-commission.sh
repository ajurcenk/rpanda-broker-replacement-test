#!/usr/bin/env bash
# check-fast-commission.sh --config --expect disabled|enabled [--label L]
# check-fast-commission.sh --evidence <new-instance> --label L [--expect-advantage yes|no] [--timings file]
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
mode=""; label="adhoc"; expect=""; inst=""; adv="yes"; timings=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) mode=config; shift ;;
    --evidence) mode=evidence; inst="$2"; shift 2 ;;
    --expect) expect="$2"; shift 2 ;;
    --label) label="$2"; shift 2 ;;
    --expect-advantage) adv="$2"; shift 2 ;;
    --timings) timings="$2"; shift 2 ;;
    *) die "bad arg $1" ;;
  esac
done
mkdir -p "$RESULTS/raw"
envs=(-e CONTROL_TOPIC="$CONTROL_TOPIC" -e SEGMENT_BYTES="$SEGMENT_BYTES")
run_py() {
  docker run --rm -i --network "$NET" --dns "$DNS_IP" --user "$(id -u):$(id -g)" -e BOOTSTRAP="$BOOTSTRAP" \
    -e RUNNING="$(running_instances | paste -sd' ')" "${envs[@]}" -v "$ROOT:/work" -w /work/tools "$TOOLS_IMAGE" python3 fc_check.py "$@"
}
case "$mode" in
  config)
    [[ "$expect" == disabled || "$expect" == enabled ]] || die "--expect disabled|enabled required"
    f="$RESULTS/raw/fc-cluster-$label.json"
    jq -n --arg ce "$(cfg_get cloud_storage_enabled)" \
          --arg b "$(cfg_get initial_retention_local_target_bytes_default)" \
          --arg m "$(cfg_get initial_retention_local_target_ms_default)" \
          '{cloud_storage_enabled:$ce, initial_retention_local_target_bytes_default:$b, initial_retention_local_target_ms_default:$m}' > "$f"
    run_py config "$label" "$expect" "/work/results/raw/fc-cluster-$label.json" || die "fast commission config check '$label' failed (expected $expect)"
    ;;
  evidence)
    [[ -n "$inst" ]] || die "--evidence <instance> required"
    f="$RESULTS/raw/du-$label.json"
    {
      echo '{'
      first=1
      members="$(admin /v1/brokers | jq -r '.[].node_id')"
      for i in $(running_instances); do
        # only current cluster members (a decommissioned broker awaiting removal is not an "existing" broker)
        id="$(awk -v i="$i" '$2==i {print $1}' "$RESULTS/node_ids.txt" | tail -1)"
        grep -qx "${id:-x}" <<<"$members" || continue
        tot="$(docker exec "$(cname "$i")" du -sb /var/lib/redpanda/data | cut -f1)"
        tops="$(docker exec "$(cname "$i")" sh -c 'cd /var/lib/redpanda/data/kafka 2>/dev/null && du -sb -- * 2>/dev/null; true' \
               | jq -Rn '[inputs | select(test("\t")) | split("\t") | {(.[1]): (.[0]|tonumber)}] | add // {}')"
        (( first )) || echo ','; first=0
        printf '"%s": {"total": %s, "topics": %s}' "$i" "$tot" "$tops"
      done
      echo '}'
    } > "$f"
    jq -e . "$f" >/dev/null || die "bad du json"
    tf=""; if [[ -n "$timings" ]]; then [[ "$(realpath "$timings")" == "$(realpath -m "$RESULTS/raw/timings-$label.json")" ]] || cp "$timings" "$RESULTS/raw/timings-$label.json"; tf="/work/results/raw/timings-$label.json"; fi
    run_py evidence "$label" "$inst" "/work/results/raw/du-$label.json" "$adv" $tf || die "fast commission evidence '$label' failed"
    ;;
  *) die "--config or --evidence required" ;;
esac
