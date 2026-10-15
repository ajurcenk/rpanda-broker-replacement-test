#!/usr/bin/env bash
# render-config.sh <instance> [--seeds "n1 n2 n3"]  -> config/rendered/<instance>.yaml
# The seed list defaults to the alias list b1/b3/b5 and is identical for every broker.
# --seeds overrides it; used only for initial cluster formation (see up.sh) and scenario 4.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
inst="${1:?usage: render-config.sh <instance> [--seeds \"alias...\"]}"; shift
seeds="${SEED_ALIASES[*]}"
founder=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --seeds) seeds="$2"; shift 2 ;;
    --founder) founder=1; shift ;;   # initial cluster formation only: may list itself
    *) die "unknown arg $1" ;;
  esac
done
[[ -n "${INST_RACK[$inst]:-}" ]] || die "unknown instance $inst"
seed_block=""
for a in $seeds; do
  seed_block+="    - host:"$'\n'"        address: ${a}.${ZONE}"$'\n'"        port: 33145"$'\n'
done
seed_block="${seed_block%$'\n'}"
mkdir -p "$ROOT/config/rendered"
out="$ROOT/config/rendered/${inst}.yaml"
awk -v inst="$inst" -v rack="${INST_RACK[$inst]}" -v seeds="$seed_block" '
  /@SEEDS@/ { print seeds; next }
  { gsub(/@INSTANCE@/, inst); gsub(/@RACK@/, rack); print }' "$ROOT/config/redpanda.yaml.tmpl" > "$out.tmp"
cat "$out.tmp" > "$out" && rm -f "$out.tmp"   # in place, keeps the inode
chmod 644 "$out"
# The instance name must never be in its own seed list (except for initial founders).
if (( ! founder )) && sed -n '/^  seed_servers:/,/^  rpc_server:/p' "$out" | grep -q "address: ${inst}\.${ZONE}"; then die "instance $inst is in its own seed list"; fi
echo "$out"
