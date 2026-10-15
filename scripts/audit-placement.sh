#!/usr/bin/env bash
# audit-placement.sh [--label L] [--informational] : one replica per rack for every partition.
set -euo pipefail
source "$(dirname "$0")/lib/common.sh"
label="adhoc"; extra=()
while [[ $# -gt 0 ]]; do case "$1" in --label) label="$2"; shift 2 ;; --informational) extra+=(--informational); shift ;; *) die "bad arg $1" ;; esac; done
py audit.py "$label" "${extra[@]}" || die "placement audit '$label' failed"
