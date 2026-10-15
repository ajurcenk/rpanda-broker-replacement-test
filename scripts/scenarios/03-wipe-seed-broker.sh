#!/usr/bin/env bash
# Scenario 3: scenario 2 for a broker behind a seed alias (b1).
exec "$(dirname "$0")/02-wipe-new-broker.sh" --seed
