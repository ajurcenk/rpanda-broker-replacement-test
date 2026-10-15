# Scenario framework: healthy-5-broker precondition, result recording, failure trap.
source "$ROOT/scripts/lib/cluster.sh"
source "$ROOT/scripts/lib/dns.sh"
source "$ROOT/scripts/lib/ops.sh"
mkdir -p "$RESULTS/scenarios" "$RESULTS/raw"
SC_NAME="${SC_NAME:?set SC_NAME}"
SC_NOTES=()
note() { SC_NOTES+=("$*"); log "[$SC_NAME] $*"; echo "$(ts) $*" >> "$RESULTS/scenarios/$SC_NAME.notes"; }
sc_result() {  # sc_result PASS|FAIL|SKIPPED <detail>
  echo "$(ts),$SC_NAME,$1,$(printf '%s' "$2" | tr ',\n' '; ')" >> "$RESULTS/scenarios.csv"
  log "[$SC_NAME] RESULT $1: $2"
}
sc_trap() { local rc=$?; (( rc == 0 )) || sc_result FAIL "exited rc=$rc; see results/scenarios/$SC_NAME.notes and logs"; }
trap sc_trap EXIT
healthy5() {
  [[ "$1" == pre ]] && wait_settled 900   # the previous scenario may have just restarted brokers
  full_gate "$SC_NAME-$1"
  local n; n="$(brokers_json | jq '[.[] | select(.membership_status=="active" and .is_alive)] | length')"
  [[ "$n" == 5 ]] || die "expected 5 active brokers, have $n"
}
# instance currently registered for a node id (latest mapping)
inst_of() { awk -v id="$1" '$1==id {print $2}' "$RESULTS/node_ids.txt" | tail -1; }
active_instances() { for id in $(brokers_json | jq -r '.[] | select(.membership_status=="active") | .node_id'); do inst_of "$id"; done | sort; }
rack_instances() { for i in $(active_instances); do [[ "${INST_RACK[$i]}" == "$1" ]] && echo "$i"; done; return 0; }
# save_logs <instance> : keep a broker's log for the report
save_logs() { docker logs "$(cname "$1")" > "$RESULTS/scenarios/$SC_NAME-$1.log" 2>&1 || true; }
