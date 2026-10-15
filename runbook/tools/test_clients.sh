#!/usr/bin/env bash
# =============================================================================
# tools/test_clients.sh - long-running test clients used during the runbook
# =============================================================================
# TEST ONLY: these clients exist only to watch for client problems during the migration.
# They use their own topics (rb.test.*) and consumer group (rb-verifier) and never touch
# business topics.
#
# Usage:
#   tools/test_clients.sh start     start producer, load producer and verifier consumer
#   tools/test_clients.sh status    show health (also shown by every step script)
#   tools/test_clients.sh stop      stop all three
#   tools/test_clients.sh verify    check that every acknowledged message is in the topic, in order
#
# What the clients do (all through the repointable DNS names in RPK_BROKERS):
#
#   sequencer producer  writes "<seq> <send-time-us>" to $TEST_TOPIC_SEQ (1 partition)
#                       at $TEST_SEQ_RATE msg/s with acks=all. rpk prints a line only after
#                       a record is acknowledged; those lines go to state/clients/acked.log.
#   load producer       writes 1 KiB records to $TEST_TOPIC_LOAD at $TEST_LOAD_RATE msg/s,
#                       so partition moves happen under write load.
#   verifier consumer   reads $TEST_TOPIC_SEQ in group $TEST_CONSUMER_GROUP and checks that
#                       sequence numbers go up by exactly 1 (gaps are logged to violations.log).
#                       It also measures end-to-end latency (send -> consume) and writes
#                       10-second stats to consumer-stats.csv:
#                         time,count,p50_ms,p99_ms,max_ms,gaps_total,replays_total
#
# The producer and consumer restart themselves if rpk exits; each restart is logged.
# =============================================================================
set -euo pipefail
source "$(dirname "$0")/../lib/common.sh"
D="$CLIENT_DIR"
mkdir -p "$D"

# Highest sequence number already in the topic (-1 if empty). Used so a restarted
# producer never reuses a number.
last_seq_in_topic() {
  local hw; hw="$(rpk topic describe "$TEST_TOPIC_SEQ" -p | awk 'NR==2 {print $NF}')"
  if [[ -z "$hw" || "$hw" == 0 ]]; then echo -1; return; fi
  rpk topic consume "$TEST_TOPIC_SEQ" -o -1 -n 1 -f '%v\n' | awk '{print $1}'
}

# ---------------------------------------------------------------------------
# Internal: the producer process (runs in the background)
# ---------------------------------------------------------------------------
run_producer() {
  # Emits TEST_SEQ_RATE lines per second, in 10 small bursts per second.
  gen() {
    local seq="$1" per_tick=$(( TEST_SEQ_RATE / 10 )) i
    (( per_tick < 1 )) && per_tick=1
    while :; do
      for (( i = 0; i < per_tick; i++ )); do
        printf '%d %s\n' "$seq" "${EPOCHREALTIME/./}"
        seq=$((seq + 1))
      done
      sleep 0.1
    done
  }
  while :; do
    local next=$(( $(last_seq_in_topic) + 1 ))
    echo "$(ts) producer starting at seq $next" >> "$D/producer.log"
    # -o '%v\n' prints the value of every ACKNOWLEDGED record; that is the acked log.
    gen "$next" | rpk topic produce "$TEST_TOPIC_SEQ" --acks -1 --delivery-timeout 60s -z none \
        -o '%v\n' >> "$D/acked.log" 2>> "$D/producer.err" || true
    echo "$(ts) ERROR: sequencer rpk produce exited; restarting in 5s" >> "$D/producer.err"
    sleep 5
  done
}

run_load() {
  local payload; payload="$(head -c 768 /dev/urandom | base64 -w 0)"   # ~1 KiB per record
  gen() {
    local per_tick=$(( TEST_LOAD_RATE / 10 )) i n=0
    (( per_tick < 1 )) && per_tick=1
    while :; do
      for (( i = 0; i < per_tick; i++ )); do printf '%d %s\n' "$n" "$payload"; n=$((n + 1)); done
      sleep 0.1
    done
  }
  while :; do
    gen | rpk topic produce "$TEST_TOPIC_LOAD" --acks -1 --delivery-timeout 60s -o '' >/dev/null 2>> "$D/load.err" || true
    echo "$(ts) ERROR: load rpk produce exited; restarting in 5s" >> "$D/load.err"
    sleep 5
  done
}

# ---------------------------------------------------------------------------
# Internal: the verifier consumer (runs in the background)
# ---------------------------------------------------------------------------
run_consumer() {
  local last; last="$(cat "$D/consumer.last" 2>/dev/null || echo -1)"
  local gaps; gaps="$(cat "$D/consumer.gaps" 2>/dev/null || echo 0)"
  local replays=0 lats=() win_start=${EPOCHSECONDS}
  while :; do
    echo "$(ts) consumer starting, last seen seq $last" >> "$D/consumer.log"
    while read -r off seq sent_us; do
      [[ "$seq" =~ ^[0-9]+$ ]] || continue
      local now_us=${EPOCHREALTIME/./}
      lats+=( $(( (now_us - sent_us) / 1000 )) )
      if (( seq == last + 1 )); then
        last=$seq
      elif (( seq <= last )); then
        replays=$((replays + 1))      # re-delivery after a consumer restart: expected, not an error
      else
        gaps=$(( gaps + seq - last - 1 ))
        echo "$(ts) GAP expected $((last + 1)) got $seq at offset $off" >> "$D/violations.log"
        last=$seq
      fi
      if (( EPOCHSECONDS - win_start >= 10 )); then
        local stats; stats="$(printf '%s\n' "${lats[@]}" | sort -n | awk '{a[NR]=$1} END {
            if (NR == 0) { print "0,,,"; exit }
            p50=a[int(NR*0.50)+1]; p99=a[int(NR*0.99)+1 > NR ? NR : int(NR*0.99)+1]
            printf "%d,%d,%d,%d", NR, p50, p99, a[NR] }')"
        echo "$(ts),$stats,$gaps,$replays" >> "$D/consumer-stats.csv"
        echo "$last" > "$D/consumer.last"; echo "$gaps" > "$D/consumer.gaps"
        lats=(); win_start=${EPOCHSECONDS}
      fi
    done < <(rpk topic consume "$TEST_TOPIC_SEQ" -g "$TEST_CONSUMER_GROUP" -f '%o %v\n' 2>> "$D/consumer.err")
    echo "$(ts) ERROR: verifier rpk consume exited; restarting in 5s" >> "$D/consumer.err"
    sleep 5
  done
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
start_bg() {   # start_bg <name> <internal-command>
  local name="$1" cmd="$2"
  if [[ -f "$D/$name.pid" ]] && kill -0 "$(cat "$D/$name.pid")" 2>/dev/null; then
    echo "$name already running (pid $(cat "$D/$name.pid"))"; return
  fi
  # setsid: own process group, so 'stop' can end rpk and the generator together.
  setsid nohup "$0" "$cmd" >> "$D/$name.log" 2>&1 < /dev/null &
  echo $! > "$D/$name.pid"
  echo "started $name (pid $!)"
}
stop_bg() {
  local name="$1"
  if [[ -f "$D/$name.pid" ]]; then
    kill -TERM -- "-$(cat "$D/$name.pid")" 2>/dev/null || true
    rm -f "$D/$name.pid"; echo "stopped $name"
  fi
}

case "${1:-}" in
  _producer) run_producer ;;
  _load)     run_load ;;
  _consumer) run_consumer ;;
  start)
    start_bg producer _producer
    start_bg load _load
    start_bg consumer _consumer
    ;;
  stop)
    stop_bg producer; stop_bg load; stop_bg consumer
    ;;
  status)
    echo "clients: $(client_health_line)"
    echo "acked messages: $(wc -l < "$D/acked.log" 2>/dev/null || echo 0)"
    echo "last 3 consumer stats (time,count,p50,p99,max,gaps,replays):"
    tail -3 "$D/consumer-stats.csv" 2>/dev/null | sed 's/^/  /'
    [[ -s "$D/violations.log" ]] && { echo "VIOLATIONS:"; tail -5 "$D/violations.log"; }
    [[ -s "$D/producer.err" ]] && { echo "producer errors (last 5):"; tail -5 "$D/producer.err"; }
    true
    ;;
  verify)
    # Read the whole sequencer topic and check every acked sequence number is there,
    # in order of first appearance. Duplicates of unacked retries are allowed.
    hw="$(rpk topic describe "$TEST_TOPIC_SEQ" -p | awk 'NR==2 {print $NF}')"
    echo "reading $hw records from $TEST_TOPIC_SEQ ..."
    rpk topic consume "$TEST_TOPIC_SEQ" -o start -n "$hw" -f '%o %v\n' > "$D/verify-topic.txt"
    awk 'NR==FNR { if (!($2 in pos)) pos[$2]=$1; next }            # topic: first offset of each seq
         { acked++; if (!($1 in pos)) { missing++; if (missing<=10) print "MISSING seq " $1; next }
           if (pos[$1] < prev) reorder++; prev=pos[$1] }
         END { printf "acked=%d missing=%d reordered=%d\n", acked, missing, reorder; exit (missing||reorder) ? 1 : 0 }' \
      "$D/verify-topic.txt" "$D/acked.log"
    ;;
  *) echo "usage: $0 start|stop|status|verify"; exit 2 ;;
esac
