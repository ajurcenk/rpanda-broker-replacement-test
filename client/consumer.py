"""Verifier consumer (group `verifier`) on order.wal.btc-try.

Checks that sequence numbers advance by exactly +1. Violations go to results/violations.log.
Commits every second, and persists {offset, last_seq} after each commit so a restart can keep
validating from the committed position. Bootstraps through the DNS aliases only.
"""
import json
import os
import signal
import time

from confluent_kafka import Consumer, TopicPartition

R = "/work/results"
TOPIC = os.environ.get("TOPIC", "order.wal.btc-try")
BOOT = os.environ["BOOTSTRAP"]
STATE = f"{R}/consumer.state"
c = Consumer({"bootstrap.servers": BOOT, "group.id": "verifier", "enable.auto.commit": False,
              "auto.offset.reset": "earliest", "client.id": "rpbr-verifier"})
c.subscribe([TOPIC])
viol = open(f"{R}/violations.log", "a", buffering=1)
offs = open(f"{R}/verifier-offsets.log", "a", buffering=1)
state = json.load(open(STATE)) if os.path.exists(STATE) else {"offset": 0, "last_seq": -1}
last_seq = state["last_seq"]
expect_offset = state["offset"]
print(f"consumer starting, last_seq={last_seq}, expecting offset {expect_offset}", flush=True)
stop = False


def on_term(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, on_term)
signal.signal(signal.SIGINT, on_term)
next_off = None
n = 0
last_commit = time.time()


def commit():
    if next_off is None:
        return
    c.commit(offsets=[TopicPartition(TOPIC, 0, next_off)], asynchronous=False)
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"offset": next_off, "last_seq": last_seq}, f)
    os.replace(tmp, STATE)
    offs.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())},{next_off},{last_seq}\n")


first = True
while not stop:
    m = c.poll(0.5)
    if m is not None and not m.error():
        seq = json.loads(m.value())["seq"]
        if first:
            first = False
            if m.offset() != expect_offset and last_seq >= 0:
                # resumed somewhere other than our own last commit: can't attribute the jump
                viol.write(f"{time.time():.3f},resume-offset-mismatch,expected_offset={expect_offset},got_offset={m.offset()},seq={seq}\n")
                last_seq = seq - 1
        if seq == last_seq + 1:
            pass
        elif seq <= last_seq:
            viol.write(f"{time.time():.3f},dup-or-reorder,expected={last_seq + 1},got={seq},offset={m.offset()}\n")
        else:
            viol.write(f"{time.time():.3f},gap,expected={last_seq + 1},got={seq},offset={m.offset()}\n")
        last_seq = max(last_seq, seq)
        next_off = m.offset() + 1
        n += 1
    if time.time() - last_commit >= 1:
        try:
            commit()
        except Exception as e:  # transient during leadership moves; retried next second
            print(f"commit failed: {e}", flush=True)
        last_commit = time.time()
try:
    commit()
finally:
    c.close()
print(f"consumer stopped, consumed {n}, last_seq={last_seq}", flush=True)
